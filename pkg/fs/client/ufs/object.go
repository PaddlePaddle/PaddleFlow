package ufs

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/baidubce/bce-sdk-go/auth"
	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/PaddlePaddle/PaddleFlow/go-sdk/service"
	v1 "github.com/PaddlePaddle/PaddleFlow/go-sdk/service/apiserver/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs/object"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func init() {
	RegisterUFS(fsCommon.S3Type, NewObjectFileSystem)
	RegisterUFS(fsCommon.BosType, NewObjectFileSystem)
}

const MiB int64 = 1024 * 1024
const GiB int64 = 1024 * 1024 * 1024
const COPY_LIMIT = uint64(5 * 1024 * 1024 * 1024)

var chunkPool = &sync.Pool{New: func() interface{} { return make([]byte, MPUChunkSize) }}
var duration int

type objectFileSystem struct {
	subPath     string // bucket:subPath/name
	storage     object.ObjectStorage
	defaultTime time.Time
	implicitDir bool
	sync.Mutex
	chunkPool *sync.Pool
	copyPool  *ants.Pool
}

type objectFileHandle struct {
	mpuInfo        mpuInfo
	storage        object.ObjectStorage
	name           string
	key            string
	size           uint64
	flags          uint32
	writeTmpfile   *os.File
	canWrite       chan struct{}
	writeSrcReader io.ReadCloser
	mu             sync.RWMutex
	writeDirty     bool
}

func (fh *objectFileHandle) Read(dest []byte, off uint64) (int, error) {
	l := uint64(len(dest))
	if off >= fh.size {
		return 0, nil
	}

	// Range: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	limit := l
	if l > 0 {
		endPos := off + l
		if endPos > fh.size {
			limit = fh.size - off
		}

	}
	in, err := fh.storage.Get(fh.key, int64(off), int64(limit))
	if err != nil {
		log.Errorf("fh.storage.Get: key[%s] off[%d] limit[%d] err[%v]", fh.key, off, limit, err)
		return 0, err
	}

	var n int
	n, err = io.ReadFull(in, dest)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		log.Errorf("io.ReadFull: key[%s] off[%d] limit[%d] err[%v]", fh.key, off, limit, err)
		return 0, err
	}
	return n, nil
}

func (fh *objectFileHandle) Write(data []byte, off uint64) (written uint32, code error) {
	log.Tracef("name[%s] offset[%d] length[%d]", fh.name, off, len(data))
	if len(data) <= 0 {
		log.Debugf("name[%s] no need to write. data len is 0", fh.name)
		return uint32(0), nil
	}
	if fh.writeTmpfile == nil {
		err := fmt.Errorf("file[%s] bad file descriptor: writeTmpfile = nil", fh.name)
		log.Errorf(err.Error())
		return uint32(0), err
	}

	if fh.canWrite != nil {
		select {
		case <-fh.canWrite:
			break
		}
	}
	fh.mu.Lock()
	defer fh.mu.Unlock()

	n, err := fh.writeTmpfile.WriteAt(data, int64(off))
	if err != nil {
		log.Errorf("fh.writeTmpfile.WriteAt: name[%s] err[%v]", fh.name, err)
		return uint32(0), err
	}
	fh.writeDirty = true
	return uint32(n), nil
}

func (fh *objectFileHandle) Flush() error {
	return fh.uploadWriteTmpFile()
}

func (fh *objectFileHandle) Release() {
	if err := fh.uploadWriteTmpFile(); err != nil {
		log.Errorf("fh.uploadWriteTmpFile: name[%s] err[%v]", fh.name, err)
	}

	if fh.writeTmpfile != nil {
		if err := fh.writeTmpfile.Close(); err != nil {
			log.Errorf("fh.writeTmpfile.Close: name[%s] err:[%v]", fh.name, err)
		}
		fh.writeTmpfile = nil
	}
}

func (fh *objectFileHandle) Fsync(flags int) error {
	return nil
}

func (fh *objectFileHandle) Truncate(size uint64) error {
	log.Tracef("name[%s] size[%d]", fh.name, size)

	if fh.writeTmpfile == nil {
		err := fmt.Errorf("object truncate: file[%s] bad file descriptor writeTmpfile = nil", fh.name)
		log.Errorf(err.Error())
		return err
	}

	// wait until read from remote to tmpFile finish
	if fh.canWrite != nil {
		select {

		case <-fh.canWrite:
			break
		}
	}
	err := fh.writeTmpfile.Truncate(int64(size))
	if err != nil {
		log.Errorf("fh.writeTmpfile.Truncate: name[%s] err[%v]", fh.name, err)
		return err
	}
	fh.writeDirty = true
	return fh.uploadWriteTmpFile()
}

func (fh *objectFileHandle) Allocate(off uint64, size uint64, mode uint32) error {
	return nil
}

func (fs *objectFileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time) error {
	//  s3不支持Utimes，但是返回报错会导致tar解压报错，因此直接跳过
	return nil
}

func (fs *objectFileSystem) String() string {
	return "object"
}

func (fs *objectFileSystem) GetAttr(name string) (*base.FileInfo, error) {
	if name == "" || name == Delimiter {
		return fs.getRootDirAttr(), nil
	}
	dirChan := make(chan *object.ListBlobsOutput, 1)
	objectChan := make(chan *object.HeadObjectOutput, 2)
	errDirBlobChan := make(chan error, 1)
	errDirChan := make(chan error, 1)
	errObjectChan := make(chan error, 1)

	fileKey := fs.objectKeyName(name)
	dirKey := fs.objectKeyName(toDirPath(name))

	// 1. check if key exists
	go func() {
		var response *object.HeadObjectOutput
		var headErr error
		for i := 0; i < 5; i++ {
			response, headErr = fs.storage.Head(fileKey)
			if headErr != nil && !isNotExistErr(headErr) {
				log.Errorf("Head[%v] object err: %v", i, headErr)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			break
		}
		if headErr != nil {
			log.Debugf("Head object err: %v", headErr)
			errObjectChan <- headErr
			return
		}
		objectChan <- response
	}()
	// 2. check if dir exists with dir key not exist
	go func() {
		if !fs.implicitDir {
			return
		}
		listInput := &object.ListInput{
			Prefix:    dirKey,
			MaxKeys:   1,
			Delimiter: Delimiter,
		}
		var dirs *object.ListBlobsOutput
		var errList error
		for i := 0; i < 5; i++ {
			dirs, errList = fs.storage.List(listInput)
			if errList != nil && !isNotExistErr(errList) {
				log.Errorf("List[%v] object err: %v", i, errList)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			break
		}
		if errList != nil {
			log.Debugf("List object err: %v", errList)
			errDirChan <- errList
			return
		}
		dirChan <- dirs
	}()
	// 3. check if dir exists with dir key exists
	go func() {
		var resp *object.HeadObjectOutput
		var headErr error
		for i := 0; i < 5; i++ {
			resp, headErr = fs.storage.Head(dirKey)
			if headErr != nil && !isNotExistErr(headErr) {
				log.Errorf("Head[%v] object err: %v", i, headErr)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			break
		}
		if headErr != nil {
			log.Debugf("Head object err: %v", headErr)
			errObjectChan <- headErr
			return
		}
		objectChan <- resp
	}()

	checking := 3
	for {
		select {
		case resp := <-objectChan:
			if resp.IsDir {
				fInfo := fs.getRootDirAttr()
				fInfo.Name = name
				fInfo.Path = fs.objectKeyName(name)
				return fInfo, nil
			} else {
				aTime := fuse.UtimeToTimespec(&resp.LastModified)
				size := int64(resp.Size)
				st := fillStat(1, 0, 0, 0, size, 4096, size/512, aTime, aTime, aTime)
				mtime := uint64((resp.LastModified).Unix())
				return &base.FileInfo{
					Name:  name,
					Path:  fileKey,
					Size:  size,
					Mtime: mtime,
					IsDir: false,
					Owner: Owner,
					Group: Group,
					Sys:   st,
				}, nil
			}
		case resp := <-errDirChan:
			log.Errorf("errDirChan object err: %v", resp)
			return nil, resp
		case resp := <-errObjectChan:
			if !isNotExistErr(resp) {
				log.Errorf("errObjectChan object err: %v", resp)
				return nil, resp
			}
			checking--
		case resp := <-errDirBlobChan:
			if !isNotExistErr(resp) {
				log.Errorf("errDirBlobChan object err: %v", resp)
				return nil, resp
			}
			checking--
		case resp := <-dirChan:
			if len(resp.Items) > 0 || len(resp.Prefixes) > 0 {
				fInfo := fs.getRootDirAttr()
				fInfo.Name = name
				fInfo.Path = fs.objectKeyName(name)
				return fInfo, nil
			}
			checking--
		}
		switch checking {
		case 1:
			break
		case 0:
			return nil, syscall.ENOENT
		}
	}
}

func (fs *objectFileSystem) Chmod(name string, mode uint32) error {
	return nil
}

func (fs *objectFileSystem) Chown(name string, uid uint32, gid uint32) error {
	return nil
}

func (fs *objectFileSystem) Truncate(name string, size uint64) error {
	return nil
}

func (fs *objectFileSystem) Access(name string, mode, callerUid, callerGid uint32) error {
	return nil
}

func (fs *objectFileSystem) Link(oldName string, newName string) error {
	return syscall.ENOSYS
}

func (fs *objectFileSystem) Mkdir(name string, mode uint32) error {
	name = toS3Path(name)
	name = toDirPath(name)
	return fs.createEmptyDir(name)
}

func (fs *objectFileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func (fs *objectFileSystem) Rename(oldName string, newName string) error {
	oldName, newName = toS3Path(oldName), toS3Path(newName)
	log.Tracef("rename: [%s]->[%s]", oldName, newName)
	var renameChildren bool

	fromIsDir, err := fs.isEmptyDir(oldName)
	if err != nil {
		log.Debugf("isEmptyDir err: %v", err)
		if err == syscall.ENOTEMPTY {
			renameChildren = true
		} else {
			return err
		}
	}
	if fromIsDir {
		oldName = toDirPath(oldName)
	}

	toIsDir, err := fs.isEmptyDir(newName)
	if err != nil {
		log.Errorf("fs.isEmptyDir err[%v]", err)
		return err
	}
	if toIsDir {
		newName = toDirPath(newName)
	}

	if fromIsDir && !toIsDir {
		newPath := fs.objectKeyName(newName)
		_, err = fs.storage.Head(newPath)
		if err == nil {
			return syscall.ENOTDIR
		}
		if err != nil && !isNotExistErr(err) {
			return err
		}
		// 来源是目录的话，rename也应该是目录
		newName = toDirPath(newName)
	} else if !fromIsDir && toIsDir {
		return syscall.EISDIR
	}
	if renameChildren {
		err = fs.renameChildren(oldName, newName)
		if err != nil {
			log.Errorf("fs.renameChildren: oldName[%s] newName[%s] err[%v]", oldName, newName, err)
		}
	} else {
		err = fs.renameObject(oldName, newName)
		if err != nil {
			log.Errorf("fs.renameObject: oldName[%s] newName[%s] err[%v]", oldName, newName, err)
		}
	}
	return err
}

func (fs *objectFileSystem) Rmdir(name string) error {
	name = toS3Path(name)
	name = toDirPath(name)
	resp, _, err := fs.list(name, "", 2, true)
	if err != nil {
		log.Errorf("fs.list: key[%s] err[%v]", name, err)
		return err
	}
	if len(resp) > 1 {
		log.Errorf("Rmdir list key[%s] not emtpy %v", name, resp)
		return syscall.ENOTEMPTY
	}
	err = fs.Unlink(name)
	if err != nil {
		log.Errorf("fs.Unlink: key[%s] err[%v]", name, err)
	}
	return err
}

func (fs *objectFileSystem) Unlink(name string) error {
	key := fs.objectKeyName(name)
	err := fs.storage.Deletes([]string{key})
	if err != nil {
		log.Errorf("fs.storage.Deletes: key[%s] err[%v]", key, err)
	}
	return err
}

func (fs *objectFileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	return nil, syscall.ENOSYS
}

func (fs *objectFileSystem) ListXAttr(name string) (attributes []string, err error) {
	return nil, syscall.ENOSYS
}

func (fs *objectFileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.ENOSYS
}

func (fs *objectFileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.ENOSYS
}

func (fs *objectFileSystem) Open(name string, flags uint32, size uint64) (FileHandle, error) {
	log.Tracef("s3 open: name[%s] flags[%d]", name, flags)
	flag := openFlags(name, flags)

	if flag < 0 {
		return nil, syscall.ENOSYS
	}

	fh := &objectFileHandle{
		name:    name,
		key:     fs.objectKeyName(name),
		size:    size,
		storage: fs.storage,
		flags:   flags,
	}

	if flags&syscall.O_ACCMODE == syscall.O_RDWR || flags&syscall.O_ACCMODE == syscall.O_WRONLY {
		err := openForWrite(fh)
		if err != nil {
			log.Errorf("openForWrite: name[%s] size[%d] flags[%d] err[%v]", name, size, flags, err)
			return nil, err
		}
	}
	return fh, nil
}

func (fs *objectFileSystem) Create(name string, flags uint32, mode uint32) (FileHandle, error) {
	log.Tracef("s3 create: name[%s] flags[%d], mode[%d]", name, flags, mode)
	fs.Lock()
	defer fs.Unlock()
	if flags&syscall.O_CREAT != 0 || flags&syscall.O_EXCL != 0 {
		// TODO if support "." and "..", need to check whether name contains "/", if contains, need to recursively created empty dir
		fh := &objectFileHandle{
			name:  name,
			key:   fs.objectKeyName(name),
			flags: flags,
			size:  0,
			// for upload new empty file, or the file will disappear after entry-cache expired, rename op will also generate bugs
			writeDirty: true,
			storage:    fs.storage,
		}
		err := openForWrite(fh)
		if err != nil {
			log.Errorf("openForWrite: name[%s] mode[%d] flags[%d] err[%v]", name, mode, flags, err)
			return nil, err
		}
		return fh, nil
	}
	return nil, syscall.ENOSYS
}

func (fs *objectFileSystem) ReadDir(name string) (stream []DirEntry, err error) {
	name = toS3Path(name)
	name = toDirPath(name)

	ch, err := fs.iterate(name, false)
	if err != nil {
		log.Errorf("fs.iterate: name[%s] err[%v]", name, err)
		return nil, err
	}

	stream = make([]DirEntry, 0)
	for finfo := range ch {
		if finfo.Name == "." {
			continue
		}
		mtime := int64(finfo.Mtime)
		size := finfo.Size
		isDir := finfo.IsDir
		fileType := uint8(TypeFile)
		if isDir {
			fileType = TypeDirectory
		}
		uid := uint32(utils.LookupUser(Owner))
		gid := uint32(utils.LookupGroup(Group))
		subName := strings.TrimSuffix(finfo.Name, Delimiter)
		stream = append(stream, DirEntry{
			Attr: &Attr{
				Type:      fileType,
				Size:      uint64(size),
				Mtime:     mtime,
				Atimensec: uint32(mtime),
				Mtimensec: uint32(mtime),
				Ctimensec: uint32(mtime),
				Uid:       uid,
				Gid:       gid,
			},
			Name: subName,
		})
	}
	return stream, nil
}

func (fs *objectFileSystem) Symlink(value string, linkName string) error {
	return syscall.ENOSYS
}

func (fs *objectFileSystem) Readlink(name string) (string, error) {
	return "", syscall.ENOSYS
}

func (fs *objectFileSystem) StatFs(name string) *base.StatfsOut {
	return &base.StatfsOut{
		Blocks:  0x1000000,
		Bfree:   0x1000000,
		Bavail:  0x1000000,
		Ffree:   0x1000000,
		Bsize:   0x1000000,
		NameLen: 1023,
	}
}

func (fs *objectFileSystem) Get(name string, flags uint32, off, limit int64) (io.ReadCloser, error) {
	log.Tracef("s3 get: name[%s] off[%d] limit[%d] ", name, off, limit)
	key := fs.objectKeyName(name)
	var response io.ReadCloser
	var err error
	for i := 0; i < 10; i++ {
		response, err = fs.storage.Get(key, off, limit)
		if err != nil {
			log.Errorf("fs.storage.Get: key[%s] off[%d] limit[%d] err[%v] [%v]", name, off, limit, err, i)
			if strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "send request failed") {
				time.Sleep(100 * time.Duration(i+1) * time.Millisecond)
				continue
			} else {
				log.Errorf("fs.storage.Get not connect error: key[%s] off[%d] limit[%d] err[%v] [%v]", name, off, limit, err, i)
				return nil, err
			}
		}
		break
	}
	return response, err
}

func (fs *objectFileSystem) Put(name string, reader io.Reader) error {
	return nil
}

func (fs *objectFileSystem) objectKeyName(name string) string {
	defer func() {
		log.Tracef("objectKeyName name[%s]", name)
	}()
	path := filepath.Join(fs.subPath, name)
	// keep '/'
	if strings.HasSuffix(name, Delimiter) {
		path += Delimiter
	}
	path = strings.TrimPrefix(path, Delimiter)
	return path
}

// getRootDirAttr return root dir info of filesystem
func (fs *objectFileSystem) getRootDirAttr() *base.FileInfo {
	// 参考bosfs的做法，启动时记录一个默认时间，目录时间属性频繁变化会导致tar压缩目录失败。
	aTime := fuse.UtimeToTimespec(&fs.defaultTime)
	st := fillStat(1, 0, 0, 0, 4096, 4096, 8, aTime, aTime, aTime)

	return &base.FileInfo{
		Name:  "",
		Path:  "",
		Size:  4096,
		Mtime: uint64(fs.defaultTime.Unix()),
		IsDir: true,
		Sys:   st,
	}
}

func (fs *objectFileSystem) getDefaultDirAttr(name string) (*base.FileInfo, error) {
	if err := fs.isDirExist(name); err != nil {
		return nil, err
	}
	fInfo := fs.getRootDirAttr()
	fInfo.Name = name
	fInfo.Path = fs.objectKeyName(name)
	return fInfo, nil
}

func (fs *objectFileSystem) isDirExist(name string) error {
	name = toDirPath(name)
	path := fs.objectKeyName(name)
	// when s3 prefix/dir has no s3 object key, cannot be list
	// thus list object under it to check existence
	dirChan := make(chan *object.ListBlobsOutput, 1)
	objectChan := make(chan *object.HeadObjectOutput, 1)
	errObjectChan := make(chan error, 1)
	errDirChan := make(chan error, 1)
	go func() {
		if fs.implicitDir {
			return
		}
		listInput := &object.ListInput{
			Prefix:  path,
			MaxKeys: 1,
		}
		var dirs *object.ListBlobsOutput
		var errList error
		for i := 0; i < 5; i++ {
			dirs, errList = fs.storage.List(listInput)
			if errList != nil && !isNotExistErr(errList) {
				log.Errorf("List object err: %v", errList)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			break
		}
		if errList != nil {
			log.Debugf("List object err: %v", errList)
			errDirChan <- errList
			return
		}
		dirChan <- dirs
	}()
	go func() {
		var resp *object.HeadObjectOutput
		var errHead error
		for i := 0; i < 5; i++ {
			resp, errHead = fs.storage.Head(path)
			if errHead != nil && !isNotExistErr(errHead) {
				log.Errorf("Head object err: %v", errHead)
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
			break
		}
		if errHead != nil {
			log.Debugf("Head object err: %v", errHead)
			errObjectChan <- errHead
			return
		}
		objectChan <- resp
	}()

	var objectNotFound bool
	var listDirsEmpty bool
	for {
		select {
		case resp := <-errDirChan:
			return resp
		case resp := <-errObjectChan:
			if resp != nil && !isNotExistErr(resp) {
				log.Errorf("isDirExist object err: %v", resp)
				return resp
			}
			objectNotFound = true
		case <-objectChan:
			return nil
		case resp := <-dirChan:
			if len(resp.Items) > 0 {
				return nil
			}
			listDirsEmpty = true
		}
		if listDirsEmpty && objectNotFound {
			return syscall.ENOENT
		}
	}
}

func (fs *objectFileSystem) exists(name string) (bool, error) {
	finfo, err := fs.GetAttr(name)
	if err != nil && err != syscall.ENOENT {
		return false, err
	}
	return finfo != nil, nil
}

func (fs *objectFileSystem) createEmptyDir(name string) error {
	log.Tracef("s3 createEmptyDir: name[%s]", name)
	path := fs.objectKeyName(name)
	if err := fs.storage.Put(path, nil); err != nil {
		log.Errorf("fs.storage.Put: name[%s] err[%v]", name, err)
		return err
	}
	return nil
}

func (fs *objectFileSystem) isEmptyDir(name string) (isDir bool, err error) {
	log.Tracef("s3 isEmptyDir: name[%s]", name)
	key := fs.objectKeyName(name)
	if !strings.HasSuffix(key, Delimiter) {
		key = key + "/"
	}
	input := &object.ListInput{
		Prefix:    key,
		MaxKeys:   2,
		Delimiter: Delimiter,
	}
	resp, err := fs.storage.List(input)

	if err != nil {
		log.Errorf("fs.storage.List: name[%s] err[%v]", name, err)
		return false, err
	}

	if len(resp.Prefixes) > 0 || len(resp.Items) > 1 {
		err = syscall.ENOTEMPTY
		isDir = true
		return
	}
	if len(resp.Items) == 1 {
		isDir = true
		if resp.Items[0].Key != key {
			err = syscall.ENOTEMPTY
		}
	}
	return
}

func (fs *objectFileSystem) renameObject(srcName, dstName string) error {
	log.Tracef("s3 renameObject: [%s]->[%s]", srcName, dstName)
	resp, err := fs.storage.Head(fs.objectKeyName(srcName))
	if err != nil {
		log.Errorf("fs.storage.Head： srcName[%s] err[%v]", srcName, err)
		return err
	}
	copySource, copyDst := fs.objectKeyName(srcName), fs.objectKeyName(dstName)

	if resp.Size > COPY_LIMIT {
		err = fs.copyObjectMultipart(copySource, copyDst, int64(resp.Size))
		if err != nil {
			log.Errorf("fs.copyObjectMultipart： copySource[%s] copyDst[%s] err[%v]", copySource, copyDst, err)
			return err
		}
	} else {
		err = fs.storage.Copy(copyDst, copySource)
		if err != nil {
			log.Errorf("fs.storage.Copy： srcName[%s] dstName[%s] err[%v]", copyDst, copySource, err)
			return err
		}
	}
	dk := []string{copySource}
	err = fs.storage.Deletes(dk)
	if err != nil {
		log.Errorf("fs.storage.Deletes： srcName[%s] dstName[%s] err[%v]", srcName, dstName, err)
	}
	return nil
}

func (fs *objectFileSystem) renameChildren(srcName, dstName string) (err error) {

	log.Debugf("s3 renameChildren: [%s]->[%s]", srcName, dstName)
	prefix, newPrefix := fs.objectKeyName(srcName), fs.objectKeyName(dstName)
	var copied []string
	var res *object.ListBlobsOutput
	for {
		input := &object.ListInput{
			Prefix:  prefix,
			MaxKeys: 1000,
		}
		if res != nil {
			input.ContinuationToken = res.NextContinuationToken
		}
		res, err = fs.storage.List(input)
		if err != nil {
			log.Errorf("fs.storage.List: input[%+v] err【%v】", input, err)
			return
		}
		if copied == nil {
			copied = make([]string, 0, len(res.Items))
		}
		var errBuf bytes.Buffer
		var copiedMutex sync.Mutex
		var wg sync.WaitGroup
		for _, content := range res.Items {
			tmpContent := content
			wg.Add(1)
			_ = fs.copyPool.Submit(func() {
				defer wg.Done()
				key := (tmpContent.Key)[len(prefix):]

				if tmpContent.Size > COPY_LIMIT {
					err = fs.copyObjectMultipart(tmpContent.Key, newPrefix+key, int64(tmpContent.Size))
					if err != nil {
						log.Errorf("fs.copyObjectMultipart： copySource[%s] copyDst[%s] err[%v]", tmpContent.Key, newPrefix+key, err)
						return
					}
				} else {
					err := fs.storage.Copy(newPrefix+key, tmpContent.Key)
					if err != nil {
						log.Errorf("fs.storage.Copy: oldKey[%s] newKey[%s] err[%v]", tmpContent.Key, newPrefix+key, err)
						errBuf.WriteString(err.Error() + "; ")
						return
					}
				}

				copiedMutex.Lock()
				copied = append(copied, tmpContent.Key)
				copiedMutex.Unlock()
			})
		}
		wg.Wait()
		if errBuf.Len() > 0 {
			log.Errorf("fs.Storage.RenameChildren: srcName[%s] dstName[%s] err[%v]", srcName, dstName, err)
			return fmt.Errorf(errBuf.String())
		}
		if !res.IsTruncated {
			break
		}
	}
	err = fs.storage.Deletes(copied)
	if err != nil {
		log.Errorf("fs.storage.Deletes: srcName[%s] dstName[%s] err[%v]", srcName, dstName, err)
	}
	return err
}

func openFlags(name string, flags uint32) int {
	log.Tracef("s3 getOpenFlags: name[%s], the flags&syscall.O_ACCMODE is %d", name, flags&syscall.O_ACCMODE)
	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return syscall.O_RDONLY
	}

	if flags&syscall.O_ACCMODE == syscall.O_RDWR {
		return syscall.O_RDWR
	}

	if flags&syscall.O_ACCMODE == syscall.O_WRONLY {
		return syscall.O_WRONLY
	}
	return -1
}

func openForWrite(fh *objectFileHandle) error {
	log.Tracef("s3 openForWrite: fh.name[%s], fh.size[%d]", fh.name, fh.size)
	filename := uuid.New().String()
	os.MkdirAll(TmpPath, 0755)
	tmpfile, err := ioutil.TempFile(TmpPath, filename)
	if err != nil {
		return syscall.ENOSYS
	}
	fh.writeTmpfile = tmpfile
	// 临时文件创建后删除，但是fd仍存在可使用,因此file可正常读写
	log.Debugf("create tempfile[%s]", tmpfile.Name())
	defer os.Remove(tmpfile.Name())
	fh.canWrite = nil
	if fh.size != 0 {
		body, err := fh.storage.Get(fh.key, 0, int64(fh.size))
		if err != nil {
			log.Errorf("fh.storage.Get: key[%s] err[%v]", fh.key, err)
			return err
		}
		fh.canWrite = make(chan struct{})
		go func() {
			defer close(fh.canWrite)
			log.Tracef("s3 openForWrite: fh.name[%s], tmpFile[%s], repsponse.Body[%+v]",
				fh.name, fh.writeTmpfile.Name(), body)
			fh.writeSrcReader = body
			_, err = io.Copy(fh.writeTmpfile, body)
			if err != nil {
				log.Errorf("io.Copy: key[%s] err[%v]", fh.key, err)
			}
			fh.writeSrcReader = nil
			body.Close()
		}()
	}
	return nil
}

// iterating through files and directories in specified path
func (fs *objectFileSystem) iterate(name string, recursive bool) (<-chan base.FileInfo, error) {
	ch := make(chan base.FileInfo, 1024*10)
	var iErr error
	go func() {
		continuationToken := ""
		for {
			finfos, continuationToken_, err := fs.list(name, continuationToken, MaxKeys, recursive)
			continuationToken = continuationToken_
			if err != nil {
				iErr = err
				break
			}

			for _, obj := range finfos {
				ch <- obj
			}
			if continuationToken == "" {
				break
			}
		}
		close(ch)
	}()

	return ch, iErr
}

// list objects or directory
func (fs *objectFileSystem) list(name, continuationToken string, limit int, recursive bool) ([]base.FileInfo, string, error) {
	log.Tracef("s3 list: name[%s] token[%s] limit[%d] recursive[%t]", name, continuationToken, limit, recursive)
	if limit > MaxKeys {
		limit = MaxKeys
	}

	prefix := fs.objectKeyName(name)

	// root dir
	if prefix == Delimiter {
		prefix = ""
	}

	input := &object.ListInput{
		Prefix:            prefix,
		MaxKeys:           int64(limit),
		ContinuationToken: continuationToken,
	}
	if !recursive {
		input.Delimiter = Delimiter
	}

	res, err := fs.storage.List(input)
	if err != nil {
		log.Errorf("fs.storage.List: name[%s] token[%s] err[%v]", name, continuationToken, err)
		return nil, "", err
	}

	fileLen := len(res.Items)
	cap := fileLen + len(res.Prefixes)
	finfos := make([]base.FileInfo, cap)

	// file
	for idx, obj := range res.Items {
		fileName := fs.getBaseName(obj.Key, name)
		sz := obj.Size
		isDir := strings.HasSuffix(obj.Key, Delimiter)
		if isDir {
			sz = 4096
			// if is dir and dir name == "", it is the path itself. rename as "."
			if fileName == "" {
				fileName = "."
			}
		}

		// build finfos for dirs
		finfos[idx] = base.FileInfo{
			Name:  fileName,
			Path:  obj.Key,
			Size:  int64(sz),
			Mtime: uint64((obj.LastModified).Unix()),
			IsDir: isDir,
		}
	}

	// directory
	for idx, obj := range res.Prefixes {
		finfos[fileLen+idx] = base.FileInfo{
			Name:  filepath.Base(obj.Prefix),
			Path:  obj.Prefix,
			Size:  4096,
			Mtime: uint64(time.Now().Unix()),
			IsDir: true,
		}
	}

	NextContinuationToken := ""
	if res.IsTruncated {
		if res.NextContinuationToken != "" {
			NextContinuationToken = res.NextContinuationToken
		}
	}

	return finfos, NextContinuationToken, nil
}

func (fh *objectFileHandle) uploadWriteTmpFile() error {
	log.Tracef("s3 uploadWriteTmpFile: fh.name[%s], fh.size[%d]", fh.name, fh.size)
	if !fh.writeDirty {
		log.Tracef("s3 uploadWriteTmpFile: fh.name[%s] writeDirty=false, no need to upload", fh.name)
		return nil
	}

	// abort mpu on error
	defer func() {
		if fh.mpuInfo.uploadID != nil {
			go func() {
				_ = fh.storage.AbortUpload(fh.key, *fh.mpuInfo.uploadID)
				fh.mpuInfo.uploadID = nil
				fh.mpuInfo.lastUploadEnd = 0
				fh.mpuInfo.lastPartNum = 0
			}()
		}
	}()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	fInfo, err := fh.writeTmpfile.Stat()
	if err != nil {
		log.Errorf("fh.writeTmpfile.Stat: name[%s] err[%v]", fh.name, err)
		return err
	}
	fileSize := fInfo.Size()
	// put empty file
	if fileSize == 0 {
		if err = fh.storage.Put(fh.key, nil); err != nil {
			log.Errorf("fh.storage.Put: name[%s] err[%v]", fh.name, err)
			return err
		}
		fh.writeDirty = false
		return nil
	}
	// put file
	if fileSize <= MPUThreshold {
		_, err = fh.writeTmpfile.Seek(0, 0)
		if err != nil {
			log.Errorf("fh.writeTmpfile.Seek: name[%s] err[%v]", fh.name, err)
			return err
		}
		err = fh.storage.Put(fh.key, fh.writeTmpfile)
		if err != nil {
			log.Errorf("fh.storage.Put: name[%s] err[%v]", fh.key, err)
		}
		fh.writeDirty = false
		return nil
	}
	// multi-part upload
	if fh.mpuInfo.uploadID != nil {
		log.Errorf("s3 uploadWriteTmpFile: fh.name[%s] mpuID not nil", fh.name)
		return syscall.EIO
	}
	if err = fh.MPU(); err != nil {
		log.Errorf("fh.MPU: name[%s] err[%v]", fh.name, err)
		return err
	}
	fh.writeDirty = false
	return nil
}

func (fh *objectFileHandle) multipartUpload(partNum int64, data []byte) error {
	// retry up to 3 times if upload a mpu failed
	var err error
	var resp *object.Part
	for retryNum := 0; retryNum < MPURetryTimes; retryNum++ {
		resp, err = fh.storage.UploadPart(fh.key, *fh.mpuInfo.uploadID, partNum, data)
		if err != nil {
			log.Errorf("fh.storage.UploadPart: key[%s] err[%v] retryNum[%d]", fh.key, err, retryNum)
		} else {
			log.Debugf("mpu upload: key[%s], mpuInfo[%+v] failed. err: %v. retryNum[%d]", fh.key, fh.mpuInfo, err, retryNum)
			fh.mpuInfo.partsETag[partNum-1] = &resp.ETag
			return nil
		}
	}
	return err
}

func (fh *objectFileHandle) multipartCommit() error {
	partCnt := fh.mpuInfo.lastPartNum
	parts := make([]*object.Part, partCnt)
	for i := int64(0); i < partCnt; i++ {
		if fh.mpuInfo.partsETag[i] == nil {
			err := fmt.Errorf("mpu partNum: %d missing ETag", i+1)
			log.Errorf("mpu commit: failed: fh.name[%s], mpuID[%s]. err:%v", fh.name, *fh.mpuInfo.uploadID, err)
			return err
		}
		parts[i] = &object.Part{
			ETag: *fh.mpuInfo.partsETag[i],
			Num:  i + 1,
		}
	}
	err := fh.storage.CompleteUpload(fh.key, *fh.mpuInfo.uploadID, parts)

	if err != nil {
		log.Errorf("fh.storage.CompleteUpload: key[%s] err[%v]", fh.key, err)
		return err
	}
	fh.mpuInfo.uploadID = nil
	fh.mpuInfo.partsETag = nil
	return nil
}

func partAndChunkSize(fileSize int64) (partSize int64, chunkSize int64, partsPerChunk int64) {
	chunkSize = MPUChunkSize // chunk size = 1 GiB
	if fileSize <= 8*GiB {   // fileSize <= 8 GiB
		// 8 MiB, 128 parts/chunk, total: 0 ~ 1,000 parts & chunks <= 8
		partSize, partsPerChunk = 8*MiB, 128
	} else if fileSize <= 256*GiB { // fileSize 8 GiB ~ 256 GiB
		// 64 MiB, 16 parts/chunk, total: 125 ~ 4,000 parts & 8 ~ 256 chunks
		partSize, partsPerChunk = 64*MiB, 16
	} else if fileSize <= 2*1024*GiB { // fileSize 256 GiB ~ 2 TiB
		// 512 MiB, 2 parts/chunk, total: 500 ~ 4000 parts & 256 ~ 2000 chunks
		partSize, partsPerChunk = 512*MiB, 2
	} else { // fileSize 2.5 TiB ~ 5 Tib
		// 1 GiB, 1 parts/chunk, total: 2500 ~ 5000 parts & 2500 ~ 5000 chunks
		partSize, partsPerChunk = 1*GiB, 1
	}
	if chunkSize%partSize != 0 || chunkSize/partSize != partsPerChunk {
		log.Errorf("not valid partSize: %d or chunkSize: %d", partSize, chunkSize)
		return 0, 0, 0
	}
	return partSize, chunkSize, partsPerChunk
}

func (fh *objectFileHandle) serialMPUTillEnd() error {
	fInfo, err := fh.writeTmpfile.Stat()
	if err != nil {
		log.Errorf("s3 serialMPUTillEnd: fh.name[%s] writeTmpfile.Stat err: %v", fh.name, err)
		return err
	}
	fileSize := fInfo.Size()
	log.Tracef("s3 mpu: fh.name[%s], fileSize[%d]", fh.name, fileSize)
	partSize, chunkSize, _ := partAndChunkSize(fileSize)
	chunkCnt := fileSize / chunkSize
	chunkLeftover := fileSize % chunkSize
	if chunkLeftover != 0 {
		chunkCnt++
	}
	log.Tracef("s3 mpu: fh.name[%s], chunkCnt: %d, chunkLeftover: %d",
		fh.name, chunkCnt, chunkLeftover)

	partCnt := fileSize / partSize
	partLeftover := fileSize % partSize
	if partLeftover != 0 {
		partCnt++
	}

	fh.mpuInfo.lastPartNum = partCnt
	fh.mpuInfo.partsETag = make([]*string, partCnt)
	chunkEG := new(errgroup.Group)

	log.Tracef("s3 mpu: fh.name[%s], fileSize[%d], chunkCnt[%d], partCnt[%d]",
		fh.name, fileSize, chunkCnt, partCnt)
	for i := int64(0); i < chunkCnt; i++ {
		// read tmp file to chunks
		chunkBuf := chunkPool.Get().([]byte)
		// resize chunk buffer for the last chunk to avoid EOF error
		if chunkLeftover != 0 && i == chunkCnt-1 {
			chunkBuf = chunkBuf[:chunkLeftover]
		} else {
			chunkBuf = chunkBuf[:cap(chunkBuf)]
		}
		chunkNum := i
		chunkEG.Go(func() error {
			if err = fh.readChunkAndMPU(fileSize, chunkNum, chunkBuf); err != nil {
				log.Errorf("fh.readChunkAndMPU: fileSize[%d] chunkNum[%d] chunkBuf[%d] err[%v]", fileSize, chunkNum, len(chunkBuf), err)
				return err
			}
			return nil
		})
	}
	if err = chunkEG.Wait(); err != nil {
		log.Errorf("chunkEG.Waite: err[%v]", err)
		return err
	}
	return nil
}

func (fh *objectFileHandle) readChunkAndMPU(fileSize, chunkNum int64, chunk []byte) error {
	defer func() {
		chunkPool.Put(chunk)
	}()
	partSize, chunkSize, partsPerChunk := partAndChunkSize(fileSize)

	// read from file to chunk buffer
	_, err := fh.writeTmpfile.ReadAt(chunk, chunkNum*chunkSize)
	if err != nil {
		log.Errorf("fh.writeTmpfile.ReadAt: name[%s] chunkNum[%d] err[%v]", fh.name, chunkNum, err)
		return err
	}
	partCnt := int64(len(chunk)) / partSize
	if len(chunk)%int(partSize) != 0 {
		partCnt += 1
	}
	mpuEG := new(errgroup.Group)
	log.Tracef("s3 mpu readFileAndUploadChunks: fh.name[%s], chunkNum: %d, chunkLength: %d, partCnt: %d, partSize: %d",
		fh.name, chunkNum, len(chunk), partCnt, partSize)
	// partNum := lastPartNum + 1; partNum <= partCnt; partNum++
	for i := int64(1); i <= partCnt; i++ {
		partNum := partsPerChunk*chunkNum + i
		start := (i - 1) * partSize
		end := start + partSize
		if end > int64(len(chunk)) {
			end = int64(len(chunk))
		}
		mpuEG.Go(func() error {
			if err = fh.multipartUpload(partNum, chunk[start:end]); err != nil {
				log.Errorf("fh.multipartUpload: fh.name[%s] multipartUpload[%d] err[%v]",
					fh.name, partNum, err)
				return err
			}
			return nil
		})
	}

	if err = mpuEG.Wait(); err != nil {
		log.Errorf("mpuEG.Wait: chunkNum[%d] error[%v]", chunkNum, err)
		return err
	}
	return nil
}

func (fh *objectFileHandle) MPU() error {
	multiPart, err := fh.storage.CreateMultipartUpload(fh.key)
	if err != nil {
		log.Errorf("s3 MPU: fh.name[%s], mpu create err: %v", fh.name, err)
		return err
	}
	fh.mpuInfo.uploadID = &multiPart.UploadId

	if err = fh.serialMPUTillEnd(); err != nil {
		log.Errorf("fh.serialMPUTillEnd: name[%s] err[%v]", fh.name, err)
		return err
	}

	if err = fh.multipartCommit(); err != nil {
		log.Errorf("fh.multipartCommit: name[%s] err[%v]",
			fh.name, err)
		return err
	}
	return nil
}

func (fs *objectFileSystem) getBaseName(objectPath, prefix string) string {
	objectPath = strings.TrimPrefix(objectPath, fs.subPath+Delimiter)
	objectPath = strings.TrimPrefix(objectPath, prefix)
	objectPath = strings.TrimPrefix(objectPath, Delimiter)
	return objectPath
}

func NewObjectFileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	var err error
	var ok bool

	endpoint, _ := properties[fsCommon.Endpoint].(string)
	endpoint = strings.TrimSuffix(endpoint, Delimiter)
	accessKey, _ := properties[fsCommon.AccessKey].(string)
	secretKey, _ := properties[fsCommon.SecretKey].(string)
	bucket, _ := properties[fsCommon.Bucket].(string)
	bucket = strings.TrimSuffix(bucket, Delimiter)
	region, _ := properties[fsCommon.Region].(string)
	subPath, _ := properties[fsCommon.SubPath].(string)
	objectType, _ := properties[fsCommon.Type].(string)
	ssl := strings.HasPrefix(endpoint, "https")
	if region == "" {
		region = AwsDefaultRegion
	}
	var secretKey_ string
	if accessKey != "" && secretKey != "" {
		secretKey_, err = common.AesDecrypt(secretKey, common.GetAESEncryptKey())
		if err != nil {
			// secretKey could not be AesEncrypted, so can use raw secretKey connect s3 server
			log.Debug("secretKey may be not descrypy")
			secretKey_ = secretKey
		}
	}

	subPathTmp, storage, err := newStorage(objectType, region, endpoint, accessKey, secretKey_, bucket, properties, ssl)
	if err != nil {
		log.Errorf("newStorage err %v", err)
		return nil, err
	}
	if subPathTmp != "" {
		subPath = subPathTmp
	}

	fs := &objectFileSystem{
		subPath:     tidySubpath(subPath),
		storage:     storage,
		defaultTime: time.Now(),
		chunkPool: &sync.Pool{New: func() interface{} {
			return make([]byte, MPUChunkSize)
		}},
	}
	p, _ := ants.NewPool(1000)
	fs.copyPool = p
	// create subPath if not exists
	if subPath != "" {
		exist, err := fs.exists("")
		if err != nil {
			log.Errorf("fs exists err %v", err)
			_, storage2, _ := newStorage(objectType, region, endpoint, accessKey, secretKey, bucket, properties, ssl)
			fs.storage = storage2
			exist, err = fs.exists("")
			if err != nil {
				log.Errorf("s3 exists err: %v", err)
				return nil, err
			}
		}
		if !exist {
			if err = fs.createEmptyDir(Delimiter); err != nil {
				log.Errorf("fs createEmptyDir err %v", err)
				_, storage2, _ := newStorage(objectType, region, endpoint, accessKey, secretKey, bucket, properties, ssl)
				fs.storage = storage2
				if err = fs.createEmptyDir(Delimiter); err != nil {
					log.Errorf("s3 create empty dir err: %v", err)
					return nil, err
				}
			}
		} else {
			// 目录存在的时候，需要判断用户是否对这个目录有owner的权限
			_, _, err = fs.list(Delimiter, "", 1, true)
			if err != nil {
				log.Errorf("fs list err %v", err)
				_, storage2, _ := newStorage(objectType, region, endpoint, accessKey, secretKey, bucket, properties, ssl)
				fs.storage = storage2
				_, _, err = fs.list(Delimiter, "", 1, true)
				if err != nil {
					log.Errorf("s3 list err: %v", err)
					return nil, err
				}

			}
		}
	}

	owner, ok := properties[fsCommon.Owner]
	if ok {
		Owner = owner.(string)
	} else {
		Owner = "root"
	}
	group, ok := properties[fsCommon.Group]

	if ok {
		Group = group.(string)
	} else {
		Group = "root"
	}
	implicitDir, ok := properties[fsCommon.ImplicitDir].(string)
	if ok && implicitDir == "false" {
		fs.implicitDir = false
	} else {
		fs.implicitDir = true
	}

	return fs, nil
}

func newStsServerClient(serverAddress string) (*service.PaddleFlowClient, error) {
	tmp := strings.Split(serverAddress, ":")
	port, _ := strconv.Atoi(tmp[1])
	config := &core.PaddleFlowClientConfiguration{
		Host:                       tmp[0],
		Port:                       port,
		ConnectionTimeoutInSeconds: 1,
	}
	pfClient, err := service.NewForClient(config)
	if err != nil {
		return nil, err
	}
	return pfClient, nil
}

func newStorage(objectType, region, endpoint, accessKey, secretKey_, bucket string, properties map[string]interface{}, ssl bool) (subpath string, storage object.ObjectStorage, err error) {
	switch objectType {
	case fsCommon.S3Type:
		awsConfig := &aws.Config{
			Region:           aws.String(region),
			Endpoint:         aws.String(endpoint),
			DisableSSL:       aws.Bool(!ssl),
			S3ForcePathStyle: aws.Bool(false),
			MaxRetries:       aws.Int(5),
		}
		if properties[fsCommon.S3ForcePathStyle] == "true" {
			awsConfig.S3ForcePathStyle = aws.Bool(true)
		}
		if properties[fsCommon.InsecureSkipVerify] == "true" {
			awsConfig.HTTPClient = &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			}
		}
		awsConfig.Credentials = credentials.NewStaticCredentials(accessKey, secretKey_, "")
		sess, err := session.NewSession(awsConfig)
		if err != nil {
			log.Errorf("new session fail: %v", err)
			return "", nil, fmt.Errorf("fail to create s3 session: %v", err)
		}
		storage = object.NewS3Storage(bucket, s3.New(sess))
	case fsCommon.BosType:
		// 1. 不传server，支持原始的conf的挂载。使用ak、sk或者加sessionToken
		// 2. 传了server，挂载参数sts=true，直接调用sts接口挂载，定时获取接口更新
		// 3. 传了server，没有设置sts=true，如果sessionToken有设置，则定时获取接口更新，没设置，则是原始挂载不更新
		paddleflowServer, hasServer := properties[fsCommon.Server].(string)
		sts, hasSts := properties[fsCommon.Sts].(string)
		bce.NewBackOffRetryPolicy(5, 20000, 300)
		// use stsCredential
		if hasServer && paddleflowServer != "" {
			log.Infof("init with sts update")
			token := properties[fsCommon.Token].(string)
			var pfClient *service.PaddleFlowClient
			pfClient, err = newStsServerClient(properties[fsCommon.Server].(string))
			if err != nil {
				log.Errorf("newstsClient err[%v]", err)
				return "", nil, err
			}
			fsName, _ := properties[fsCommon.FsName].(string)
			username, _ := properties[fsCommon.UserName].(string)

			if hasSts && sts == "true" {
				log.Infof("init wit sts")
				var stsAPIResult *v1.GetStsResponse
				stsAPIResult, err = pfClient.APIV1().FileSystem().Sts(context.TODO(), &v1.GetStsRequest{
					FsName:   fsName,
					Username: username,
				}, token)
				if err != nil {
					log.Errorf("newstsClient GetSts err[%v]", err)
					return "", nil, err
				}
				subpath = stsAPIResult.SubPath

				bucket = stsAPIResult.Bucket
				region = stsAPIResult.Region

				secretKey_, err = common.AesDecrypt(stsAPIResult.SecretAccessKey, common.GetAESEncryptKey())
				if err != nil {
					log.Errorf("AesDecrypt: err[%v]", err)
					return "", nil, err
				}
				stsCredential, err := auth.NewSessionBceCredentials(
					stsAPIResult.AccessKeyId,
					secretKey_,
					stsAPIResult.SessionToken)
				if err != nil {
					log.Errorf("NewSessionBceCredentials: err[%v]", err)
					return "", nil, err
				}
				clientConfig := bos.BosClientConfiguration{
					Ak:               stsAPIResult.AccessKeyId,
					Sk:               secretKey_,
					Endpoint:         stsAPIResult.Endpoint,
					RedirectDisabled: false,
				}
				// 初始化一个BosClient
				bosClient, err := bos.NewClientWithConfig(&clientConfig)
				if err != nil {
					log.Errorf("NewClientWithConfigl: err[%v]", err)
					return "", nil, fmt.Errorf("fail to create bos client: %v", err)
				}
				bosClient.Config.Credentials = stsCredential
				duration = stsAPIResult.Duration

				// update sts Credentials
				go func() {
					for {
						time.Sleep(time.Duration(duration-int(float64(duration)*0.8)) * time.Second)
						stsAPIResult, err = pfClient.APIV1().FileSystem().Sts(context.TODO(), &v1.GetStsRequest{
							FsName:   fsName,
							Username: username,
						}, token)
						if err != nil {
							log.Errorf("GetSts: err[%v]", err)
							time.Sleep(1 * time.Second)
							continue
						}
						secretKey_, err = common.AesDecrypt(stsAPIResult.SecretAccessKey, common.GetAESEncryptKey())
						stsCredential, err = auth.NewSessionBceCredentials(
							stsAPIResult.AccessKeyId,
							secretKey_,
							stsAPIResult.SessionToken)
						if err != nil {
							log.Errorf("NewSessionBceCredentials: err[%v]", err)
							time.Sleep(2 * time.Second)
							continue
						}
						bosClient.Config.Credentials = stsCredential
					}
				}()
				storage = object.NewBosClient(bucket, bosClient, true)
				return subpath, storage, nil
			}
			log.Infof("init with sessionToken")
			fs, err := pfClient.APIV1().FileSystem().Get(context.TODO(), &v1.GetFileSystemRequest{
				FsName:   fsName,
				Username: username,
			}, token)
			if err != nil {
				log.Errorf("GetSts: err[%v]", err)
				return subpath, storage, err
			}
			newProperties := fs.Properties
			secretKey_, err = common.AesDecrypt(newProperties[fsCommon.SecretKey], common.GetAESEncryptKey())
			accessKey = newProperties[fsCommon.AccessKey]

			clientConfig := bos.BosClientConfiguration{
				Ak:               accessKey,
				Sk:               secretKey_,
				Endpoint:         newProperties[fsCommon.Endpoint],
				RedirectDisabled: false,
			}
			// 初始化一个BosClient
			bosClient, err := bos.NewClientWithConfig(&clientConfig)
			if err != nil {
				panic(err)
			}

			stsCredential, err := auth.NewSessionBceCredentials(
				newProperties[fsCommon.AccessKey],
				secretKey_,
				newProperties[fsCommon.BosSessionToken])
			if err != nil {
				log.Errorf("NewSessionBceCredentials: err[%v]", err)
				time.Sleep(2 * time.Second)
			}
			bosClient.Config.Credentials = stsCredential

			storage = object.NewBosClient(fs.Properties[fsCommon.Bucket], bosClient, true)
			go func() {
				for {
					fs, err = pfClient.APIV1().FileSystem().Get(context.TODO(), &v1.GetFileSystemRequest{
						FsName:   fsName,
						Username: username,
					}, token)
					if err != nil {
						log.Errorf("get filesystem fsName[%s]: err[%v]", fsName, err)
						time.Sleep(10 * time.Second)
						continue
					}
					newProperties = fs.Properties
					secretKey_, err = common.AesDecrypt(newProperties[fsCommon.SecretKey], common.GetAESEncryptKey())
					stsCredential, err = auth.NewSessionBceCredentials(
						newProperties[fsCommon.AccessKey],
						secretKey_,
						newProperties[fsCommon.BosSessionToken])
					if err != nil {
						log.Errorf("NewSessionBceCredentials: err[%v]", err)
						time.Sleep(2 * time.Second)
						continue
					}
					bosClient.Config.Credentials = stsCredential
					time.Sleep(60 * time.Second)
				}
			}()
			return subpath, storage, nil
		}
		log.Infof("init bos with ak sk")
		clientConfig := bos.BosClientConfiguration{
			Ak:               accessKey,
			Sk:               secretKey_,
			Endpoint:         endpoint,
			RedirectDisabled: false,
		}
		// 初始化一个BosClient
		bosClient, err := bos.NewClientWithConfig(&clientConfig)
		if err != nil {
			log.Errorf("new bos client fail: %v", err)
			return "", nil, fmt.Errorf("fail to create bos client: %v", err)
		}
		startBySTS := false
		sessionToken, ok := properties[fsCommon.BosSessionToken].(string)

		if ok {
			log.Infof("init bos with sessionToken")
			startBySTS = true
			stsCredential, err := auth.NewSessionBceCredentials(
				accessKey,
				secretKey_,
				sessionToken)
			if err != nil {
				log.Errorf("NewSessionBceCredentials err[%v]", err)
				return "", nil, err
			}
			bosClient.Config.Credentials = stsCredential
		}
		storage = object.NewBosClient(bucket, bosClient, startBySTS)
	default:
		panic("object storage not found")
	}
	return
}
