/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ufs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/utils"
)

const (
	Delimiter        = "/"
	MaxKeys          = 1000
	AwsDefaultRegion = "us-east-1"
	TmpPath          = "./tmp/pfs/"
)

var Owner string
var Group string

type s3FileSystem struct {
	bucket      string
	subpath     string // bucket:subpath/name
	sess        *session.Session
	s3          *s3.S3
	defaultTime time.Time
	sync.Mutex
}

var _ UnderFileStorage = &s3FileSystem{}

// Used for pretty printing.
func (fs *s3FileSystem) String() string {
	return base.S3Type
}

func (fs *s3FileSystem) getFullPath(name string) string {
	if strings.HasPrefix(name, Delimiter) && name != Delimiter {
		name = strings.TrimPrefix(name, Delimiter)
	}
	// will remove suffix "/"
	path := filepath.Join(fs.subpath, name)
	// keep '/'
	if strings.HasSuffix(name, Delimiter) {
		path += Delimiter
	}
	return path
}

func (fs *s3FileSystem) getBaseName(objectPath, prefix string) string {
	objectPath = strings.TrimPrefix(objectPath, fs.subpath+Delimiter)
	objectPath = strings.TrimPrefix(objectPath, prefix)
	objectPath = strings.TrimPrefix(objectPath, Delimiter)
	return objectPath
}

// list objects or directory
func (fs *s3FileSystem) list(prefix, marker string, limit int, recursive bool) ([]base.FileInfo, string, error) {

	if limit > MaxKeys {
		limit = MaxKeys
	}
	limit_ := int64(limit)
	request := &s3.ListObjectsInput{
		Bucket:  &fs.bucket,
		Prefix:  &prefix,
		Marker:  &marker,
		MaxKeys: &limit_,
	}
	if !recursive {
		delim := Delimiter
		request.Delimiter = &delim
	}

	res, err := fs.s3.ListObjects(request)
	if err != nil {
		return nil, "", err
	}

	fileLen := len(res.Contents)
	cap := fileLen + len(res.CommonPrefixes)
	finfos := make([]base.FileInfo, cap)

	realPrefix := strings.TrimPrefix(prefix, fs.subpath+Delimiter)

	// file
	for idx, obj := range res.Contents {
		sz := *obj.Size
		isDir := strings.HasSuffix(*obj.Key, "/")
		if isDir {
			sz = 4096
		}
		finfos[idx] = base.FileInfo{
			Name:  fs.getBaseName(*obj.Key, realPrefix),
			Path:  *obj.Key,
			Size:  sz,
			Mtime: uint64((*obj.LastModified).Unix()),
			IsDir: isDir,
		}
	}

	// directory
	for idx, obj := range res.CommonPrefixes {
		finfos[fileLen+idx] = base.FileInfo{
			Name:  fs.getBaseName(*obj.Prefix, realPrefix),
			Path:  *obj.Prefix,
			Size:  4096,
			Mtime: uint64(time.Now().Unix()),
			IsDir: true,
		}
	}

	if *res.IsTruncated {
		return finfos, *res.NextMarker, nil
	}

	return finfos, "", nil
}

// iterating through files and directories in specified path
func (fs *s3FileSystem) iterate(path string, recursive bool) (<-chan base.FileInfo, error) {
	ch := make(chan base.FileInfo, 1024*10)
	var iErr error
	go func() {
		marker := ""
		for true {
			finfos, marker, err := fs.list(path, marker, MaxKeys, recursive)
			if err != nil {
				iErr = err
				break
			}

			for _, obj := range finfos {
				ch <- obj
			}
			if len(marker) == 0 {
				break
			}
		}
		close(ch)
	}()

	return ch, iErr
}

func (fs *s3FileSystem) isBucketExists(bucket string) (bool, error) {
	request := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := fs.s3.HeadBucket(request)
	if err != nil {
		if strings.Contains(err.Error(), s3.ErrCodeNoSuchBucket) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isNotExistErr(err error) bool {
	// bos will return NotFound
	if strings.Contains(err.Error(), "NotFound") {
		return true
	}

	// s3
	if strings.Contains(err.Error(), s3.ErrCodeNoSuchKey) {
		return true
	}

	return false
}

// getRootDirAttr return root dir info of filesystem
func (fs *s3FileSystem) getRootDirAttr() *base.FileInfo {
	// 参考bosfs的做法，启动时记录一个默认时间，目录时间属性频繁变化会导致tar压缩目录失败。
	aTime := fuse.UtimeToTimespec(&fs.defaultTime)
	var perm uint32
	perm = syscall.S_IFDIR | 0777
	uid := uint32(utils.LookupUser(Owner))
	gid := uint32(utils.LookupGroup(Group))

	st := fillStat(1, perm, uid, gid, 4096, 4096, 8, aTime, aTime, aTime)

	return &base.FileInfo{
		Name:  "",
		Path:  "",
		Size:  4096,
		Mtime: uint64(fs.defaultTime.Unix()),
		IsDir: true,
		Owner: Owner,
		Group: Group,
		Mode:  utils.StatModeToFileMode(int(perm)),
		Sys:   st,
	}
}

func (fs *s3FileSystem) getDirAttr(name string) (*base.FileInfo, error) {
	if !strings.HasSuffix(name, Delimiter) {
		name += Delimiter
	}

	path := fs.getFullPath(name)

	// check empty directory first
	// some object storage (eg minio) cannot get empty directory info through listObjcet
	request := &s3.HeadObjectInput{
		Bucket: &fs.bucket,
		Key:    &path,
	}
	_, err := fs.s3.HeadObject(request)

	if err != nil {
		if isNotExistErr(err) {
			finfos, _, e := fs.list(path, "", 1, true)
			if e != nil {
				return nil, e
			}
			if len(finfos) == 0 {
				return nil, syscall.ENOENT
			}
		}
	}

	finfo := fs.getRootDirAttr()
	finfo.Name = name
	finfo.Path = path
	return finfo, nil
}

// object_path may point to an object or a directory, we need to distinguish between
// these cases:
// a -> file
// a/ -> empty directory
// c/b/c -> file
// c/b/ -> non-empty directory
// c/b/c/ -> empty directory
func (fs *s3FileSystem) GetAttr(name string) (*base.FileInfo, error) {
	path := fs.getFullPath(name)

	if path == "" {
		return fs.getRootDirAttr(), nil
	}

	request := &s3.HeadObjectInput{
		Bucket: &fs.bucket,
		Key:    &path,
	}

	// file
	response, err := fs.s3.HeadObject(request)

	if err != nil {
		if isNotExistErr(err) {
			return fs.getDirAttr(name)
		}
		return nil, err
	}

	aTime := fuse.UtimeToTimespec(response.LastModified)

	size := *response.ContentLength
	isDir := strings.HasSuffix(path, "/")
	mode := syscall.S_IFREG | 0666

	// if empty directory, s3 will return size=0
	if isDir {
		size = 4096
		mode = syscall.S_IFDIR | 0777
	}

	uid := uint32(utils.LookupUser(Owner))
	gid := uint32(utils.LookupGroup(Group))
	st := fillStat(1, uint32(mode), uid, gid, size, 4096, size/512, aTime, aTime, aTime)

	return &base.FileInfo{
		Name:  name,
		Path:  path,
		Size:  size,
		Mtime: uint64((*response.LastModified).Unix()),
		IsDir: isDir,
		Owner: Owner,
		Group: Group,
		Mode:  utils.StatModeToFileMode(mode),
		Sys:   st,
	}, nil
}

// These should update the file's ctime too.
func (fs *s3FileSystem) Chmod(name string, mode uint32) error {
	// s3不支持chmod，但是返回报错会导致tar解压报错，因此直接跳过
	return nil
}

func (fs *s3FileSystem) Chown(name string, uid uint32, gid uint32) error {
	//  s3不支持chown，但是返回报错会导致tar解压报错，因此直接跳过
	return nil
}

func (fs *s3FileSystem) Utimens(name string, atime *time.Time, mtime *time.Time) error {
	//  s3不支持Utimes，但是返回报错会导致tar解压报错，因此直接跳过
	return nil
}

func (fs *s3FileSystem) Truncate(name string, size uint64) error {
	if size == 0 {
		if err := fs.Unlink(name); err != nil {
			return err
		}
		return fs.createEmptyFile(name)
	}
	return syscall.ENOSYS
}

func (fs *s3FileSystem) Access(name string, mode, callerUid, callerGid uint32) error {
	return nil
}

// Tree structure
func (fs *s3FileSystem) Link(oldName string, newName string) error {
	return syscall.ENOSYS
}

func (fs *s3FileSystem) exists(name string) (bool, error) {
	finfo, err := fs.GetAttr(name)
	if err != nil && err != syscall.ENOENT {
		return false, err
	}
	return finfo != nil, nil
}

func (fs *s3FileSystem) createEmptyDir(dir string) error {
	if !strings.HasSuffix(dir, Delimiter) {
		dir += Delimiter
	}
	// create empty directory
	request := &s3.PutObjectInput{
		Bucket: &fs.bucket,
		Key:    &dir,
		Body:   nil,
	}

	_, err := fs.s3.PutObject(request)
	return err
}

func (fs *s3FileSystem) Mkdir(name string, mode uint32) error {
	if !strings.HasSuffix(name, Delimiter) {
		name += Delimiter
	}
	exist, err := fs.exists(name)
	if err != nil {
		return err
	}
	if exist {
		return syscall.EEXIST
	}

	path := fs.getFullPath(name)
	return fs.createEmptyDir(path)
}

func (fs *s3FileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

// Rename file oldName to newName, only support the file in the same bucket
func (fs *s3FileSystem) Rename(oldName string, newName string) error {
	oldPath := fs.bucket + "/" + fs.getFullPath(oldName)
	newPath := fs.getFullPath(newName)

	finfo, err := fs.GetAttr(oldName)
	if err != nil {
		return err
	}
	if finfo.IsDir {
		return syscall.ENOSYS
	}

	request := &s3.CopyObjectInput{
		Bucket:     &fs.bucket,
		Key:        &newPath,
		CopySource: &oldPath,
	}
	_, err = fs.s3.CopyObject(request)

	if err != nil {
		return err
	}
	return fs.Unlink(oldName)
}

func (fs *s3FileSystem) Rmdir(name string) error {
	if !strings.HasSuffix(name, Delimiter) {
		name = name + Delimiter
	}

	path := fs.getFullPath(name)

	// check empty directory
	finfos, _, err := fs.list(path, "", 2, true)
	if err != nil {
		return err
	}

	// note: finfos contains path
	if len(finfos) > 1 {
		return syscall.ENOTEMPTY
	}
	return fs.Unlink(name)
}

func (fs *s3FileSystem) Unlink(name string) error {
	key := fs.getFullPath(name)
	request := &s3.DeleteObjectInput{
		Bucket: &fs.bucket,
		Key:    &key,
	}
	_, err := fs.s3.DeleteObject(request)
	return err
}

// // Extended attributes.
func (fs *s3FileSystem) GetXAttr(name string, attribute string) (data []byte, err error) {
	return nil, syscall.ENOSYS
}

func (fs *s3FileSystem) ListXAttr(name string) (attributes []string, err error) {
	return nil, syscall.ENOSYS
}

func (fs *s3FileSystem) RemoveXAttr(name string, attr string) error {
	return syscall.ENOSYS
}

func (fs *s3FileSystem) SetXAttr(name string, attr string, data []byte, flags int) error {
	return syscall.ENOSYS
}

func (fs *s3FileSystem) getOpenFlags(name string, flags uint32) int {
	log.Debugf("the flags&syscall.O_ACCMODE is %d", flags&syscall.O_ACCMODE)
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

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (fs *s3FileSystem) Open(name string, flags uint32) (fd base.FileHandle, err error) {
	flag := fs.getOpenFlags(name, flags)

	if flag < 0 {
		return nil, syscall.ENOSYS
	}

	// read only
	finfo, err := fs.GetAttr(name)
	if err != nil {
		return nil, err
	}

	fh := &s3FileHandle{
		bucket: fs.bucket,
		name:   name,
		size:   finfo.Size,
		fs:     fs,
		flags:  flags,
	}

	if flags&syscall.O_ACCMODE == syscall.O_RDWR || flags&syscall.O_ACCMODE == syscall.O_WRONLY {
		err := fs.openForWrite(fh)
		if err != nil {
			return nil, err
		}
	}
	return fh, nil
}

func (fs *s3FileSystem) createEmptyFile(name string) error {
	exist, err := fs.exists(name)

	if err != nil {
		return err
	}

	if exist {
		return syscall.EEXIST
	}

	path := fs.getFullPath(name)
	// create empty file
	request := &s3.PutObjectInput{
		Bucket: &fs.bucket,
		Key:    &path,
		Body:   nil,
	}

	_, err = fs.s3.PutObject(request)
	return err
}

func (fs *s3FileSystem) Create(name string, flags uint32, mode uint32) (fd base.FileHandle, err error) {
	fs.Lock()
	defer fs.Unlock()
	if flags&syscall.O_CREAT != 0 || flags&syscall.O_EXCL != 0 {
		// create empty file, make GetAttr work
		if err := fs.createEmptyFile(name); err != nil {
			return nil, err
		}

		fh := &s3FileHandle{
			bucket: fs.bucket,
			name:   name,
			fs:     fs,
			flags:  flags,
			size:   0,
		}
		err = fs.openForWrite(fh)
		if err != nil {
			return nil, err
		}
		return fh, nil
	}
	return nil, syscall.ENOSYS
}

func (fs *s3FileSystem) openForWrite(fh *s3FileHandle) error {
	log.Debugf("S3 OpenForWrite: fh.name[%s]", fh.name)
	fullPath := fh.fs.getFullPath(fh.name)
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
		request := &s3.GetObjectInput{
			Bucket: &fh.bucket,
			Key:    &fullPath,
		}

		response, err := fh.fs.s3.GetObject(request)
		if err != nil {
			log.Debugf("get object failed: %v", err)
			return err
		}
		fh.canWrite = make(chan struct{})
		go func() {
			defer close(fh.canWrite)
			log.Debugf("th fh.name[%s], repsponse.Body[%+v]", fh.writeTmpfile.Name(), response.Body)
			fh.writeSrcReader = response.Body
			_, err = io.Copy(fh.writeTmpfile, response.Body)
			if err != nil {
				log.Debugf("copy failed: %v", err)
			}
			fh.writeSrcReader = nil
			response.Body.Close()
		}()
	}
	return nil
}

// Directory handling
func (fs *s3FileSystem) ReadDir(name string) (stream []base.DirEntry, err error) {
	path := fs.getFullPath(name)
	if !strings.HasSuffix(path, Delimiter) {
		path += Delimiter
	}

	ch, err := fs.iterate(path, false)
	if err != nil {
		return nil, err
	}

	for finfo := range ch {
		mode := syscall.S_IFREG | 0666
		if finfo.IsDir {
			mode = int(utils.StatModeToFileMode(syscall.S_IFDIR | 0777))
		}
		subName := strings.TrimSuffix(finfo.Name, Delimiter)
		if subName == "" {
			continue
		}
		stream = append(stream, base.DirEntry{
			Mode: uint32(mode),
			Name: subName,
		})
	}
	err = nil
	return
}

// Symlinks.
func (fs *s3FileSystem) Symlink(value string, linkName string) error {
	return syscall.ENOSYS
}

func (fs *s3FileSystem) Readlink(name string) (string, error) {
	return "", syscall.ENOSYS
}

func (fs *s3FileSystem) StatFs(name string) *base.StatfsOut {
	// 256 T
	return &base.StatfsOut{
		Blocks:  0x1000000,
		Bfree:   0x1000000,
		Bavail:  0x1000000,
		Ffree:   0x1000000,
		Bsize:   0x1000000,
		NameLen: 1023,
	}
}

type s3FileHandle struct {
	bucket         string
	name           string
	size           int64
	flags          uint32
	writeTmpfile   *os.File
	canWrite       chan struct{}
	writeSrcReader io.ReadCloser
	fs             *s3FileSystem
}

var _ base.FileHandle = &s3FileHandle{}

func (fh *s3FileHandle) String() string {
	return fmt.Sprintf("s3FileHandle(%s)", fh.name)
}

func (fh *s3FileHandle) SetInode(*nodefs.Inode) {
}
func (fh *s3FileHandle) InnerFile() nodefs.File {
	return nil
}

func (fh *s3FileHandle) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	fullPath := fh.fs.getFullPath(fh.name)
	request := &s3.GetObjectInput{
		Bucket: &fh.bucket,
		Key:    &fullPath,
	}
	l := int64(len(buf))
	if off >= fh.size {
		return fuse.ReadResultData(buf[0:0]), fuse.OK
	}
	if fh.size == 0 {
		return fuse.ReadResultData(buf[0:0]), fuse.OK
	}
	// Range: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	if l > 0 {
		endPos := off + l
		if endPos > fh.size {
			endPos = fh.size
		}
		r := fmt.Sprintf("bytes=%d-%d", off, endPos-1)
		request.Range = &r
	} else if off > 0 {
		r := fmt.Sprintf("bytes=%d-", off)
		request.Range = &r
	}

	response, err := fh.fs.s3.GetObject(request)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(data), fuse.OK
}

// s3 do not support random write
func (fh *s3FileHandle) Write(data []byte, off int64) (uint32, fuse.Status) {
	log.Debugf("S3 Write: fh.name[%s]", fh.name)
	fullPath := fh.fs.getFullPath(fh.name)
	if fh.writeTmpfile != nil {
		if fh.canWrite != nil {
			select {
			case <-fh.canWrite:
				break
			}
		}
		n, err := fh.writeTmpfile.WriteAt(data, off)
		return uint32(n), fuse.ToStatus(err)
	}

	body := bytes.NewReader(data)
	request := &s3.PutObjectInput{
		Bucket: &fh.bucket,
		Key:    &fullPath,
		Body:   body,
	}
	_, err := fh.fs.s3.PutObject(request)
	return uint32(len(data)), fuse.ToStatus(err)
}

func (fh *s3FileHandle) Release() {
	log.Debugf("S3 Release: fh.name[%s]", fh.name)
	if fh.writeTmpfile != nil {
		if fh.canWrite != nil {
			select {
			case <-fh.canWrite:
				break
			}
		}
		fullPath := fh.fs.getFullPath(fh.name)
		fh.writeTmpfile.Seek(0, 0)
		request := &s3.PutObjectInput{
			Bucket: &fh.bucket,
			Key:    &fullPath,
			Body:   fh.writeTmpfile,
		}
		_, err := fh.fs.s3.PutObject(request)
		if err != nil {
			log.Debugf("put object error: [%+v]", err)
		}
		fh.writeTmpfile.Close()
		fh.writeTmpfile = nil
	}
}

func (fh *s3FileHandle) Flush() fuse.Status {
	return fuse.OK
}

func (fh *s3FileHandle) Fsync(flags int) (code fuse.Status) {
	return fuse.OK
}

// not support
func (fh *s3FileHandle) GetLk(owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *s3FileHandle) SetLk(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *s3FileHandle) SetLkw(owner uint64, lk *fuse.FileLock, flags uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *s3FileHandle) setLock(owner uint64, lk *fuse.FileLock, flags uint32, blocking bool) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fh *s3FileHandle) Truncate(size uint64) fuse.Status {
	if size != 0 {
		return fuse.ENOSYS
	}

	err := fh.fs.Truncate(fh.name, size)
	if err != nil {
		return fuse.ToStatus(err)
	}

	// 已经打开写，则清空已写内容
	if fh.writeTmpfile != nil {
		if fh.writeSrcReader != nil {
			// 关闭reader会导致io.copy结束
			fh.writeSrcReader.Close()
		}
		fh.writeTmpfile.Truncate(0)
		fh.writeTmpfile.Seek(0, 0)
	}
	return fuse.OK
}

func (fh *s3FileHandle) Chmod(mode uint32) fuse.Status {
	return fuse.ToStatus(fh.fs.Chmod(fh.name, mode))
}

func (fh *s3FileHandle) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.ToStatus(fh.fs.Chown(fh.name, uid, gid))
}

func (fh *s3FileHandle) GetAttr(a *fuse.Attr) fuse.Status {
	finfo, err := fh.fs.GetAttr(fh.name)
	if err != nil {
		return fuse.ToStatus(err)
	}

	stat_t := finfo.Sys.(syscall.Stat_t)
	a.FromStat(&stat_t)
	return fuse.OK
}
func (fh *s3FileHandle) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.ToStatus(fh.fs.Utimens(fh.name, atime, mtime))
}

func (fh *s3FileHandle) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.ENOSYS
}

func NewS3FileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	endpoint := properties[base.Endpoint].(string)
	accessKey := properties[base.AccessKey].(string)
	secretKey := properties[base.SecretKey].(string)
	bucket := properties[base.Bucket].(string)
	region := properties[base.Region].(string)
	subpath := properties[base.SubPath].(string)

	endpoint = strings.TrimSuffix(endpoint, "/")
	bucket = strings.TrimSuffix(bucket, "/")
	ssl := strings.ToLower(endpoint) == "https"
	if region == "" {
		region = AwsDefaultRegion
	}
	awsConfig := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		DisableSSL:       aws.Bool(!ssl),
		S3ForcePathStyle: aws.Bool(true),
	}
	if accessKey != "" && secretKey != "" {
		secretKey, err := common.AesDecrypt(secretKey, common.AESEncryptKey)
		if err != nil {
			return nil, err
		}
		awsConfig.Credentials = credentials.NewStaticCredentials(accessKey, secretKey, "")
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("Fail to create s3 session: %s", err)
	}

	if strings.HasPrefix(subpath, Delimiter) {
		subpath = strings.TrimPrefix(subpath, Delimiter)
	}

	fs := &s3FileSystem{
		bucket:      bucket,
		subpath:     subpath,
		sess:        sess,
		s3:          s3.New(sess),
		defaultTime: time.Now(),
	}

	exist, err := fs.isBucketExists(bucket)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, errors.New("BucketNotExist")
	}

	// create subpath if not exists
	if subpath != "" {
		exist, err = fs.exists("")
		if err != nil {
			log.Debugf("s3 exists err: %v", err)
			return nil, err
		}
		if !exist {
			if err := fs.createEmptyDir(subpath); err != nil {
				log.Debugf("s3 create empty dir err: %v", err)
				return nil, err
			}
		} else {
			// 目录存在的时候，需要判断用户是否对这个目录有owner的权限
			_, _, err = fs.list(subpath, "", 1, true)
			if err != nil {
				log.Debugf("s3 list err: %v", err)
				return nil, err
			}
		}
	}

	owner, ok := properties[base.Owner]
	if ok {
		Owner = owner.(string)
	} else {
		Owner = "root"
	}
	group, ok := properties[base.Group]

	if ok {
		Group = group.(string)
	} else {
		Group = "root"
	}

	return fs, nil
}

func init() {
	RegisterUFS(base.S3Type, NewS3FileSystem)
}
