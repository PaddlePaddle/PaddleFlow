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
	"golang.org/x/sync/errgroup"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/utils"
	fsCommon "paddleflow/pkg/fs/common"
)

const (
	Delimiter        = "/"
	MaxKeys          = 1000
	AwsDefaultRegion = "us-east-1"
	TmpPath          = "./tmp/pfs/"
	MaxFileSize      = 5 * 1024 * 1024 * 1024 * 1024 // s3: support upto 5 TiB file size
	// mpu
	MPURetryTimes  = 2
	MPUThreshold   = 200 * 1024 * 1024      // customized for performance
	MPUChunkSize   = 1 * 1024 * 1024 * 1024 // chunk size 1 GiB
	MPUMinPartSize = 5 * 1024 * 1024        // s3: Each part must be at least 5 MB ~ 5 GB in size (except for the last part)
	MPUMaxPartSize = 5 * 1024 * 1024 * 1024 // s3: Each part must be at least 5 MB ~ 5 GB in size (except for the last part)
	MPUMaxPartNum  = 10000                  // s3: between 1~10,000
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
	chunkPool *sync.Pool
}

var _ UnderFileStorage = &s3FileSystem{}

// Used for pretty printing.
func (fs *s3FileSystem) String() string {
	return fsCommon.S3Type
}

func (fs *s3FileSystem) getFullPath(name string) string {
	name = toS3Path(name)
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
func (fs *s3FileSystem) list(name, continuationToken string, limit int, recursive bool) ([]base.FileInfo, string, error) {
	log.Tracef("s3 list: name[%s] token[%s] limit[%d] recursive[%t]", name, continuationToken, limit, recursive)
	if limit > MaxKeys {
		limit = MaxKeys
	}
	limit_ := int64(limit)
	fullPath := fs.getFullPath(name)
	request := &s3.ListObjectsV2Input{
		Bucket:            &fs.bucket,
		Prefix:            &fullPath,
		ContinuationToken: &continuationToken,
		MaxKeys:           &limit_,
	}
	if !recursive {
		delim := Delimiter
		request.Delimiter = &delim
	}

	res, err := fs.s3.ListObjectsV2(request)
	if err != nil {
		log.Errorf("s3 list: name[%s] token[%s] failed. err: %v", name, continuationToken, err)
		return nil, "", err
	}

	fileLen := len(res.Contents)
	cap := fileLen + len(res.CommonPrefixes)
	finfos := make([]base.FileInfo, cap)

	// file
	for idx, obj := range res.Contents {
		fileName := fs.getBaseName(*obj.Key, name)
		sz := *obj.Size
		isDir := strings.HasSuffix(*obj.Key, Delimiter)
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
			Path:  *obj.Key,
			Size:  sz,
			Mtime: uint64((*obj.LastModified).Unix()),
			IsDir: isDir,
		}
	}

	// directory
	for idx, obj := range res.CommonPrefixes {
		finfos[fileLen+idx] = base.FileInfo{
			Name:  fs.getBaseName(*obj.Prefix, name),
			Path:  *obj.Prefix,
			Size:  4096,
			Mtime: uint64(time.Now().Unix()),
			IsDir: true,
		}
	}

	NextContinuationToken := ""
	if *res.IsTruncated {
		if res.NextContinuationToken != nil {
			NextContinuationToken = *res.NextContinuationToken
		}
	}

	return finfos, NextContinuationToken, nil
}

// iterating through files and directories in specified path
func (fs *s3FileSystem) iterate(name string, recursive bool) (<-chan base.FileInfo, error) {
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

func (fs *s3FileSystem) isBucketExists(bucket string) (bool, error) {
	request := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := fs.s3.HeadBucket(request)
	if err != nil {
		if strings.Contains(err.Error(), s3.ErrCodeNoSuchBucket) {
			return false, nil
		}
		log.Errorf("s3 isBucketExists: s3.HeadBucket bucket[%s] err:%v", bucket, err)
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

func (fs *s3FileSystem) getDefaultDirAttr(name string) (*base.FileInfo, error) {
	if err := fs.isDirExist(name); err != nil {
		return nil, err
	}
	fInfo := fs.getRootDirAttr()
	fInfo.Name = name
	fInfo.Path = fs.getFullPath(name)
	return fInfo, nil
}

func (fs *s3FileSystem) isDirExist(name string) error {
	name = toDirPath(name)
	// when s3 prefix/dir has no s3 object key, cannot be list
	// thus list object under it to check existence
	fInfos, _, err := fs.list(name, "", 1, true)
	if err != nil {
		log.Debugf("s3 isDirExist: fs.list name[%s] err:%v", name, err)
		return err
	}
	if len(fInfos) == 0 {
		return syscall.ENOENT
	}
	return nil
}

// object_path may point to an object or a directory, we need to distinguish between
// these cases:
// a -> file
// a/ -> empty directory
// c/b/c -> file
// c/b/ -> non-empty directory
// c/b/c/ -> empty directory
func (fs *s3FileSystem) GetAttr(name string) (*base.FileInfo, error) {
	log.Tracef("s3 getAttr: name[%s]", name)
	name = toS3Path(name)
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
	// TODO refactor. reduce chances using default dir attr
	if err != nil {
		log.Debugf("s3 getAttr: name[%s] s3.HeadObject failed. err: %v", name, err)
		if isNotExistErr(err) {
			// compatible with case where s3 dir can have no key
			return fs.getDefaultDirAttr(name)
		}
		return nil, err
	}

	aTime := fuse.UtimeToTimespec(response.LastModified)

	size := *response.ContentLength
	isDir := strings.HasSuffix(path, Delimiter)
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
	log.Tracef("s3 truncate: name[%s] size[%d] do not impl. use fh", name, size)
	return nil
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

func (fs *s3FileSystem) createEmptyDir(name string) error {
	log.Tracef("s3 createEmptyDir: name[%s]", name)
	path := fs.getFullPath(toDirPath(name))
	if err := fs.putEmptyFile(path); err != nil {
		log.Debugf("s3 createEmptyDir: name[%s] s3.putEmptyFile err: %v", name, err)
		return err
	}
	return nil
}

func (fs *s3FileSystem) putEmptyFile(path string) error {
	log.Tracef("s3 putEmptyFile: full path[%s]", path)
	request := &s3.PutObjectInput{
		Bucket: &fs.bucket,
		Key:    aws.String(path),
		Body:   nil,
	}
	_, err := fs.s3.PutObject(request)
	if err != nil {
		log.Errorf("s3 putEmptyFile: s3.PutObject[%s] err: %v", path, err)
	}
	return err
}

func (fs *s3FileSystem) Mkdir(name string, mode uint32) error {
	log.Tracef("s3 mkdir: name[%s]", name)
	name = toS3Path(name)
	name = toDirPath(name)
	exist, err := fs.exists(name)
	if err != nil {
		return err
	}
	if exist {
		return syscall.EEXIST
	}
	return fs.createEmptyDir(name)
}

func (fs *s3FileSystem) Mknod(name string, mode uint32, dev uint32) error {
	return syscall.ENOSYS
}

func toDirPath(name string) string {
	if !strings.HasSuffix(name, Delimiter) {
		name = name + Delimiter
	}
	return name
}

// inode path has a prefix "/". s3 path deos not.
func toS3Path(name string) string {
	// inodePath -> s3 prefix
	// "/" -> "/"
	// "/a" - > "a"
	// "/a/" - > "a/"
	if name != Delimiter {
		name = strings.TrimPrefix(name, Delimiter)
	}
	return name
}

func (fs *s3FileSystem) isEmptyDir(name string) (isDir bool, err error) {
	log.Tracef("s3 isEmptyDir: name[%s]", name)
	fullPath := fs.getFullPath(name)
	if !strings.HasSuffix(fullPath, Delimiter) {
		fullPath = fullPath + "/"
	}
	maxKey := int64(2)
	listInput := &s3.ListObjectsV2Input{
		Bucket:    &fs.bucket,
		Prefix:    &fullPath,
		MaxKeys:   &maxKey,
		Delimiter: aws.String(Delimiter),
	}
	resp, err := fs.s3.ListObjectsV2(listInput)
	if err != nil {
		log.Errorf("s3 isEmptyDir: name[%s] s3.ListObjectsV2 failed: %v", name, err)
		return false, err
	}
	if len(resp.CommonPrefixes) > 0 || len(resp.Contents) > 1 {
		err = syscall.ENOTEMPTY
		isDir = true
		return
	}
	if len(resp.Contents) == 1 {
		isDir = true
		if *resp.Contents[0].Key != fullPath {
			err = syscall.ENOTEMPTY
		}
	}
	return
}

func (fs *s3FileSystem) renameObject(srcName, dstName string) error {
	log.Tracef("s3 renameObject: [%s]->[%s]", srcName, dstName)
	oldName, newName := fs.getFullPath(srcName), fs.getFullPath(dstName)
	oldFullName := fs.bucket + Delimiter + oldName
	request := &s3.CopyObjectInput{
		Bucket:     &fs.bucket,
		Key:        &newName,
		CopySource: &oldFullName,
	}
	log.Debugf("rename object request is %+v", *request)
	_, err := fs.s3.CopyObject(request)
	if err != nil {
		log.Errorf("s3 renameObject: [%s] -> [%s] s3.CopyObject failed: %v", srcName, dstName, err)
		return err
	}

	requestDelete := &s3.DeleteObjectInput{
		Bucket: &fs.bucket,
		Key:    &oldName,
	}
	_, err = fs.s3.DeleteObject(requestDelete)
	if err != nil {
		log.Errorf("s3 renameObject: srcName[%s] s3.DeleteObject failed: %v", srcName, err)
	}
	return err
}

func (fs *s3FileSystem) renameChildren(srcName, dstName string) (err error) {
	log.Tracef("s3 renameChildren: [%s]->[%s]", srcName, dstName)
	prefix, newPrefix := fs.getFullPath(srcName), fs.getFullPath(dstName)
	var copied []string
	var res *s3.ListObjectsV2Output
	for {
		param := s3.ListObjectsV2Input{
			Bucket: &fs.bucket,
			Prefix: &prefix,
		}
		if res != nil {
			param.ContinuationToken = res.ContinuationToken
		}
		res, err = fs.s3.ListObjectsV2(&param)
		if err != nil {
			log.Errorf("s3 renameChildren: [%s]->[%s] s3.ListObjectsV2[%s] err: %v", srcName, dstName, prefix, err)
			return
		}
		if copied == nil {
			copied = make([]string, 0, len(res.Contents))
		}
		// after the server side copy, we want to delete all the files
		// using multi-delete, which is capped to 1000 on aws. If we
		// are going to make an arbitrary limit that sounds like a
		// good one (and we want to have an arbitrary limit because we
		// don't want to rename a million objects here)
		total := len(copied) + len(res.Contents)
		if total > 1000 || total == 1000 && *res.IsTruncated {
			return syscall.E2BIG
		}
		// say dir is "/a/dir" and it has "1", "2", "3", and we are
		// moving it to "/b/" items will be a/dir/1, a/dir/2, a/dir/3,
		// and we will copy them to b/1, b/2, b/3 respectively
		group := new(errgroup.Group)
		var lock sync.Mutex
		for _, content := range res.Contents {
			tmpContent := content
			group.Go(func() error {
				key := (*tmpContent.Key)[len(prefix):]
				from := fs.bucket + Delimiter + *tmpContent.Key
				_, err = fs.s3.CopyObject(&s3.CopyObjectInput{
					Bucket:       &fs.bucket,
					Key:          aws.String(newPrefix + key),
					CopySource:   &from,
					StorageClass: tmpContent.StorageClass,
				})
				if err != nil {
					log.Errorf("s3 renameChildren: [%s]->[%s] s3.CopyObject err: %v", from, newPrefix+key, err)
					return err
				}
				lock.Lock()
				copied = append(copied, *tmpContent.Key)
				lock.Unlock()
				return nil
			})
		}
		if err = group.Wait(); err != nil {
			return err
		}
		if !*res.IsTruncated {
			break
		}
	}
	log.Debugf("rename copies %v", copied)
	var items s3.Delete
	var objs = make([]*s3.ObjectIdentifier, len(copied))

	for i, _ := range copied {
		objs[i] = &s3.ObjectIdentifier{Key: &copied[i]}
	}
	items.SetObjects(objs)

	_, err = fs.s3.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: &fs.bucket,
		Delete: &items,
	})
	if err != nil {
		log.Errorf("s3 renameChildren: [%s]->[%s] s3.DeleteObjects err: %v", srcName, dstName, err)
	}
	return err
}

// Rename file oldName to newName, only support the file in the same bucket
func (fs *s3FileSystem) Rename(oldName, newName string) error {
	oldName, newName = toS3Path(oldName), toS3Path(newName)
	log.Tracef("s3 rename: [%s]->[%s]", oldName, newName)
	var renameChildren bool

	fromIsDir, err := fs.isEmptyDir(oldName)
	if err != nil {
		log.Debugf("fromdir is emtpy dir err %v", err)
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
		log.Debugf("todir is emtpy dir err %v", err)
		return err
	}
	if toIsDir {
		newName = toDirPath(newName)
	}

	if fromIsDir && !toIsDir {
		newPath := fs.getFullPath(newName)
		_, err = fs.s3.HeadObject(&s3.HeadObjectInput{
			Key:    &newPath,
			Bucket: &fs.bucket,
		})
		if err == nil {
			return syscall.ENOTDIR
		}
		if !isNotExistErr(err) {
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
			log.Debugf("renameChildren err is %v", err)
		}
	} else {
		err = fs.renameObject(oldName, newName)
		if err != nil {
			log.Debugf("renameObject err %v", err)
		}
	}
	return err
}

func (fs *s3FileSystem) Rmdir(name string) error {
	log.Tracef("s3 rmdir: %s", name)
	name = toS3Path(name)
	name = toDirPath(name)

	// check empty directory
	finfos, _, err := fs.list(name, "", 2, true)
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
	log.Tracef("s3 unlink: %s", name)
	key := fs.getFullPath(name)
	request := &s3.DeleteObjectInput{
		Bucket: &fs.bucket,
		Key:    &key,
	}
	_, err := fs.s3.DeleteObject(request)
	if err != nil {
		log.Errorf("s3 unlink: name[%s] s3.DeleteObject err: %v", name, err)
	}
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

// File handling.  If opening for writing, the file's mtime
// should be updated too.
func (fs *s3FileSystem) Open(name string, flags uint32) (fd base.FileHandle, err error) {
	log.Tracef("s3 open: name[%s] flags[%d]", name, flags)
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
		path:   fs.getFullPath(name),
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
	log.Tracef("s3 createEmptyFile: name[%s]", name)
	exist, err := fs.exists(name)

	if err != nil {
		return err
	}

	if exist {
		return syscall.EEXIST
	}

	path := fs.getFullPath(name)
	if err := fs.putEmptyFile(path); err != nil {
		log.Errorf("s3 createEmptyFile: s3.putEmptyFile[%s] err:%v", path, err)
	}
	return err
}

func (fs *s3FileSystem) Create(name string, flags, mode uint32) (fd base.FileHandle, err error) {
	log.Tracef("s3 create: name[%s] flags[%d], mode[%d]", name, flags, mode)
	fs.Lock()
	defer fs.Unlock()
	if flags&syscall.O_CREAT != 0 || flags&syscall.O_EXCL != 0 {
		// create empty file, make GetAttr work
		if err := fs.createEmptyFile(name); err != nil {
			log.Debugf("s3 create: name[%s] createEmptyFile err:%v", name, err)
			return nil, err
		}

		// TODO if support "." and "..", need to check whether name contains "/", if contains, need to recursively created empty dir
		fh := &s3FileHandle{
			bucket: fs.bucket,
			name:   name,
			path:   fs.getFullPath(name),
			fs:     fs,
			flags:  flags,
			size:   0,
		}
		err = fs.openForWrite(fh)
		if err != nil {
			log.Debugf("s3 create: name[%s] openForWrite err:%v", name, err)
			return nil, err
		}
		return fh, nil
	}
	return nil, syscall.ENOSYS
}

func (fs *s3FileSystem) openForWrite(fh *s3FileHandle) error {
	log.Tracef("s3 openForWrite: fh.name[%s]", fh.name)
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
			Key:    aws.String(fh.path),
		}

		response, err := fh.fs.s3.GetObject(request)
		if err != nil {
			log.Errorf("s3 openForWrite: s3.GetObject[%s] err: %v", fh.path, err)
			return err
		}
		fh.canWrite = make(chan struct{})
		go func() {
			defer close(fh.canWrite)
			log.Tracef("s3 openForWrite: fh.name[%s], tmpFile[%s], repsponse.Body[%+v]",
				fh.name, fh.writeTmpfile.Name(), response.Body)
			fh.writeSrcReader = response.Body
			_, err = io.Copy(fh.writeTmpfile, response.Body)
			if err != nil {
				log.Errorf("s3 openForWrite: fh.name[%s] copy err: %v", fh.name, err)
			}
			fh.writeSrcReader = nil
			response.Body.Close()
		}()
	}
	return nil
}

// Directory handling
func (fs *s3FileSystem) ReadDir(name string) ([]base.DirEntry, error) {
	log.Tracef("s3 readDir: name[%s]", name)
	name = toS3Path(name)
	name = toDirPath(name)

	ch, err := fs.iterate(name, false)
	if err != nil {
		log.Debugf("s3 readDir: name[%s] iterate err: %v", name, err)
		return nil, err
	}
	stream := make([]base.DirEntry, 0)
	for finfo := range ch {
		mode := syscall.S_IFREG | 0666
		if finfo.IsDir {
			mode = int(utils.StatModeToFileMode(syscall.S_IFDIR | 0777))
		}
		if finfo.Name == "." {
			continue
		}
		subName := strings.TrimSuffix(finfo.Name, Delimiter)
		stream = append(stream, base.DirEntry{
			Mode: uint32(mode),
			Name: subName,
		})
	}
	return stream, nil
}

// Symlinks.
func (fs *s3FileSystem) Symlink(value string, linkName string) error {
	return syscall.ENOSYS
}

func (fs *s3FileSystem) Readlink(name string) (string, error) {
	return "", syscall.ENOSYS
}

func (fs *s3FileSystem) Get(name string, flags uint32, off, limit int64) (io.ReadCloser, error) {
	log.Tracef("s3 get: name[%s] off[%d] limit[%d] ", name, off, limit)
	fullPath := fs.getFullPath(name)
	request := &s3.GetObjectInput{
		Bucket: &fs.bucket,
		Key:    &fullPath,
	}
	// Range: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	if limit > 0 {
		endPos := off + limit
		r := fmt.Sprintf("bytes=%d-%d", off, endPos-1)
		request.Range = &r
	} else if off > 0 {
		r := fmt.Sprintf("bytes=%d-", off)
		request.Range = &r
	}

	response, err := fs.s3.GetObject(request)
	if err != nil {
		log.Errorf("s3 get: s3.GetObject[%s] err: %v ", name, err)
		return nil, err
	}
	return response.Body, err
}

func (fs *s3FileSystem) Put(name string, reader io.Reader) error {
	return nil
}

func (fs *s3FileSystem) StatFs(name string) *base.StatfsOut {
	log.Tracef("s3 statFs:name[%s]", name)
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

type mpuInfo struct {
	uploadID       *string
	fileEndPos     int64
	lastPartNum    int64
	lastUploadEnd  int64
	partsSize      []int64
	partsETag      []*string
	lastWriteError error
	writeEG        *errgroup.Group
	flushCnt       int
}

type s3FileHandle struct {
	mpuInfo        mpuInfo
	bucket         string
	name           string
	path           string
	size           int64
	flags          uint32
	writeTmpfile   *os.File
	canWrite       chan struct{}
	writeSrcReader io.ReadCloser
	fs             *s3FileSystem
	mu             sync.RWMutex
	writeDirty     bool
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
	log.Tracef("s3 read: fh.name[%s] len[%d] off[%d]", fh.name, len(buf), off)
	request := &s3.GetObjectInput{
		Bucket: &fh.bucket,
		Key:    aws.String(fh.path),
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
		log.Errorf("s3 read: s3.GetObject[%s] err: %v", fh.name, err)
		return nil, fuse.ToStatus(err)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		log.Errorf("s3 read: fh.name[%s]io.ReadAll err: %v", fh.name, err)
		return nil, fuse.ToStatus(err)
	}
	return fuse.ReadResultData(data), fuse.OK
}

// s3 do not support random write
func (fh *s3FileHandle) Write(data []byte, offset int64) (uint32, fuse.Status) {
	log.Tracef("s3 write: fh.name[%s] offset[%d] length[%d]", fh.name, offset, len(data))
	if fh.writeTmpfile == nil {
		log.Errorf("s3 write: fh.name[%s] failed writeTmpfile = nil", fh.name)
		return uint32(0), fuse.EIO
	}

	if fh.mpuInfo.lastWriteError != nil {
		log.Errorf("s3 write: fh.name[%s] lastWriteError:%v", fh.name, fh.mpuInfo.lastWriteError)
		return uint32(0), fuse.ToStatus(fh.mpuInfo.lastWriteError)
	}

	if fh.canWrite != nil {
		select {
		case <-fh.canWrite:
			break
		}
	}
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if !fh.writeDirty && fh.mpuInfo.fileEndPos == 0 {
		fInfo, err := fh.writeTmpfile.Stat()
		if err != nil {
			log.Errorf("s3 write: fh.name[%s] writeTmpfile.Stat err: %v", fh.name, err)
			return uint32(0), fuse.ToStatus(err)
		}
		fh.mpuInfo.fileEndPos = fInfo.Size()
		log.Tracef("s3 write: fh.name[%s] set fh.mpuInfo.fileEndPos %d", fh.name, fh.mpuInfo.fileEndPos)
	}

	n, err := fh.writeTmpfile.WriteAt(data, offset)
	if err != nil {
		log.Errorf("s3 write: fh.name[%s] WriteAt err: %v", fh.name, err)
		return uint32(0), fuse.ToStatus(err)
	}
	fh.writeDirty = true
	fh.mpuInfo.fileEndPos += int64(len(data))
	return uint32(n), fuse.OK
}

// if wrapAll == false, it will leave content not uploaded if not fill up partSize()
func (fh *s3FileHandle) serialMPUTillEnd(wrapAll bool) error {
	partSize := fh.partSize()
	for fh.mpuInfo.fileEndPos-fh.mpuInfo.lastUploadEnd >= partSize {
		log.Tracef("s3 serialMPUTillEnd front fh.name[%s], fh.mpuInfo.fileEndPos[%d], fh.mpuInfo.lastUploadEnd[%d], partSize[%d]",
			fh.name, fh.mpuInfo.fileEndPos, fh.mpuInfo.lastUploadEnd, partSize)
		start, end := fh.mpuInfo.lastUploadEnd, fh.mpuInfo.lastUploadEnd+partSize
		partNum := fh.mpuInfo.lastPartNum + 1
		// update mpu info
		fh.mpuInfo.partsSize = append(fh.mpuInfo.partsSize, partSize)
		fh.mpuInfo.lastUploadEnd = end
		fh.mpuInfo.lastPartNum = partNum
		// read file and upload
		if err := fh.readFileMPUAsync(partNum, start, end); err != nil {
			log.Debugf("s3 write: fh.name[%s], serial readFileMPUAsync[%d] [%d]-[%d] err:%v",
				fh.name, partNum, start, end, err)
			return err
		}
	}
	if wrapAll {
		log.Tracef("s3 serialMPUTillEnd wrapall fh.name[%s], fh.mpuInfo.fileEndPos[%d], fh.mpuInfo.lastUploadEnd[%d]", fh.name, fh.mpuInfo.fileEndPos, fh.mpuInfo.lastUploadEnd)
		start, end := fh.mpuInfo.lastUploadEnd, fh.mpuInfo.fileEndPos
		partNum := fh.mpuInfo.lastPartNum + 1
		// update mpu info
		fh.mpuInfo.partsSize = append(fh.mpuInfo.partsSize, fh.mpuInfo.fileEndPos-fh.mpuInfo.lastUploadEnd)
		fh.mpuInfo.lastUploadEnd = fh.mpuInfo.fileEndPos
		fh.mpuInfo.lastPartNum = partNum
		// read file and upload
		if err := fh.readFileMPUAsync(partNum, start, end); err != nil {
			log.Debugf("s3 write: fh.name[%s], wrapall readFileMPUAsync[%d] [%d]-[%d] err:%v",
				fh.name, partNum, start, end, err)
			return err
		}
	}
	return nil
}

func (fh *s3FileHandle) readFileMPUAsync(partNum, startPos, endPos int64) error {
	log.Tracef("s3 readFileMPUAsync: fh.name[%s], partNum[%d], startPos[%d], endPos[%d]",
		fh.name, partNum, startPos, endPos)
	bufSz := endPos - startPos
	buf := make([]byte, bufSz)
	if _, err := fh.writeTmpfile.ReadAt(buf, startPos); err != nil {
		log.Errorf("s3 readFileMPU: fh.name[%s], partNum: %d. writeTmpfile.ReadAt err:%v",
			fh.name, partNum, err)
		fh.mpuInfo.lastWriteError = err
		return err
	}

	fh.mpuInfo.writeEG.Go(func() error {
		if err := fh.multipartUpload(partNum, buf); err != nil {
			log.Debugf("s3 readFileMPU: fh.name[%s], multipartUpload[%d] err: %v",
				fh.name, partNum, err)
			return err
		}
		return nil
	})
	return nil
}

func (fh *s3FileHandle) Release() {
	log.Tracef("s3 release: fh.name[%s], tmpSize[%d], flushCnt[%d]",
		fh.name, fh.mpuInfo.fileEndPos, fh.mpuInfo.flushCnt)

	if err := fh.uploadWriteTmpFile(true); err != nil {
		log.Errorf("s3 release: fh.name[%s] fh.uploadWriteTmpFile err: %v", fh.name, err)
	}

	if fh.writeTmpfile != nil {
		if err := fh.writeTmpfile.Close(); err != nil {
			log.Errorf("s3 release: fh.name[%s] writeTmpfile.Close() err: %v", fh.name, err)
		}
		fh.writeTmpfile = nil
	}
}

func (fh *s3FileHandle) Flush() fuse.Status {
	fh.mpuInfo.flushCnt++
	log.Tracef("s3 flush: fh.name[%s], tmpSize[%d], flushCnt[%d]",
		fh.name, fh.mpuInfo.fileEndPos, fh.mpuInfo.flushCnt)
	return fuse.ToStatus(fh.uploadWriteTmpFile(true))
}

func (fh *s3FileHandle) uploadWriteTmpFile(commit bool) error {
	if !fh.writeDirty {
		log.Tracef("s3 uploadWriteTmpFile: fh.name[%s] writeDirty=false, no need to upload", fh.name)
		return nil
	}

	// abort mpu on error
	defer func() {
		if fh.mpuInfo.uploadID != nil && fh.mpuInfo.lastWriteError != nil {
			go func() {
				_ = fh.multipartAbort()
				fh.mpuInfo.uploadID = nil
				fh.mpuInfo.lastUploadEnd = 0
				fh.mpuInfo.lastPartNum = 0
			}()
		}
	}()

	if fh.mpuInfo.lastWriteError != nil {
		log.Debugf("s3 uploadWriteTmpFile: fh.name[%s], lastWriteError:%v, cannot upload", fh.name, fh.mpuInfo.lastWriteError)
		return fh.mpuInfo.lastWriteError
	}

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.mpuInfo.uploadID == nil {
		if fh.mpuInfo.fileEndPos > MPUThreshold {
			if err := fh.multipartCreate(); err != nil {
				log.Debugf("s3 uploadWriteTmpFile: fh.name[%s], mpu create err: %v", fh.name, err)
				return err
			}
		} else if commit {
			if fh.mpuInfo.fileEndPos == 0 {
				if err := fh.fs.putEmptyFile(fh.path); err != nil {
					log.Debugf("s3 uploadWriteTmpFile: fh.name[%s], putEmptyFile err: %v", fh.name, err)
					return err
				}
			} else if fh.mpuInfo.fileEndPos <= MPUThreshold {
				if err := fh.putFile(fh.mpuInfo.fileEndPos); err != nil {
					log.Debugf("s3 uploadWriteTmpFile: fh.name[%s], putFile length[%d] err: %v", fh.name, fh.mpuInfo.fileEndPos, err)
					return err
				}
			}
			fh.writeDirty = false
			log.Tracef("s3 uploadWriteTmpFile: fh.name[%s] commit finished", fh.name)
			return nil
		} else {
			return nil
		}
	}

	if fh.mpuInfo.uploadID == nil {
		log.Errorf("s3 uploadWriteTmpFile: fh.name[%s] mpuID nil", fh.name)
		return syscall.EIO
	}

	// read file and upload
	if err := fh.serialMPUTillEnd(commit); err != nil {
		log.Debugf("s3 flush: fh.name[%s], serialMPUTillEnd err:%v", fh.name, err)
		return err
	}
	if commit {
		if err := fh.mpuInfo.writeEG.Wait(); err != nil {
			log.Debugf("s3 flush: fh.name[%s], lastWriteError:%v, cannot flush", fh.name, fh.mpuInfo.lastWriteError)
			return fh.mpuInfo.lastWriteError
		}

		if err := fh.multipartCommit(); err != nil {
			log.Debugf("s3 flush: fh.name[%s], multipartCommit err:%v",
				fh.name, err)
			return err
		}
		fh.writeDirty = false
	}
	return nil
}

func (fh *s3FileHandle) putFile(fileSize int64) error {
	log.Tracef("s3 put: fh.name[%s],size[%d]", fh.name, fileSize)
	data := make([]byte, fileSize)
	_, err := fh.writeTmpfile.ReadAt(data, 0)
	if err != nil {
		log.Errorf("s3 put: fh.name[%s], failed reading temp file. err:%v", fh.name, err)
		return err
	}
	body := bytes.NewReader(data)
	request := &s3.PutObjectInput{
		Bucket: &fh.bucket,
		Key:    aws.String(fh.path),
		Body:   body,
	}
	_, err = fh.fs.s3.PutObject(request)
	if err != nil {
		log.Errorf("s3 putFile: s3.PutObject[%s] err: %v", fh.path, err)
	}
	return err
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
	log.Tracef("s3 truncate: fh.name[%s], size[%d]", fh.name, size)

	if fh.writeTmpfile == nil {
		log.Errorf("s3 truncate: fh.name[%s] failed writeTmpfile = nil", fh.name)
		return fuse.EIO
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
		log.Debugf("s3 truncate: fh.name[%s], writeTmpfile.Truncate err: %v", fh.name, err)
		if fh.mpuInfo.lastWriteError == nil {
			fh.mpuInfo.lastWriteError = err
		}
		return fuse.ToStatus(err)
	}
	fh.writeDirty = true
	fh.mpuInfo.fileEndPos = int64(size)
	return fuse.ToStatus(fh.uploadWriteTmpFile(true))
}

func (fh *s3FileHandle) Chmod(mode uint32) fuse.Status {
	return fuse.ToStatus(fh.fs.Chmod(fh.name, mode))
}

func (fh *s3FileHandle) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.ToStatus(fh.fs.Chown(fh.name, uid, gid))
}

func (fh *s3FileHandle) GetAttr(a *fuse.Attr) fuse.Status {
	log.Tracef("s3 getAttr: fh.name[%s] GetAttr", fh.name)
	finfo, err := fh.fs.GetAttr(fh.name)
	if err != nil {
		log.Debugf("s3 getAttr: fh.name[%s] GetAttr err : %v", fh.name, err)
		return fuse.ToStatus(err)
	}

	stat_t := finfo.Sys.(syscall.Stat_t)
	a.FromStat(&stat_t)
	return fuse.OK
}

func (fh *s3FileHandle) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.ToStatus(fh.fs.Utimens(fh.name, atime, mtime))
}

func (fh *s3FileHandle) partSize() (size int64) {
	if fh.mpuInfo.lastPartNum < 1000 {
		size = 5 * 1024 * 1024
	} else if fh.mpuInfo.lastPartNum < 2000 {
		size = 25 * 1024 * 1024
	} else {
		size = 125 * 1024 * 1024
	}
	return size
}

func NewS3FileSystem(properties map[string]interface{}) (UnderFileStorage, error) {
	log.Tracef("NewS3FileSystem: %+v", properties)
	endpoint := properties[fsCommon.Endpoint].(string)
	accessKey := properties[fsCommon.AccessKey].(string)
	secretKey := properties[fsCommon.SecretKey].(string)
	bucket := properties[fsCommon.Bucket].(string)
	region := properties[fsCommon.Region].(string)
	subpath := properties[fsCommon.SubPath].(string)

	endpoint = strings.TrimSuffix(endpoint, Delimiter)
	bucket = strings.TrimSuffix(bucket, Delimiter)
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
		chunkPool: &sync.Pool{New: func() interface{} {
			return make([]byte, MPUChunkSize)
		}},
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
			if err := fs.createEmptyDir(Delimiter); err != nil {
				log.Debugf("s3 create empty dir err: %v", err)
				return nil, err
			}
		} else {
			// 目录存在的时候，需要判断用户是否对这个目录有owner的权限
			_, _, err = fs.list(Delimiter, "", 1, true)
			if err != nil {
				log.Debugf("s3 list err: %v", err)
				return nil, err
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

	return fs, nil
}

func init() {
	RegisterUFS(fsCommon.S3Type, NewS3FileSystem)
}

// ------ mpu ------//

func (fh *s3FileHandle) multipartCreate() error {
	log.Tracef("s3 mpu create: fh.name[%s]", fh.name)

	mpu := s3.CreateMultipartUploadInput{
		Bucket: &fh.bucket,
		Key:    aws.String(fh.path),
	}
	log.Debugf("s3 mpu create: fh.name[%s], create param: %v ", fh.name, mpu)

	respCreate, err := fh.fs.s3.CreateMultipartUpload(&mpu)
	if err != nil {
		log.Errorf("s3 mpu create: fh.name[%s] create failed, err: %v ", fh.name, err)
		if fh.mpuInfo.lastWriteError == nil {
			fh.mpuInfo.lastWriteError = err
		}
		return err
	}
	if respCreate.UploadId == nil {
		err := fmt.Errorf("respCreate.UploadId nil")
		log.Errorf("s3 mpu create: fh.name[%s] create failed, err: %v ", fh.name, err)
		if fh.mpuInfo.lastWriteError == nil {
			fh.mpuInfo.lastWriteError = syscall.EAGAIN
		}
		return err
	}
	fh.mpuInfo.uploadID = respCreate.UploadId
	fh.mpuInfo.writeEG = new(errgroup.Group)
	fh.mpuInfo.partsETag = make([]*string, MPUMaxPartNum)
	log.Debugf("s3 mpu create: fh.name[%s], create resp: %v ", fh.name, respCreate)
	return nil
}

func (fh *s3FileHandle) multipartUpload(partNum int64, data []byte) error {
	mpu := s3.UploadPartInput{
		Bucket:     &fh.bucket,
		Key:        aws.String(fh.path),
		PartNumber: aws.Int64(partNum),
		UploadId:   fh.mpuInfo.uploadID,
		Body:       bytes.NewReader(data),
	}
	// retry up to 3 times if upload a mpu failed
	var err error
	var resp *s3.UploadPartOutput
	for retryNum := 0; retryNum < MPURetryTimes; retryNum++ {
		resp, err = fh.fs.s3.UploadPart(&mpu)
		if err != nil {
			log.Errorf("s3 mpu upload: fh.name[%s], upload part[%v] failed. err: %v. retryNum[%d]", fh.name, mpu, err, retryNum)
		} else {
			log.Tracef("s3 mpu upload: fh.name[%s], uploaded partNum: %d, eTag:%s, retryNum[%d]", fh.name, partNum, *resp.ETag, retryNum)
			fh.mpuInfo.partsETag[partNum-1] = resp.ETag
			return nil
		}
	}
	if err != nil && fh.mpuInfo.lastWriteError == nil {
		fh.mpuInfo.lastWriteError = err
	}
	return err
}

func (fh *s3FileHandle) multipartCommit() error {
	partCnt := fh.mpuInfo.lastPartNum
	parts := make([]*s3.CompletedPart, partCnt)
	for i := int64(0); i < partCnt; i++ {
		if fh.mpuInfo.partsETag[i] == nil {
			err := fmt.Errorf("s3 mpu partNum: %d missing ETag", i+1)
			log.Errorf("s3 mpu commit: failed: fh.name[%s], mpuID[%s]. err:%v", fh.name, *fh.mpuInfo.uploadID, err)
			return err
		}
		parts[i] = &s3.CompletedPart{
			ETag:       fh.mpuInfo.partsETag[i],
			PartNumber: aws.Int64(i + 1),
		}
	}

	commit := s3.CompleteMultipartUploadInput{
		Bucket:   &fh.bucket,
		Key:      aws.String(fh.path),
		UploadId: fh.mpuInfo.uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	}
	log.Tracef("s3 mpu commit: fh.name[%s], commit param: %v, mpuID[%s], partCnt[%d]",
		fh.name, commit, *fh.mpuInfo.uploadID, partCnt)

	respCommit, err := fh.fs.s3.CompleteMultipartUpload(&commit)
	if err != nil {
		log.Errorf("s3 mpu commit: fh.name[%s], commit failed. err: %v ", fh.name, err)
		if fh.mpuInfo.lastWriteError == nil {
			fh.mpuInfo.lastWriteError = err
		}
		return err
	}
	fh.mpuInfo.uploadID = nil
	fh.mpuInfo.partsSize = nil
	fh.mpuInfo.partsETag = nil
	log.Tracef("s3 mpu commit: fh.name[%s], commit resp: %v ", fh.name, respCommit)
	return nil
}

func (fh *s3FileHandle) multipartAbort() error {
	log.Debugf("s3 mpu abort: fh.name[%s]", fh.name)
	mpu := s3.AbortMultipartUploadInput{
		Bucket:   &fh.bucket,
		Key:      aws.String(fh.path),
		UploadId: fh.mpuInfo.uploadID,
	}
	resp, err := fh.fs.s3.AbortMultipartUpload(&mpu)
	if err != nil {
		log.Errorf("s3 mpu abort: fh.name[%s], err:%v", fh.name, err)
		return err
	}
	log.Tracef("s3 mpu abort: fh.name[%s], resp:%v", fh.name, resp)
	return nil
}