/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package fs

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	cache "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache_new"
	kv "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv_new"
	meta "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta_new"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs_new"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	utils2 "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

func init() {
	log_ := config.ServerConfig{Log: logger.LogConfig{Level: "info"}}
	err := logger.InitStandardFileLogger(&log_.Log)
	if err != nil {
		log.Errorf("InitStandardFileLogger err: %v", err)
		panic("init logger err")
	}
}

type PFSClient struct {
	server string
	fsID   string
	pfs    *FileSystem
}

func NewPFSClient(fsMeta fsCommon.FSMeta, links map[string]fsCommon.FSMeta) (*PFSClient, error) {
	client := &PFSClient{}
	err := client.initPFS(fsMeta, links)
	return client, err
}

func NewFSClientForTest(fsMeta fsCommon.FSMeta) (*PFSClient, error) {
	vfsConfig := vfs.InitConfig(
		vfs.WithDataCacheConfig(cache.Config{
			BlockSize:    BlockSize,
			MaxReadAhead: MaxReadAheadNum,
			Expire:       DataCacheExpire,
			Config: kv.Config{
				Driver:    kv.MemType,
				CachePath: DataCachePath,
			},
		}),
		vfs.WithMetaConfig(meta.Config{
			AttrCacheExpire:  MetaCacheExpire,
			EntryCacheExpire: EntryCacheExpire,
			Config: kv.Config{
				Driver:    Driver,
				CachePath: MetaCachePath,
			},
		}),
	)
	pfs, err := NewFileSystem(fsMeta, nil, true, false, "", vfsConfig)
	if err != nil {
		log.Errorf("new a fileSystem with fsMeta [%+v] failed: %v", fsMeta, err)
		return nil, err
	}

	client := PFSClient{}
	client.pfs = pfs
	return &client, nil
}

func getMetaAndLinks(server string, fsID string) (fsCommon.FSMeta, map[string]fsCommon.FSMeta, error) {
	fsMeta := fsCommon.FSMeta{}
	httpClient, err := client.NewHttpClient(server, client.DefaultTimeOut)
	if err != nil {
		return fsMeta, nil, err
	}
	token, err := utils2.GetRootToken(&logger.RequestContext{})
	if err != nil {
		log.Errorf("get root token failed: %v", err)
		return fsMeta, nil, err
	}
	client, err := base.NewClient(fsID, httpClient, token)
	if err != nil {
		log.Errorf("init client with fs[%s] and server[%s] failed: %v", fsID, server, err)
		return fsMeta, nil, err
	}
	fsMeta, err = client.GetFSMeta()
	if err != nil {
		log.Errorf("get fsMeta from pfs server failed: %v", err)
		return fsMeta, nil, err
	}
	if fsMeta.Type == fsCommon.MockType {
		return fsMeta, nil, nil
	}
	client.FsName = fsMeta.Name
	links, err := client.GetLinks()
	if err != nil {
		log.Errorf("get links from pfs server failed: %v", err)
		return fsMeta, nil, err
	}
	return fsMeta, links, nil
}

func (c *PFSClient) initPFS(fsMeta fsCommon.FSMeta, links map[string]fsCommon.FSMeta) error {
	vfsConfig := vfs.InitConfig(
		vfs.WithDataCacheConfig(cache.Config{
			BlockSize:    BlockSize,
			MaxReadAhead: MaxReadAheadNum,
			Expire:       DataCacheExpire,
			Config: kv.Config{
				Driver:    kv.MemType,
				CachePath: DataCachePath,
			},
		}),
		vfs.WithMetaConfig(meta.Config{
			AttrCacheExpire:  MetaCacheExpire,
			EntryCacheExpire: EntryCacheExpire,
			Config: kv.Config{
				Driver: Driver,
			},
		}),
	)
	pfs, err := NewFileSystem(fsMeta, links, false, true, linkMetaDirPrefix, vfsConfig)
	if err != nil {
		log.Errorf("new a fileSystem for [%s] failed: %v", fsMeta.ID, err)
		return err
	}
	c.pfs = pfs
	return nil
}

func (c *PFSClient) Create(path string) (io.WriteCloser, error) {
	// 参考os.create
	flags := os.O_RDWR | os.O_CREATE | os.O_TRUNC
	mode := 0666
	file, err := c.pfs.Create(path, uint32(flags), uint32(mode))
	if err != nil {
		log.Errorf("create file[%s] failed: %v", path, err)
		return nil, err
	}
	return file, nil
}

func (c *PFSClient) Open(path string) (io.ReadCloser, error) {
	file, err := c.pfs.Open(path)
	if err != nil {
		log.Errorf("open file[%s] failed: %v", path, err)
		return nil, err
	}
	return file, nil
}

func (c *PFSClient) CreateFile(path string, content []byte) (int, error) {
	file, err := c.Create(path)
	if err != nil {
		log.Errorf("create file[%s] failed: %v", path, err)
		return 0, err
	}
	defer file.Close()
	return file.Write(content)
}

func (c *PFSClient) SaveFile(file io.Reader, destPath, fileName string) error {
	newFile, err := c.Create(filepath.Join(destPath, fileName))
	if err != nil {
		log.Errorf("create file[%s] failed: %v", filepath.Join(destPath, fileName), err)
		return err
	}
	defer newFile.Close()
	_, err = io.Copy(newFile, file)
	if err != nil {
		log.Errorf("copy file err:%v", err)
		return err
	}
	return err
}

func (c *PFSClient) Remove(path string) error {
	attr, err := c.pfs.Stat(path)
	if err != nil {
		log.Errorf("get stat for path[%s] failed: %v", path, err)
		return err
	}
	if attr.IsDir() {
		return c.pfs.Rmdir(path)
	}
	return c.pfs.Unlink(path)
}

func (c *PFSClient) RemoveAll(path string) error {
	return c.removeAll(path)
}

func (c *PFSClient) removeAll(path string) error {
	if path == "" {
		// fail silently to retain compatibility with previous behavior
		// of RemoveAll. See issue 28830.
		return nil
	}

	// The rmdir system call does not permit removing ".",
	// so we don't permit it either.
	if utils.EndsWithDot(path) {
		return &os.PathError{Op: "RemoveAll", Path: path, Err: syscall.EINVAL}
	}

	// Simple case: if Remove works, we're done.
	err := c.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	parent, err := c.pfs.Open(path)
	if os.IsNotExist(err) {
		// If parent does not exist, base cannot exist. Fail silently
		return nil
	}
	if err != nil {
		return err
	}
	defer parent.Close()

	dirs, err := parent.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, dir := range dirs {
		if err := c.RemoveAll(filepath.Join(path, dir)); err != nil {
			log.Errorf("remove all dir[%s] failed: %v", dir, err)
			return err
		}
	}

	// 子目录删除完成后，要删除父目录
	err = c.Remove(path)
	return err
}

func (c *PFSClient) IsDir(path string) (bool, error) {
	attr, err := c.pfs.Stat(path)
	if err != nil {
		log.Errorf("get stat for path[%s] failed: %v", path, err)
		return false, err
	}
	return attr.IsDir(), nil
}

func (c *PFSClient) Exist(path string) (bool, error) {
	_, err := c.pfs.Stat(path)
	log.Errorf("debug: client: path[%s], err: [%s]", path, err)
	if err != nil && (utils.ToSyscallErrno(err) == syscall.ENOENT ||
		utils.ToSyscallErrno(err) == syscall.ENOTDIR) {
		return false, nil
	} else if err != nil {
		log.Errorf("get stats for path[%s] failed: %v", path, err)
		return false, err
	}
	return true, nil
}

func (c *PFSClient) IsEmptyDir(path string) (bool, error) {
	file, err := c.pfs.Open(path)
	if err != nil {
		log.Errorf("open file[%s] failed: %v", path, err)
		return false, err
	}
	dirs, err := file.Readdirnames(1)
	if err != nil {
		log.Errorf("readdirnames for path[%s] failed: %v", path, err)
		return false, err
	}
	if len(dirs) == 0 {
		return true, nil
	}
	return false, nil
}

func (c *PFSClient) Mkdir(path string, perm os.FileMode) error {
	return c.pfs.Mkdir(path, perm)
}

func (c *PFSClient) MkdirAll(path string, perm os.FileMode) error {
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	log.Debugf("begin mkdirall: path[%s]", path)
	dir, err := c.pfs.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		err = c.MkdirAll(path[:j-1], perm)
		if err != nil {
			return err
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	err = c.Mkdir(path, perm)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := c.pfs.Stat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}

func (c *PFSClient) ListDir(path string) ([]os.FileInfo, error) {
	file, err := c.pfs.Open(path)
	if err != nil {
		log.Errorf("open file[%s] failed: %v", path, err)
		return []os.FileInfo{}, err
	}
	dirs, err := file.Readdir(-1)
	if err != nil {
		log.Errorf("readdir for path[%s] failed: %v", path, err)
		return []os.FileInfo{}, err
	}
	return dirs, nil
}

func (c *PFSClient) Readdirnames(path string, n int) ([]string, error) {
	file, err := c.pfs.Open(path)
	if err != nil {
		log.Errorf("open file[%s] failed: %v", path, err)
		return []string{}, err
	}
	dirs, err := file.Readdirnames(n)
	if err != nil {
		log.Errorf("readdir for path[%s] failed: %v", path, err)
		return []string{}, err
	}
	return dirs, nil
}

func (c *PFSClient) Rename(srcPath, dstPath string) error {
	return c.pfs.Rename(srcPath, dstPath)
}

func (c *PFSClient) Copy(srcPath, dstPath string) error {
	fileInfo, err := c.pfs.Stat(srcPath)
	if err != nil {
		return err
	}
	srcPath = filepath.Clean(srcPath)
	dstPath = filepath.Clean(dstPath)
	var returnErr error
	if fileInfo.IsDir() {
		returnErr = c.Walk(srcPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			destFile := strings.Replace(path, srcPath, dstPath, 1)
			if info.IsDir() {
				err = c.MkdirAll(destFile, os.ModePerm)
				if err != nil {
					return err
				}
			} else {
				destDir := filepath.Dir(destFile)
				err = c.MkdirAll(destDir, os.ModePerm)
				if err != nil {
					return err
				}
				err = c.copyFile(path, destFile)
				if err != nil {
					return err
				}
			}
			return nil
		})
	} else {
		err = c.MkdirAll(filepath.Dir(dstPath), os.ModePerm)
		if err != nil {
			return err
		}
		returnErr = c.copyFile(srcPath, dstPath)
	}
	return returnErr
}

func (c *PFSClient) copyFile(srcPath, dstPath string) error {
	srcFile, err := c.Open(srcPath)
	if err != nil {
		log.Errorf("open file[%s] failed: %s", srcPath, err)
		return err
	}
	defer srcFile.Close()
	dstFile, err := c.Create(dstPath)
	if err != nil {
		log.Errorf("create file[%s] failed: %v", dstPath, err)
		return err
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		log.Errorf("copy file from [%s] to [%s] failed: %v", srcPath, dstPath, err)
		return err
	}
	return nil
}

func (c *PFSClient) Size(path string) (int64, error) {
	attr, err := c.pfs.Stat(path)
	if err != nil {
		log.Errorf("get stat for path[%s] failed: %v", path, err)
		return 0, err
	}
	return attr.Size(), nil
}

func (c *PFSClient) Chmod(path string, fm os.FileMode) error {
	return c.pfs.Chmod(path, fm)
}

func (c *PFSClient) Walk(root string, walkFn filepath.WalkFunc) error {
	info, err := c.pfs.Stat(root)
	if err != nil {
		err = walkFn(root, nil, err)
	} else {
		err = c.walk(root, info, walkFn)
	}
	if err == filepath.SkipDir {
		return nil
	}
	return err
}

func (c *PFSClient) walk(path string, info fs.FileInfo, walkFn filepath.WalkFunc) error {
	if !info.IsDir() {
		return walkFn(path, info, nil)
	}

	names, err := c.Readdirnames(path, -1)
	err1 := walkFn(path, info, err)
	// If err != nil, walk can't walk into this directory.
	// err1 != nil means walkFn want walk to skip this directory or stop walking.
	// Therefore, if one of err and err1 isn't nil, walk will return.
	if err != nil || err1 != nil {
		// The caller's behavior is controlled by the return value, which is decided
		// by walkFn. walkFn may ignore err and return nil.
		// If walkFn returns SkipDir, it will be handled by the caller.
		// So walk should return whatever walkFn returns.
		return err1
	}

	for _, name := range names {
		filename := filepath.Join(path, name)
		fileInfo, err := c.pfs.Stat(filename)
		if err != nil {
			if err := walkFn(filename, fileInfo, err); err != nil && err != filepath.SkipDir {
				return err
			}
		} else {
			err = c.walk(filename, fileInfo, walkFn)
			if err != nil {
				if !fileInfo.IsDir() || err != filepath.SkipDir {
					return err
				}
			}
		}
	}
	return nil
}

func (c *PFSClient) Stat(path string) (os.FileInfo, error) {
	attr, err := c.pfs.Stat(path)
	if err != nil {
		log.Errorf("get stat for path[%s] failed: %v", path, err)
		return attr, err
	}
	return attr, nil
}
