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

package fs

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/utils"
)

type MockClient struct {
	pathPrefix string
}

func (c *MockClient) Create(path string) (io.WriteCloser, error) {
	return os.Create(filepath.Join(c.pathPrefix, path))
}

func (c *MockClient) Open(path string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(c.pathPrefix, path))
}

func (c *MockClient) CreateFile(path string, content []byte) (int, error) {
	file, err := os.Create(filepath.Join(c.pathPrefix, path))
	if err != nil {
		log.Errorf("create file[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return 0, err
	}
	defer file.Close()
	return file.Write(content)
}

func (c *MockClient) SaveFile(file io.Reader, destPath, fileName string) error {
	newFile, err := os.Create(filepath.Join(c.pathPrefix, destPath, fileName))
	if err != nil {
		log.Errorf("create file[%s] failed: %v", filepath.Join(c.pathPrefix, destPath, fileName), err)
		return err
	}
	_, err = io.Copy(newFile, file)
	return err
}

func (c *MockClient) Remove(path string) error {
	return os.Remove(filepath.Join(c.pathPrefix, path))
}

func (c *MockClient) RemoveAll(path string) error {
	return os.RemoveAll(filepath.Join(c.pathPrefix, path))
}

func (c *MockClient) IsDir(path string) (bool, error) {
	attr, err := os.Stat(filepath.Join(c.pathPrefix, path))
	if err != nil {
		log.Errorf("get stat for path[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return false, err
	}
	return attr.IsDir(), nil
}

func (c *MockClient) Exist(path string) (bool, error) {
	_, err := os.Stat(filepath.Join(c.pathPrefix, path))
	if err != nil && (utils.ToSyscallErrno(err) == syscall.ENOENT ||
		utils.ToSyscallErrno(err) == syscall.ENOTDIR) {
		return false, nil
	} else if err != nil {
		log.Errorf("get stats for path[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return false, err
	}
	return true, nil
}

func (c *MockClient) IsEmptyDir(path string) (bool, error) {
	file, err := os.Open(filepath.Join(c.pathPrefix, path))
	if err != nil {
		log.Errorf("open file[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return false, err
	}
	dirs, err := file.Readdirnames(1)
	if err != nil && err != io.EOF {
		log.Errorf("readdirnames for path[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return false, err
	}
	if len(dirs) == 0 {
		return true, nil
	}
	return false, nil
}

func (c *MockClient) Mkdir(path string, perm os.FileMode) error {
	return os.Mkdir(filepath.Join(c.pathPrefix, path), perm)
}

func (c *MockClient) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(filepath.Join(c.pathPrefix, path), perm)
}

func (c *MockClient) ListDir(path string) ([]os.FileInfo, error) {
	file, err := os.Open(filepath.Join(c.pathPrefix, path))
	if err != nil {
		log.Errorf("open file[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return []os.FileInfo{}, err
	}
	dirs, err := file.Readdir(-1)
	if err != nil {
		log.Errorf("readdir for path[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return []os.FileInfo{}, err
	}
	return dirs, nil
}

func (c *MockClient) Readdirnames(path string, n int) ([]string, error) {
	file, err := os.Open(filepath.Join(c.pathPrefix, path))
	if err != nil {
		log.Errorf("open file[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return []string{}, err
	}
	dirs, err := file.Readdirnames(n)
	if err != nil {
		log.Errorf("readdir for path[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return []string{}, err
	}
	return dirs, nil
}

func (c *MockClient) Rename(srcPath, dstPath string) error {
	return os.Rename(filepath.Join(c.pathPrefix, srcPath), filepath.Join(c.pathPrefix, dstPath))
}

func (c *MockClient) Copy(srcPath, dstPath string) error {
	fileInfo, err := os.Stat(filepath.Join(c.pathPrefix, srcPath))
	if err != nil {
		return err
	}
	srcPath = filepath.Clean(filepath.Join(c.pathPrefix, srcPath))
	dstPath = filepath.Clean(filepath.Join(c.pathPrefix, dstPath))
	var returnErr error
	if fileInfo.IsDir() {
		returnErr = filepath.Walk(srcPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			destFile := strings.Replace(path, srcPath, dstPath, 1)
			if info.IsDir() {
				err = os.MkdirAll(destFile, os.ModePerm)
				if err != nil {
					return err
				}
			} else {
				destDir := filepath.Dir(destFile)
				err = os.MkdirAll(destDir, os.ModePerm)
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
		err = os.MkdirAll(filepath.Dir(dstPath), os.ModePerm)
		if err != nil {
			return err
		}
		returnErr = c.copyFile(srcPath, dstPath)
	}
	return returnErr
}

func (c *MockClient) copyFile(srcPath, dstPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		log.Errorf("open file[%s] failed: %s", srcPath, err)
		return err
	}
	defer srcFile.Close()
	dstFile, err := os.Create(dstPath)
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

func (c *MockClient) Size(path string) (int64, error) {
	attr, err := os.Stat(filepath.Join(c.pathPrefix, path))
	if err != nil {
		log.Errorf("get stat for path[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return 0, err
	}
	return attr.Size(), nil
}

func (c *MockClient) Chmod(path string, fm os.FileMode) error {
	return os.Chmod(filepath.Join(c.pathPrefix, path), fm)
}

func (c *MockClient) Chown(name string, uid, gid int) error {
	return os.Chown(name, uid, gid)
}

func (c *MockClient) Walk(root string, walkFn filepath.WalkFunc) error {
	return nil
}

func (c *MockClient) Stat(path string) (os.FileInfo, error) {
	attr, err := os.Stat(filepath.Join(c.pathPrefix, path))
	if err != nil {
		log.Errorf("get stat for path[%s] failed: %v", filepath.Join(c.pathPrefix, path), err)
		return attr, err
	}
	return attr, nil
}
