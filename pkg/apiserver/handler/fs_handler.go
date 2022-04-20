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

package handler

import (
	"fmt"
	iofs "io/fs"
	"io/ioutil"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	api "paddleflow/pkg/apiserver/controller/fs"
	"paddleflow/pkg/fs/client/fs"
	"paddleflow/pkg/fs/common"
)

const (
	maxRetryCount = 3
	// unit is time.Millisecond
	sleepMillisecond = 100
)

type FsServerEmptyError struct {
}

// 用于存储文件/目录的 MTime/CTime/ATime
type PathTimeMap struct {
	PTMap map[string]time.Time
}

func (pm *PathTimeMap) WalkFunc(path string, info iofs.FileInfo, err error) error {
	if err != nil {
		return err
	}

	pm.PTMap[path] = info.ModTime()

	return nil
}

// 获取最近的时间
func (pm *PathTimeMap) LatesTime() (path string, latestTime time.Time) {
	path = ""
	latestTime = time.Time{}

	for p, t := range pm.PTMap {
		// 这里没有直接 使用 if t.After(latestTime) 的原因是： filepath.Walk 是按照字母序进行遍历，因此，采用当前这种写法，
		// 可以保证获取到的 path 是最内层路径
		if !latestTime.After(t) {
			latestTime = t
			path = p
		}
	}

	return path, latestTime
}

func (e FsServerEmptyError) Error() string {
	return fmt.Sprint("the server of fs is empty, please set the value of it")
}

var ReadFileFromFs = func(fsID, filePath string, logEntry *log.Entry) ([]byte, error) {
	fsHandle, err := NewFsHandlerWithServer(fsID, logEntry)
	if err != nil {
		logEntry.Errorf("NewFsHandler failed. err: %v", err)
		return nil, err
	}
	runYaml, err := fsHandle.ReadFsFile(filePath)
	if err != nil {
		logEntry.Errorf("NewFsHandler failed. err: %v", err)
		return nil, err
	}
	return runYaml, nil
}

type FsHandler struct {
	log      *log.Entry
	fsID     string
	fsClient fs.FSClient
}

// 方便单测
var NewFsHandlerWithServer = func(fsID string, logEntry *log.Entry) (*FsHandler, error) {
	logEntry.Debugf("begin to new a FsHandler with fsID[%s]", fsID)
	var fsClientError error = nil
	var fsHandler FsHandler
	var fsClient fs.FSClient

	for i := 0; i < maxRetryCount; i++ {
		fsClientError = nil
		fsHandler = FsHandler{
			fsID: fsID,
		}
		fsClient, fsClientError = fsHandler.getFSClient()
		if fsClientError != nil {
			logEntry.Errorf("new a FSClient with fsID[%s] failed: %v", fsID, fsClientError)
			time.Sleep(sleepMillisecond * time.Millisecond)
			continue
		}
		fsHandler.fsClient = fsClient
		fsHandler.log = logEntry
		break
	}
	if fsClientError != nil {
		return nil, fsClientError
	}
	return &fsHandler, nil
}

// 方便其余模块调用 fsHandler单测
func MockerNewFsHandlerWithServer(fsID string, logEntry *log.Entry) (*FsHandler, error) {
	os.MkdirAll("./mock_fs_handler", 0755)

	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		SubPath: "./mock_fs_handler",
	}

	fsClient, err := fs.NewFSClientForTest(testFsMeta)
	if err != nil {
		return nil, err
	}

	fsHandler := FsHandler{
		fsClient: fsClient,
		log:      logEntry,
	}

	return &fsHandler, nil
}

func (fh *FsHandler) ReadFsFile(path string) ([]byte, error) {
	fh.log.Debugf("begin to get the content of file[%s] for fsId[%s]",
		path, fh.fsID)

	Reader, err := fh.fsClient.Open(path)
	if err != nil {
		fh.log.Errorf("Read the content of file[%s] for fsID [%s] failed: %s",
			path, fh.fsID, err.Error())
		return nil, err
	}
	defer Reader.Close()

	content, err := ioutil.ReadAll(Reader)
	if err != nil {
		fh.log.Errorf("Read the content of file[%s] for fsID [%s] failed: %s",
			path, fh.fsID, err.Error())
		return nil, err
	}

	return content, nil
}

func (fh *FsHandler) Stat(path string) (os.FileInfo, error) {
	fh.log.Debugf("begin to get the stat of file[%s] with fsId[%s]",
		path, fh.fsID)

	fileInfo, err := fh.fsClient.Stat(path)
	if err != nil {
		fh.log.Errorf("get the stat of file[%s] with fsID [%s] failed: %s",
			path, fh.fsID, err.Error())
		return fileInfo, err
	}

	return fileInfo, err
}

func (fh *FsHandler) Exist(path string) (bool, error) {
	return fh.fsClient.Exist(path)
}

func (fh *FsHandler) RemoveAll(path string) error {
	return fh.fsClient.RemoveAll(path)
}

func (fh *FsHandler) MkdirAll(path string, perm os.FileMode) error {
	return fh.fsClient.MkdirAll(path, perm)
}

func (fh *FsHandler) ModTime(path string) (time.Time, error) {
	fh.log.Debugf("begin to get the modtime of file[%s] with fsId[%s]",
		path, fh.fsID)

	fileInfo, err := fh.Stat(path)
	if err != nil {
		fh.log.Debugf("get the modtime of file[%s] with fsId[%s] failed: %s",
			path, fh.fsID, err.Error())
		return time.Time{}, err
	}

	modTime := fileInfo.ModTime()
	return modTime, nil
}

// 获取 path 下所有文件和目录（包括path本身）的最新的 mtime
func (fh *FsHandler) LastModTime(path string) (time.Time, error) {
	ok, err := fh.fsClient.IsDir(path)
	if err != nil {
		fh.log.Debugf("cannot get the type of path[%s] with fsId[%s]: %s", path, fh.fsID, err.Error())
		return time.Time{}, err
	} else if !ok {
		return fh.ModTime(path)
	} else {
		pm := PathTimeMap{
			PTMap: map[string]time.Time{},
		}
		err := fh.fsClient.Walk(path, pm.WalkFunc)
		if err != nil {
			fh.log.Debugf("cannot get the latest mtime of path[%s] with fsId[%s]: %s", path, fh.fsID, err.Error())
			return time.Time{}, err
		} else {
			_, t := pm.LatesTime()
			return t, nil
		}
	}
	return time.Time{}, nil
}

func (fh *FsHandler) getFSClient() (fs.FSClient, error) {
	fsService := api.GetFileSystemService()
	fsModel, err := fsService.GetFileSystem(fh.fsID)
	if err != nil {
		log.Errorf("get file system with fsID[%s] error[%v]", fh.fsID, err)
		return nil, err
	}

	linkService := api.GetLinkService()
	listLinks, _, err := linkService.GetLink(&api.GetLinkRequest{FsID: fh.fsID})
	if err != nil {
		log.Errorf("get file system links with fsID[%s] error[%v]", fh.fsID, err)
		return nil, err
	}

	fsMeta := common.FSMeta{
		ID:            fsModel.ID,
		Name:          fsModel.Name,
		UfsType:       fsModel.Type,
		ServerAddress: fsModel.ServerAddress,
		SubPath:       fsModel.SubPath,
		Properties:    fsModel.PropertiesMap,
		Type:          common.FSType,
	}
	links := make(map[string]common.FSMeta)
	for _, link := range listLinks {
		links[link.FsPath] = common.FSMeta{
			ID:            link.ID,
			UfsType:       link.Type,
			ServerAddress: link.ServerAddress,
			SubPath:       link.SubPath,
			Properties:    link.PropertiesMap,
			Type:          common.LinkType,
		}
	}
	return fs.NewFSClient(fsMeta, links)
}
