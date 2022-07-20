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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

func prepareTestEnv() (fs.FSClient, *logger.RequestContext, error) {
	os.RemoveAll("./mock_fs_handler")
	os.MkdirAll("./mock_fs_handler", 0755)
	os.MkdirAll("./mock_fs_handler/test_path_time/path_time/time", 0755)
	os.MkdirAll("./mock_fs_handler/test_path_time2", 0755)

	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		SubPath: "./mock_fs_handler",
	}

	requestContext := logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}

	fsClient, err := fs.NewFSClientForTest(testFsMeta)
	writerCloser, err := fsClient.Create("./run.yaml")
	if err != nil {
		return nil, nil, err
	}

	defer writerCloser.Close()

	_, err = writerCloser.Write([]byte("test paddleflow pipeline"))
	if err != nil {
		return nil, nil, err
	}

	writerCloser2, err := fsClient.Create("./test_path_time/path_time/time/a.txt")
	if err != nil {
		return nil, nil, err
	}

	defer writerCloser2.Close()

	_, err = writerCloser2.Write([]byte("test paddleflow pipeline"))
	if err != nil {
		return nil, nil, err
	}

	return fsClient, &requestContext, err
}

func TestReadFsFile(t *testing.T) {
	fsClient, requestContext, err := prepareTestEnv()
	assert.Equal(t, err, nil)

	fsHandler := FsHandler{
		fsClient: fsClient,
		log:      logger.LoggerForRequest(requestContext),
	}

	content, err := fsHandler.ReadFsFile("./run.yaml")
	assert.Equal(t, err, nil)

	assert.NotEqual(t, len(content), 0)
}

func TestStat(t *testing.T) {
	fsClient, requestContext, err := prepareTestEnv()
	assert.Equal(t, err, nil)

	fsHandler := FsHandler{
		fsClient: fsClient,
		log:      logger.LoggerForRequest(requestContext),
	}

	fileInfo, err := fsHandler.Stat("./run.yaml")
	assert.Equal(t, fileInfo.Name(), "run.yaml")
	fmt.Println(fileInfo.ModTime())
}

func TestModTime(t *testing.T) {
	fsClient, requestContext, err := prepareTestEnv()
	assert.Equal(t, err, nil)

	fsHandler := FsHandler{
		fsClient: fsClient,
		log:      logger.LoggerForRequest(requestContext),
	}

	modTime, err := fsHandler.ModTime("/run.yaml")
	assert.Equal(t, err, nil)
	fmt.Println("modTime by testModTime", modTime)
}

func TestWalkFunc(t *testing.T) {
	_, _, err := prepareTestEnv()
	assert.Equal(t, err, nil)

	pt := PathTimeMap{
		PTMap: map[string]time.Time{},
	}

	filepath.Walk("mock_fs_handler", pt.WalkFunc)
	// fmt.Println(pt.PTMap)

	assert.Equal(t, 7, len(pt.PTMap))

	_, ok := pt.PTMap["mock_fs_handler"]
	assert.Equal(t, true, ok)

	_, ok = pt.PTMap["mock_fs_handler/test_path_time/path_time/time"]
	assert.Equal(t, true, ok)

	_, ok = pt.PTMap["mock_fs_handler/test_path_time/path_time/time"]
	assert.Equal(t, true, ok)
}

func TestLatestTime(t *testing.T) {
	fsClient, requestContext, err := prepareTestEnv()
	assert.Equal(t, err, nil)

	pt := PathTimeMap{
		PTMap: map[string]time.Time{},
	}

	filepath.Walk("mock_fs_handler", pt.WalkFunc)
	fmt.Println("PathToMtime: ", pt.PTMap)

	p, lt := pt.LatestTime()
	fmt.Println("latest:", p, lt)

	fsHandler := FsHandler{
		fsClient: fsClient,
		log:      logger.LoggerForRequest(requestContext),
	}

	modTime, err := fsHandler.ModTime("test_path_time/path_time/time/a.txt")

	fi, err := os.Lstat("mock_fs_handler/test_path_time/path_time/time/a.txt")
	assert.Equal(t, lt, fi.ModTime())

	fmt.Println(fi.ModTime())
	fmt.Println(modTime)
}

func TestLastModTime(t *testing.T) {
	fsClient, requestContext, err := prepareTestEnv()
	assert.Equal(t, err, nil)

	fsHandler := FsHandler{
		fsClient: fsClient,
		log:      logger.LoggerForRequest(requestContext),
	}

	modTime, err := fsHandler.LastModTime("test_path_time")
	assert.Equal(t, nil, err)

	fi, err := os.Lstat("mock_fs_handler/test_path_time/path_time/time/a.txt")
	assert.Equal(t, modTime.UnixNano(), fi.ModTime().UnixNano())
}
