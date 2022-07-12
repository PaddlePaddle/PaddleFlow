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

package common

import (
	"fmt"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fs"
	fscommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/stretchr/testify/assert"
)

func CreatefileByFsClient(path string, isDir bool) error {
	testFsMeta := fscommon.FSMeta{
		UfsType: fscommon.LocalType,
		SubPath: "./mock_fs_handler",
	}
	fsClient, err := fs.NewFSClientForTest(testFsMeta)
	if !isDir {
		writerCloser, err := fsClient.Create(path)
		if err != nil {
			return err
		}

		defer writerCloser.Close()

		_, err = writerCloser.Write([]byte("test paddleflow pipeline"))
		return err
	} else {
		err = fsClient.MkdirAll(path, 0777)
		return err
	}
}

func TestCalculateFingerprint(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	handler.NewFsHandlerWithServer("xx", logger.LoggerForRun("common"))

	err := CreatefileByFsClient("/art1.txt", false)
	if err != nil {
		fmt.Println(err.Error())
	}

	assert.Equal(t, err, nil)

	// 符合预期
	content, err := GetArtifactContent("art1.txt", 1024, "xx", logger.LoggerForRun("common"))

	assert.Equal(t, nil, err)
	assert.Equal(t, "test paddleflow pipeline", content)

	// 内容过大
	content, err = GetArtifactContent("art1.txt", 10, "xx", logger.LoggerForRun("common"))
	assert.NotEqual(t, nil, err)

	// 测试目录
	err = CreatefileByFsClient("/art2", true)
	assert.Equal(t, err, nil)

	content, err = GetArtifactContent("art2", 10, "xx", logger.LoggerForRun("common"))
	assert.NotEqual(t, nil, err)
	fmt.Println(err.Error())
}
