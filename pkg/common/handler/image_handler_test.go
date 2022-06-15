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

package handler

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

func TestNewImageHandler(t *testing.T) {
	ServerConf := &config.ServerConfig{}
	err := config.InitConfigFromYaml(ServerConf, "../../../config/server/default/paddleserver.yaml")
	config.GlobalServerConfig = ServerConf

	handler, err := InitPFImageHandler()
	assert.Equal(t, err, nil)
	assert.Equal(t, handler.concurrency, 10)
	assert.Equal(t, handler.removeLocalImage, true)

	config.GlobalServerConfig.ImageConf.Server = ""
	handler, err = InitPFImageHandler()
	assert.Equal(t, handler.imageRepo, config.GlobalServerConfig.ImageConf.Namespace)

	config.GlobalServerConfig.ImageConf.Server = "dede"
	config.GlobalServerConfig.ImageConf.Namespace = ""
	handler, err = InitPFImageHandler()
	assert.Equal(t, handler.imageRepo, config.GlobalServerConfig.ImageConf.Server)
	// assert.Equal(t, handler.imageRepo, "registry.baidubce.com/pipeline")
}

var runid string

func callback(info ImageInfo, err error) error {
	runid = info.RunID
	fmt.Println("hahah")
	return nil
}

func NewImageHandlerForTest() (*ImageHandler, error) {
	NewFsHandlerWithServer = MockerNewFsHandlerWithServer
	ServerConf := &config.ServerConfig{}
	err := config.InitConfigFromYaml(ServerConf, "../../../config/server/default/paddleserver.yaml")
	config.GlobalServerConfig = ServerConf
	handler, err := InitPFImageHandler()
	if err != nil {
		return &ImageHandler{}, nil
	}

	return handler, err
}

func TestclassifyEnvType(t *testing.T) {
	type1 := classifyEnvType("abcd")
	assert.Equal(t, type1, RegistryUrl)

	type2 := classifyEnvType("abcd.tar")
	assert.Equal(t, type2, TarFile)
}

func TestHandleImage(t *testing.T) {
	handler, err := NewImageHandlerForTest()
	assert.Equal(t, err, nil)
	ctx := &logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}
	handler.HandleImage("abcd", "123", "456", []string{"12345"}, ctx.Logging(), callback)
	time.Sleep(1 * time.Second)
	assert.Equal(t, runid, "123")

	handler.HandleImage("abcd.tar", "1234", "456", []string{"12345"}, ctx.Logging(), callback)
}

func copyFile(src, dst string) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}

	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}

func TestGenerateImageUrl(t *testing.T) {
	handler, err := NewImageHandlerForTest()
	assert.Equal(t, err, nil)
	requestContext := &logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}

	info := imageHandleInfo{
		runID:     "1224",
		fsID:      "fs-root-sftpahz1",
		dockerEnv: "./k8s_pause.tar",
		logEntry:  requestContext.Logging(),
	}
	url := handler.generateImageUrl(info, "12348999090")
	fmt.Println(url)
}

/********
func TestLoadAndTagImage(t *testing.T) {
	handler, err := NewImageHandlerForTest()
	assert.Equal(t, err, nil)
	requestContext := &logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}

	info := imageHandleInfo{
		runID:     "1224",
		fsID:      "fs-root-sftpahz1",
		fsHost:    "10.21.195.71",
		fsRpcPort: 8082,
		dockerEnv: "./golang.tar",
		reqCtx:    requestContext,
	}

	Url, err := handler.loadAndTagImage(info, "8735189b1527")
	assert.Equal(t, err, nil)
	assert.NotEqual(t, Url, "")
}

func TestPushImage(t *testing.T) {
	handler, err := NewImageHandlerForTest()
	assert.Equal(t, err, nil)

	requestContext := &logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}

	err = handler.pushImage(requestContext, "registry.baidubce.com/pipeline/golang:20210916")
	assert.Equal(t, err, nil)

	err = handler.pushImage(requestContext, "registry.baidubce.com/pipeline/golang:20210913dede")
	assert.NotEqual(t, err, nil)

}

func TestRemoveImage(t *testing.T) {
	fmt.Print("Remove")
	handler, err := NewImageHandlerForTest()
	assert.Equal(t, err, nil)

	requestContext := &logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}

	handler.removeLocalImage = true

	IDs := []string{"registry.baidubce.com/pipeline/golang:20210916", "registry.baidubce.com/pipeline/pf/8735189b/fs_root_sftpahz1/golang_tar:1631803032706"}
	handler.RemoveImage(requestContext, IDs)
}

func TestHandleImageConfig(t *testing.T) {
	// 需要在本地准备测试物料: 1、创建 testdata 目录，2、执行 docker save k8s.gcr.io/pause:3.2 -o k8s_pause.tar
	handler, err := NewImageHandlerForTest()
	assert.Equal(t, err, nil)

	err = copyFile("testdata/k8s_pause.tar", "mock_fs_handler/k8s_pause.tar")
	assert.Equal(t, err, nil)

	requestContext := &logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}

	info := imageHandleInfo{
		runID:     "1224",
		fsID:      "fs-root-sftpahz1",
		fsHost:    "10.21.195.71",
		fsRpcPort: 8082,
		dockerEnv: "./k8s_pause.tar",
		reqCtx:    requestContext,
	}
	conf, err := handler.handleImageConfig(info)
	assert.Equal(t, conf.Config[0:64], "80d28bedfe5dec59da9ebf8e6260224ac9008ab5c11dbbe16ee3ba3e4439ac2c")

	// 一个tar包 有多个镜像，需要准备物料
	info2 := imageHandleInfo{
		runID:     "1224",
		fsID:      "fs-root-sftpahz1",
		fsHost:    "10.21.195.71",
		fsRpcPort: 8082,
		dockerEnv: "./mutli_docker.tar",
		reqCtx:    requestContext,
	}

	_, err = handler.handleImageConfig(info2)
	assert.NotEqual(t, err, nil)

	err = copyFile("../../pipeline/testcase/run.yaml", "mock_fs_handler/run.yaml")
	assert.Equal(t, err, nil)

	info3 := imageHandleInfo{
		runID:     "1224",
		fsID:      "fs-root-sftpahz1",
		fsHost:    "10.21.195.71",
		fsRpcPort: 8082,
		dockerEnv: "./run.yaml",
		reqCtx:    requestContext,
	}

	_, err = handler.handleImageConfig(info3)
	assert.NotEqual(t, err, nil)
}

func TestGenerateRemoveTags(t *testing.T) {
	handler, err := NewImageHandlerForTest()
	assert.Equal(t, err, nil)

	imageConfig := ImageConfig{
		Config:   "80d28bedfe5dec59da9ebf8e6260224ac9008ab5c11dbbe16ee3ba3e4439ac2c.json",
		RepoTags: []string{"abcd:123", "dede:ede", "k8s.gcr.io/pause:3.2"},
	}

	requestContext := &logger.RequestContext{
		RequestID: "abcde",
		UserName:  "xiaodu",
		ErrorCode: "0",
	}

	tags := handler.generateRemoveTags(requestContext, imageConfig)
	assert.Equal(t, tags, []string{"abcd:123", "dede:ede"})
}
***************/
