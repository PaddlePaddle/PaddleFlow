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

package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database/dbinit"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

const (
	MockRootUser = "root"
	MockFsName   = "mockFs"
)

func TestCreatePipeline(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	pwd, err := os.Getwd()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(pwd)

	createPplReq := CreatePipelineRequest{
		FsName:   MockFsName,
		UserName: "",
		YamlPath: "../../../../example/wide_and_deep/run.yaml",
		Name:     "mockPplName",
	}
	ValidateWorkflowForPipeline = func(ppl models.Pipeline) error { return nil }
	handler.ReadFileFromFs = func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) { return os.ReadFile(runYamlPath) }

	// test create
	resp, err := CreatePipeline(ctx, createPplReq)
	assert.Nil(t, err)
	assert.Equal(t, createPplReq.Name, resp.Name)

	// test get
	ppl, err := GetPipelineByID(ctx, resp.ID)
	assert.Nil(t, err)
	assert.Equal(t, resp.Name, ppl.Name)
	b, _ := json.Marshal(ppl)
	fmt.Printf("%s\n", b)

	// test create duplicates - expect fail
	_, err = CreatePipeline(ctx, createPplReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("please use existing pipeline[%s] with the same md5[%s] in fs[%s]. No need to re-create",
		ppl.ID, ppl.PipelineMd5, ppl.FsID), err)
}

func TestListPipeline(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := models.Pipeline{
		Pk:           1,
		ID:           "ppl-000001",
		Name:         "ppl-000001",
		FsID:         "fs-000001",
		FsName:       "fsname",
		UserName:     "root",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
	}
	ppl2 := models.Pipeline{
		Pk:           2,
		ID:           "ppl-000002",
		Name:         "ppl-000002",
		FsID:         "fs-000002",
		FsName:       "fsname2",
		UserName:     "root",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
	}
	ID1, err := models.CreatePipeline(ctx.Logging(), &ppl1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, ID1)

	ID2, err := models.CreatePipeline(ctx.Logging(), &ppl2)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.ID, ID2)

	// test list
	resp, err := ListPipeline(ctx, "", 10, []string{}, []string{}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(resp.PipelineList))
	b, _ := json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)
}
