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

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database/dbinit"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	pkgPipeline "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	pkgPplCommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

// 测试创建pipeline
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
		Name:     "distribute_wide_and_deep",
		Desc:     "pipeline test",
	}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(pkgPipeline.NewWorkflow, func(wfSource schema.WorkflowSource, runID, entry string, params map[string]interface{}, extra map[string]string,
		callbacks pkgPipeline.WorkflowCallbacks) (*pkgPipeline.Workflow, error) {
		return &pkgPipeline.Workflow{}, nil
	})
	defer patch1.Reset()

	// test create 失败，pplname 和 wfsname 不一致
	createPplReq.Name = "wrongPplName"
	resp, err := CreatePipeline(ctx, createPplReq, MockFsID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("validateWorkflowForPipeline failed. err:pplName[wrongPplName] in request is not the same as name[distribute_wide_and_deep] in pipeline yaml."), err)

	// create 成功
	createPplReq.Name = "distribute_wide_and_deep"
	resp, err = CreatePipeline(ctx, createPplReq, MockFsID)
	assert.Nil(t, err)
	assert.Equal(t, createPplReq.Name, resp.Name)

	// test get success
	getPplResp, err := GetPipeline(ctx, resp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineDetailList), 1)
	assert.Equal(t, getPplResp.PipelineDetailList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[0].Pk, int64(1))

	fmt.Printf("=========================\n=========================\n")
	b, _ := json.Marshal(getPplResp.Pipeline)
	fmt.Printf("%s\n", b)
	b, _ = json.Marshal(getPplResp.PipelineDetailList)
	fmt.Printf("%s\n", b)
	fmt.Printf("\n=========================\n=========================\n")

	// test create 失败，重复创建
	_, err = CreatePipeline(ctx, createPplReq, MockFsID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("CreatePipeline failed: user[root] already has pipeline[distribute_wide_and_deep], cannot create again!"), err)

	// 更改用户名后，创建成功
	ctx = &logger.RequestContext{UserName: "another_user"}
	createPplReq.Name = "distribute_wide_and_deep"
	resp, err = CreatePipeline(ctx, createPplReq, MockFsID)
	assert.Nil(t, err)
	assert.Equal(t, createPplReq.Name, resp.Name)

	// test get success
	getPplResp, err = GetPipeline(ctx, resp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineDetailList), 1)
	assert.Equal(t, getPplResp.PipelineDetailList[0].PipelineID, "ppl-000002")
	assert.Equal(t, getPplResp.PipelineDetailList[0].Pk, int64(2))
}

// 测试更新pipeline
func TestUpdatePipeline(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: "normalUser"}

	createPplReq := CreatePipelineRequest{
		FsName:   MockFsName,
		UserName: "",
		YamlPath: "../../../../example/wide_and_deep/run.yaml",
		Name:     "distribute_wide_and_deep",
		Desc:     "pipeline test",
	}

	pipelineID := "ppl-000001"
	updatePplReq := UpdatePipelineRequest{
		FsName:   MockFsName,
		UserName: "",
		YamlPath: "../../../../example/wide_and_deep/run.yaml",
		Desc:     "pipeline test",
	}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(pkgPipeline.NewWorkflow, func(wfSource schema.WorkflowSource, runID, entry string, params map[string]interface{}, extra map[string]string,
		callbacks pkgPipeline.WorkflowCallbacks) (*pkgPipeline.Workflow, error) {
		return &pkgPipeline.Workflow{}, nil
	})
	defer patch1.Reset()

	// test update 失败，pipeline没有创建，不能更新
	resp, err := UpdatePipeline(ctx, updatePplReq, pipelineID, MockFsID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("UpdatePipeline failed: pipeline[distribute_wide_and_deep] not created for user[normalUser], pls create first!"), err)

	// create 成功
	createPplResp, err := CreatePipeline(ctx, createPplReq, MockFsID)
	assert.Nil(t, err)
	assert.Equal(t, createPplReq.Name, createPplResp.Name)

	// test get success
	getPplResp, err := GetPipeline(ctx, createPplResp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineDetailList), 1)
	assert.Equal(t, getPplResp.PipelineDetailList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[0].Pk, int64(1))

	// update 失败，yaml name 与 pipeline记录中的 name 不一样
	updatePplReq.YamlPath = "../../../../example/pipeline/base_pipeline/run.yaml"
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID, MockFsID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("update pipeline failed, pplname[base_pipeline] in yaml not the same as [distribute_wide_and_deep] of pipeline[ppl-000001]"), err)

	// update 成功
	updatePplReq.YamlPath = "../../../../example/wide_and_deep/run.yaml"
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID, MockFsID)
	assert.Nil(t, err)
	assert.Equal(t, createPplResp.PipelineID, resp.PipelineID)

	// 其他用户，update失败
	ctx = &logger.RequestContext{UserName: "anotherUser"}
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID, MockFsID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("user[anotherUser] has no access to resource[pipeline] with Name[ppl-000001]"), err)

	// root用户，update成功
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID, MockFsID)
	assert.Nil(t, err)
	assert.Equal(t, createPplResp.PipelineID, resp.PipelineID)

	// root用户，test get success
	getPplResp, err = GetPipeline(ctx, createPplResp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineDetailList), 3)
	assert.Equal(t, getPplResp.PipelineDetailList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[0].Pk, int64(1))
	assert.Equal(t, getPplResp.PipelineDetailList[1].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[1].Pk, int64(2))
	assert.Equal(t, getPplResp.PipelineDetailList[2].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[2].Pk, int64(3))

	// root用户，test get success
	ctx = &logger.RequestContext{UserName: "normalUser"}
	getPplResp, err = GetPipeline(ctx, createPplResp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineDetailList), 3)
	assert.Equal(t, getPplResp.PipelineDetailList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[0].Pk, int64(1))
	assert.Equal(t, getPplResp.PipelineDetailList[1].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[1].Pk, int64(2))
	assert.Equal(t, getPplResp.PipelineDetailList[2].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineDetailList[2].Pk, int64(3))
}

func TestListPipeline(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	ppl2 := models.Pipeline{
		Pk:       2,
		ID:       "ppl-000002",
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "root",
	}
	pplDetail2 := models.PipelineDetail{
		Pk:           2,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "root",
		Pipeline:     ppl2,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	pplID2, pplDetailPk2, err := models.CreatePipeline(ctx.Logging(), &pplDetail2)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.ID, pplID2)
	assert.Equal(t, pplDetail2.Pk, pplDetailPk2)

	// test list
	resp, err := ListPipeline(ctx, "", 10, []string{}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.PipelineList[1].ID, "ppl-000002")
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ := json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定maxkeys
	resp, err = ListPipeline(ctx, "", 1, []string{}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.IsTruncated, true)
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定userfilter
	resp, err = ListPipeline(ctx, "", 10, []string{"user1", "user2"}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.IsTruncated, true)
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定返回包含最后一条记录的pipeline记录集合，导致istruncated = false
	resp, err = ListPipeline(ctx, "", 10, []string{"root"}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000002")
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, namefilter
	// 先测试不能匹配前缀，注意不存在匹配记录时，istruncated = false
	resp, err = ListPipeline(ctx, "", 1, []string{}, []string{"ppl"})
	assert.Nil(t, err)
	assert.Equal(t, 0, len(resp.PipelineList))
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// 再测试精确匹配前缀，能够生效
	resp, err = ListPipeline(ctx, "", 1, []string{}, []string{"ppl1"})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.IsTruncated, true)
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list，user非root时，无法指定userfilter，只返回自己有权限的pipeline
	ctx = &logger.RequestContext{UserName: "user1"}
	resp, err = ListPipeline(ctx, "", 10, []string{"root"}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.PipelineList[0].UserName, "user1")
	assert.Equal(t, resp.IsTruncated, true)
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)
}

func TestGetPipeline(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	pplDetail2 := models.PipelineDetail{
		Pk:           2,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	pplID2, pplDetailPk2, err := models.CreatePipeline(ctx.Logging(), &pplDetail2)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID2)
	assert.Equal(t, pplDetail2.Pk, pplDetailPk2)

	// test get pipeline 失败，pipeline id不存在
	resp, err := GetPipeline(ctx, "wrongPplID", "", 10, []string{})
	assert.NotNil(t, err)
	assert.Equal(t, "get pipeline[wrongPplID] failed, err: record not found", err.Error())
	b, _ := json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline 失败，用户没有权限
	ctx = &logger.RequestContext{UserName: "user2"}
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{})
	assert.NotNil(t, err)
	assert.Equal(t, "user[user2] has no access to resource[pipeline] with Name[ppl-000001]", err.Error())
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline, 指定maxkeys
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 2, len(resp.PipelineDetailList))
	assert.Equal(t, resp.PipelineDetailList[0].Pk, int64(1))
	assert.Equal(t, resp.PipelineDetailList[1].Pk, int64(2))
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test get pipeline, 指定fsfilter
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{"fsname"})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 1, len(resp.PipelineDetailList))
	assert.Equal(t, resp.PipelineDetailList[0].Pk, int64(1))
	assert.Equal(t, resp.IsTruncated, true)
	assert.NotEqual(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test get pipeline, 指定返回包含最后一条记录的pipeline记录集合，导致istruncated = false
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{"fsname2"})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 1, len(resp.PipelineDetailList))
	assert.Equal(t, resp.PipelineDetailList[0].Pk, int64(2))
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test get pipeline, 没有pipeline detail匹配时，istruncated=false
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{"fsname2"})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 1, len(resp.PipelineDetailList))
	assert.Equal(t, resp.PipelineDetailList[0].Pk, int64(2))
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)
}

func TestGetPipelineDetail(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	pplDetail2 := models.PipelineDetail{
		Pk:           2,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	pplID2, pplDetailPk2, err := models.CreatePipeline(ctx.Logging(), &pplDetail2)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID2)
	assert.Equal(t, pplDetail2.Pk, pplDetailPk2)

	// test get pipeline detail 失败，pipeline id不存在
	resp, err := GetPipelineDetail(ctx, "wrongPplID", 1)
	assert.NotNil(t, err)
	assert.Equal(t, "get pipeline[wrongPplID] failed, err: record not found", err.Error())
	b, _ := json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline detail 失败，用户没有权限
	ctx = &logger.RequestContext{UserName: "user2"}
	resp, err = GetPipelineDetail(ctx, "ppl-000001", 1)
	assert.NotNil(t, err)
	assert.Equal(t, "user[user2] has no access to resource[pipeline] with Name[ppl-000001]", err.Error())
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline detail 失败, detailPk 不存在
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = GetPipelineDetail(ctx, "ppl-000001", 3)
	assert.NotNil(t, err)
	assert.Equal(t, "get pipeline detail[3] failed, err: record not found", err.Error())
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline detail 成功
	resp, err = GetPipelineDetail(ctx, "ppl-000001", 1)
	assert.Nil(t, err)
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)
}

// todo: 测试有schedule在运行的场景（不能删除）
func TestDeletePipeline(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	// 创建pipeline前，test delete pipeline 失败，pipeline id不存在
	err := DeletePipeline(ctx, "wrongPplID")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[wrongPplID] failed. not exist", err.Error())

	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	ppl2 := models.Pipeline{
		Pk:       2,
		ID:       "ppl-000002",
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "user2",
	}
	pplDetail2 := models.PipelineDetail{
		Pk:           2,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user2",
		Pipeline:     ppl2,
	}

	ppl3 := models.Pipeline{
		Pk:       3,
		ID:       "ppl-000003",
		Name:     "ppl3",
		Desc:     "ppl3",
		UserName: "root",
	}
	pplDetail3 := models.PipelineDetail{
		Pk:           3,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname3",
		FsName:       "fsname3",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_3",
		UserName:     "root",
		Pipeline:     ppl3,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	pplID2, pplDetailPk2, err := models.CreatePipeline(ctx.Logging(), &pplDetail2)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.ID, pplID2)
	assert.Equal(t, pplDetail2.Pk, pplDetailPk2)

	pplID3, pplDetailPk3, err := models.CreatePipeline(ctx.Logging(), &pplDetail3)
	assert.Nil(t, err)
	assert.Equal(t, ppl3.ID, pplID3)
	assert.Equal(t, pplDetail3.Pk, pplDetailPk3)

	// test delete pipeline 失败，用户没有权限
	ctx = &logger.RequestContext{UserName: "user2"}
	err = DeletePipeline(ctx, "ppl-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] failed. Access denied", err.Error())

	// test delete pipeline 成功
	ctx = &logger.RequestContext{UserName: "user1"}
	err = DeletePipeline(ctx, "ppl-000001")
	assert.Nil(t, err)

	// 再次删除，pipeline不存在，删除失败
	err = DeletePipeline(ctx, "ppl-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] failed. not exist", err.Error())

	// test delete pipeline 成功，root用户能够删除自己创建的pipeline
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = DeletePipeline(ctx, "ppl-000002")
	assert.Nil(t, err)

	err = DeletePipeline(ctx, "ppl-000002")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000002] failed. not exist", err.Error())

	// test delete pipeline 成功，root用户也能删除别人创建的pipeline
	err = DeletePipeline(ctx, "ppl-000003")
	assert.Nil(t, err)

	err = DeletePipeline(ctx, "ppl-000003")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000003] failed. not exist", err.Error())
}

// todo: 测试有schedule在运行的场景（不能删除）
func TestDeletePipelineDetail(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	pplDetail2 := models.PipelineDetail{
		Pk:           2,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "user1-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	pplDetail3 := models.PipelineDetail{
		Pk:           3,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "user1-fsname3",
		FsName:       "fsname3",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_3",
		UserName:     "user1",
		Pipeline:     ppl1,
	}

	ppl2 := models.Pipeline{
		Pk:       2,
		ID:       "ppl-000002",
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "root",
	}

	pplDetail4 := models.PipelineDetail{
		Pk:           4,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname4",
		FsName:       "fsname4",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_4",
		UserName:     "root",
		Pipeline:     ppl2,
	}

	pplDetail5 := models.PipelineDetail{
		Pk:           5,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname5",
		FsName:       "fsname5",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_5",
		UserName:     "root",
		Pipeline:     ppl2,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	pplID2, pplDetailPk2, err := models.CreatePipeline(ctx.Logging(), &pplDetail2)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID2)
	assert.Equal(t, pplDetail2.Pk, pplDetailPk2)

	pplID3, pplDetailPk3, err := models.CreatePipeline(ctx.Logging(), &pplDetail3)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID3)
	assert.Equal(t, pplDetail3.Pk, pplDetailPk3)

	pplID4, pplDetailPk4, err := models.CreatePipeline(ctx.Logging(), &pplDetail4)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.ID, pplID4)
	assert.Equal(t, pplDetail4.Pk, pplDetailPk4)

	pplID5, pplDetailPk5, err := models.CreatePipeline(ctx.Logging(), &pplDetail5)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.ID, pplID5)
	assert.Equal(t, pplDetail5.Pk, pplDetailPk5)

	// test delete pipeline detail 失败，pipeline记录不存在
	err = DeletePipelineDetail(ctx, "ppl-000003", 1)
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline detail failed. pipeline[ppl-000003] not exist", err.Error())

	// test delete pipeline detail 失败，用户没有权限
	ctx = &logger.RequestContext{UserName: "user2"}
	err = DeletePipelineDetail(ctx, "ppl-000001", 1)
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline detail[1] of pipeline[ppl-000001] failed. Access denied", err.Error())

	// delete pipeline detail 失败，pipeline detail不存在，删除失败
	ctx = &logger.RequestContext{UserName: "user1"}
	err = DeletePipelineDetail(ctx, "ppl-000001", 6)
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline detail failed. pipeline detail[6] not exist", err.Error())

	// test delete pipeline detail 成功
	err = DeletePipelineDetail(ctx, "ppl-000001", 1)
	assert.Nil(t, err)

	// 再次删除，pipeline不存在，删除失败
	err = DeletePipelineDetail(ctx, "ppl-000001", 1)
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline detail failed. pipeline detail[1] not exist", err.Error())

	// test delete pipeline detail 成功，root用户能够删除自己创建的pipeline
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = DeletePipelineDetail(ctx, "ppl-000002", 4)
	assert.Nil(t, err)

	err = DeletePipelineDetail(ctx, "ppl-000002", 4)
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline detail failed. pipeline detail[4] not exist", err.Error())

	// test delete pipeline 成功，root用户也能删除别人创建的pipeline
	err = DeletePipelineDetail(ctx, "ppl-000001", 2)
	assert.Nil(t, err)

	err = DeletePipelineDetail(ctx, "ppl-000001", 2)
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline detail failed. pipeline detail[2] not exist", err.Error())

	// 当pipeline只有一个detail时，不能删除detail，只能删除pipeline
	err = DeletePipelineDetail(ctx, "ppl-000002", 5)
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline detail[5] failed. only one pipeline detail left, pls delete pipeline instead", err.Error())
}
