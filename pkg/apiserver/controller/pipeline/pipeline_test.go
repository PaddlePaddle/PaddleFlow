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
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	pkgPipeline "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

// 测试创建pipeline
// yaml结构校验跟run相同，所以此处略过
// todo: fs路径不存在 & 访问权限(需要挂载，不好测试)
func TestCreatePipeline(t *testing.T) {
	driver.InitMockDB()
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
		Desc:     "pipeline test",
	}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile("../../../../example/wide_and_deep/run.yaml")
	})
	defer patch.Reset()

	patch1 := gomonkey.ApplyFunc(pkgPipeline.NewWorkflow, func(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string,
		callbacks pkgPipeline.WorkflowCallbacks) (*pkgPipeline.Workflow, error) {
		return &pkgPipeline.Workflow{}, nil
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})

	defer patch2.Reset()

	// 创建失败: desc长度超过256
	createPplReq.Desc = strings.Repeat("a", util.MaxDescLength+1)
	_, err = CreatePipeline(ctx, createPplReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("desc too long, should be less than %d", util.MaxDescLength), err)

	// 创建失败: fsname为空
	createPplReq.Desc = "pipeline test"
	createPplReq.FsName = ""
	_, err = CreatePipeline(ctx, createPplReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create pipeline failed. fsname shall not be empty"), err)

	// create 成功
	createPplReq.FsName = MockFsName
	resp, err := CreatePipeline(ctx, createPplReq)
	assert.Nil(t, err)

	// test get success
	getPplResp, err := GetPipeline(ctx, resp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineVersions.PipelineVersionList), 1)
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].ID, "1")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].YamlPath, createPplReq.YamlPath)

	fmt.Printf("=========================\n=========================\n")
	b, _ := json.Marshal(getPplResp.Pipeline)
	fmt.Printf("%s\n", b)
	b, _ = json.Marshal(getPplResp.PipelineVersions.PipelineVersionList)
	fmt.Printf("%s\n", b)
	fmt.Printf("\n=========================\n=========================\n")

	// test create 失败，重复创建
	_, err = CreatePipeline(ctx, createPplReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("CreatePipeline failed: user[root] already has pipeline[distribute_wide_and_deep], cannot create again, use update instead!"), err)

	// 更改用户名后，创建成功
	// 而且yamlPath不传，使用默认的./run.yaml
	createPplReq.YamlPath = ""
	ctx = &logger.RequestContext{UserName: "another_user"}
	resp, err = CreatePipeline(ctx, createPplReq)
	assert.Nil(t, err)

	// test get success
	getPplResp, err = GetPipeline(ctx, resp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineVersions.PipelineVersionList), 1)
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].PipelineID, "ppl-000002")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].ID, "1")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].YamlPath, "./run.yaml")
}

// 测试更新pipeline
// todo: fs路径不存在 & 访问权限
func TestUpdatePipeline(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: "normalUser"}

	createPplReq := CreatePipelineRequest{
		FsName:   MockFsName,
		UserName: "",
		YamlPath: "../../../../example/wide_and_deep/run.yaml",
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

	patch1 := gomonkey.ApplyFunc(pkgPipeline.NewWorkflow, func(wfSource schema.WorkflowSource, runID string, params map[string]interface{}, extra map[string]string,
		callbacks pkgPipeline.WorkflowCallbacks) (*pkgPipeline.Workflow, error) {
		return &pkgPipeline.Workflow{}, nil
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})

	defer patch2.Reset()

	// test update 失败，pipeline没有创建，不能更新
	resp, err := UpdatePipeline(ctx, updatePplReq, pipelineID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("update pipeline[ppl-000001] failed. err:pipeline[ppl-000001] not exist"), err)

	// create 成功
	createPplResp, err := CreatePipeline(ctx, createPplReq)
	assert.Nil(t, err)

	// test get success
	getPplResp, err := GetPipeline(ctx, createPplResp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineVersions.PipelineVersionList), 1)
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].ID, "1")

	// update 失败: desc长度超过256
	updatePplReq.Desc = strings.Repeat("a", util.MaxDescLength+1)
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("desc too long, should be less than %d", util.MaxDescLength), err)

	// update 失败: fsname为空
	updatePplReq.Desc = "pipeline test"
	updatePplReq.FsName = ""
	_, err = UpdatePipeline(ctx, updatePplReq, pipelineID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("update pipeline failed. fsname shall not be empty"), err)

	// update 失败，yaml name 与 pipeline记录中的 name 不一样
	updatePplReq.FsName = MockFsName
	updatePplReq.YamlPath = "../../../../example/pipeline/base_pipeline/run.yaml"
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("update pipeline failed, pplname[base_pipeline] in yaml not the same as [distribute_wide_and_deep] of pipeline[ppl-000001]"), err)

	// update 成功
	updatePplReq.YamlPath = "../../../../example/wide_and_deep/run.yaml"
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID)
	assert.Nil(t, err)
	assert.Equal(t, createPplResp.PipelineID, resp.PipelineID)

	// 其他用户，update失败
	ctx = &logger.RequestContext{UserName: "anotherUser"}
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("update pipeline[ppl-000001] failed. Access denied for user[anotherUser]"), err)

	// root用户，update成功
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = UpdatePipeline(ctx, updatePplReq, pipelineID)
	assert.Nil(t, err)
	assert.Equal(t, createPplResp.PipelineID, resp.PipelineID)

	// root用户，test get success
	getPplResp, err = GetPipeline(ctx, createPplResp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineVersions.PipelineVersionList), 3)
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].ID, "1")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[1].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[1].ID, "2")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[2].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[2].ID, "3")

	// 普通用户，test get success
	ctx = &logger.RequestContext{UserName: "normalUser"}
	getPplResp, err = GetPipeline(ctx, createPplResp.PipelineID, "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, getPplResp.Pipeline.Name, "distribute_wide_and_deep")
	assert.Equal(t, len(getPplResp.PipelineVersions.PipelineVersionList), 3)
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[0].ID, "1")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[1].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[1].ID, "2")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[2].PipelineID, "ppl-000001")
	assert.Equal(t, getPplResp.PipelineVersions.PipelineVersionList[2].ID, "3")
}

// 测试list pipeline
// todo：测试marker不为空
func TestListPipeline(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	_, _, _, _ = insertPipeline(t, ctx.Logging())

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
	assert.NotEqual(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定userfilter
	resp, err = ListPipeline(ctx, "", 10, []string{"user1", "user2"}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定userfilter为root
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

	// nameFilter必须精确匹配，不支持模糊匹配
	resp, err = ListPipeline(ctx, "", 1, []string{}, []string{"ppl1"})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list，user非root时，不指定userFilter，只能返回自己有权限的pipeline
	ctx = &logger.RequestContext{UserName: "user1"}
	resp, err = ListPipeline(ctx, "", 10, []string{}, []string{})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(resp.PipelineList))
	assert.Equal(t, resp.PipelineList[0].ID, "ppl-000001")
	assert.Equal(t, resp.PipelineList[0].UserName, "user1")
	assert.Equal(t, resp.IsTruncated, false)
	assert.Equal(t, resp.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test list，user非root时，指定userfilter时会报错
	resp, err = ListPipeline(ctx, "", 10, []string{"root"}, []string{})
	assert.NotNil(t, err)
	assert.Equal(t, "only root user can set userFilter!", err.Error())
	println("")
	fmt.Printf("%s\n", b)
}

// todo：测试marker不为空
func TestGetPipeline(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := model.Pipeline{
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplVersion1 := model.PipelineVersion{
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	pplVersion2 := model.PipelineVersion{
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user1",
	}

	pplID1, pplVersionID1, err := storage.Pipeline.CreatePipeline(ctx.Logging(), &ppl1, &pplVersion1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplVersion1.Pk, int64(1))
	assert.Equal(t, pplVersionID1, pplVersion1.ID)
	assert.Equal(t, pplVersionID1, "1")
	assert.Equal(t, pplVersion1.PipelineID, ppl1.ID)

	pplID2, pplVersionID2, err := storage.Pipeline.UpdatePipeline(ctx.Logging(), &ppl1, &pplVersion2)
	assert.Nil(t, err)
	assert.Equal(t, pplID2, ppl1.ID)

	assert.Equal(t, pplVersion2.Pk, int64(2))
	assert.Equal(t, pplVersionID2, pplVersion2.ID)
	assert.Equal(t, pplVersionID2, "2")

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

	// test get pipeline 成功，root用户可以查看所有pipeline
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 2, len(resp.PipelineVersions.PipelineVersionList))
	assert.Equal(t, resp.PipelineVersions.PipelineVersionList[0].ID, "1")
	assert.Equal(t, resp.PipelineVersions.PipelineVersionList[1].ID, "2")
	assert.Equal(t, resp.PipelineVersions.IsTruncated, false)
	assert.Equal(t, resp.PipelineVersions.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test get pipeline, 指定maxkeys
	ctx = &logger.RequestContext{UserName: "user1"}
	resp, err = GetPipeline(ctx, "ppl-000001", "", 1, []string{})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 1, len(resp.PipelineVersions.PipelineVersionList))
	assert.Equal(t, resp.PipelineVersions.PipelineVersionList[0].ID, "1")
	assert.Equal(t, resp.PipelineVersions.IsTruncated, true)
	assert.NotEqual(t, resp.PipelineVersions.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test get pipeline, 指定fsfilter
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{"fsname"})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 1, len(resp.PipelineVersions.PipelineVersionList))
	assert.Equal(t, resp.PipelineVersions.PipelineVersionList[0].ID, "1")
	assert.Equal(t, resp.PipelineVersions.IsTruncated, false)
	assert.Equal(t, resp.PipelineVersions.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)

	// test get pipeline, 没有pipeline version匹配时，istruncated=false
	resp, err = GetPipeline(ctx, "ppl-000001", "", 10, []string{"notExistFsname"})
	assert.Nil(t, err)
	assert.Equal(t, resp.Pipeline.ID, "ppl-000001")
	assert.Equal(t, resp.Pipeline.Name, "ppl1")
	assert.Equal(t, 0, len(resp.PipelineVersions.PipelineVersionList))
	assert.Equal(t, resp.PipelineVersions.IsTruncated, false)
	assert.Equal(t, resp.PipelineVersions.NextMarker, "")
	b, _ = json.Marshal(resp)
	println("")
	fmt.Printf("%s\n", b)
}

func TestGetPipelineVersion(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := model.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplVersion1 := model.PipelineVersion{
		Pk:           1,
		PipelineID:   ppl1.ID,
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	pplVersion2 := model.PipelineVersion{
		Pk:           2,
		PipelineID:   ppl1.ID,
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user1",
	}

	pplID1, pplVersionID1, err := storage.Pipeline.CreatePipeline(ctx.Logging(), &ppl1, &pplVersion1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplVersion1.Pk, int64(1))
	assert.Equal(t, pplVersionID1, pplVersion1.ID)
	assert.Equal(t, pplVersionID1, "1")
	assert.Equal(t, pplVersion1.PipelineID, ppl1.ID)

	pplID2, pplVersionID2, err := storage.Pipeline.UpdatePipeline(ctx.Logging(), &ppl1, &pplVersion2)
	assert.Nil(t, err)
	assert.Equal(t, pplID2, ppl1.ID)

	assert.Equal(t, pplVersion2.Pk, int64(2))
	assert.Equal(t, pplVersionID2, pplVersion2.ID)
	assert.Equal(t, pplVersionID2, "2")

	// test get pipeline version 失败，pipeline id不存在
	resp, err := GetPipelineVersion(ctx, "wrongPplID", "1")
	assert.NotNil(t, err)
	assert.Equal(t, "get pipeline[wrongPplID] version[1] failed. err:pipeline[wrongPplID] not exist", err.Error())
	b, _ := json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline version 失败，用户没有权限
	ctx = &logger.RequestContext{UserName: "user2"}
	resp, err = GetPipelineVersion(ctx, "ppl-000001", "1")
	assert.NotNil(t, err)
	assert.Equal(t, "get pipeline[ppl-000001] version[1] failed. Access denied for user[user2]", err.Error())
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline version 失败, versionID 不存在
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = GetPipelineVersion(ctx, "ppl-000001", "3")
	assert.NotNil(t, err)
	assert.Equal(t, "get pipeline[ppl-000001] version[3] failed. err:pipeline[ppl-000001] version[3] not exist", err.Error())
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline version 成功
	resp, err = GetPipelineVersion(ctx, "ppl-000001", "1")
	assert.Nil(t, err)
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)

	// test get pipeline version 成功
	resp, err = GetPipelineVersion(ctx, "ppl-000001", "2")
	assert.Nil(t, err)
	b, _ = json.Marshal(resp)
	fmt.Printf("\n%s\n", b)
}

// todo: 测试有schedule在运行的场景（不能删除）
func TestDeletePipeline(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	// 创建pipeline前，test delete pipeline 失败，pipeline id不存在
	err := DeletePipeline(ctx, "wrongPplID")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[wrongPplID] failed. err:pipeline[wrongPplID] not exist", err.Error())

	ppl1 := model.Pipeline{
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplVersion1 := model.PipelineVersion{
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	ppl2 := model.Pipeline{
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "user2",
	}
	pplVersion2 := model.PipelineVersion{
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user2",
	}

	ppl3 := model.Pipeline{
		Name:     "ppl3",
		Desc:     "ppl3",
		UserName: "root",
	}
	pplVersion3 := model.PipelineVersion{
		FsID:         "root-fsname3",
		FsName:       "fsname3",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_3",
		UserName:     "root",
	}

	pplID1, pplVersionID1, err := storage.Pipeline.CreatePipeline(ctx.Logging(), &ppl1, &pplVersion1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplVersion1.Pk, int64(1))
	assert.Equal(t, pplVersionID1, pplVersion1.ID)
	assert.Equal(t, pplVersionID1, "1")
	assert.Equal(t, pplVersion1.PipelineID, ppl1.ID)

	pplID2, pplVersionID2, err := storage.Pipeline.CreatePipeline(ctx.Logging(), &ppl2, &pplVersion2)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.Pk, int64(2))
	assert.Equal(t, pplID2, ppl2.ID)
	assert.Equal(t, pplID2, "ppl-000002")

	assert.Equal(t, pplVersion2.Pk, int64(2))
	assert.Equal(t, pplVersionID2, pplVersion2.ID)
	assert.Equal(t, pplVersionID2, "1")
	assert.Equal(t, pplVersion2.PipelineID, ppl2.ID)

	pplID3, pplVersionID3, err := storage.Pipeline.CreatePipeline(ctx.Logging(), &ppl3, &pplVersion3)
	assert.Nil(t, err)
	assert.Equal(t, ppl3.Pk, int64(3))
	assert.Equal(t, pplID3, ppl3.ID)
	assert.Equal(t, pplID3, "ppl-000003")

	assert.Equal(t, pplVersion3.Pk, int64(3))
	assert.Equal(t, pplVersionID3, pplVersion3.ID)
	assert.Equal(t, pplVersionID3, "1")
	assert.Equal(t, pplVersion3.PipelineID, ppl3.ID)

	// test delete pipeline 失败，用户没有权限
	ctx = &logger.RequestContext{UserName: "user2"}
	err = DeletePipeline(ctx, "ppl-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] failed. Access denied for user[user2]", err.Error())

	// test delete pipeline 成功
	ctx = &logger.RequestContext{UserName: "user1"}
	err = DeletePipeline(ctx, "ppl-000001")
	assert.Nil(t, err)

	// 再次删除，pipeline不存在，删除失败
	err = DeletePipeline(ctx, "ppl-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] failed. err:pipeline[ppl-000001] not exist", err.Error())

	// test delete pipeline 成功，root用户能够删除自己创建的pipeline
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = DeletePipeline(ctx, "ppl-000002")
	assert.Nil(t, err)

	err = DeletePipeline(ctx, "ppl-000002")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000002] failed. err:pipeline[ppl-000002] not exist", err.Error())

	// test delete pipeline 成功，root用户也能删除别人创建的pipeline
	err = DeletePipeline(ctx, "ppl-000003")
	assert.Nil(t, err)

	err = DeletePipeline(ctx, "ppl-000003")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000003] failed. err:pipeline[ppl-000003] not exist", err.Error())
}

// todo: 测试有schedule在运行的场景（不能删除）
func TestDeletePipelineVersion(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	ppl1 := model.Pipeline{
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplVersion1 := model.PipelineVersion{
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	pplVersion2 := model.PipelineVersion{
		FsID:         "user1-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "user1",
	}

	pplVersion3 := model.PipelineVersion{
		FsID:         "user1-fsname3",
		FsName:       "fsname3",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_3",
		UserName:     "user1",
	}

	ppl2 := model.Pipeline{
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "root",
	}

	pplVersion4 := model.PipelineVersion{
		FsID:         "root-fsname4",
		FsName:       "fsname4",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_4",
		UserName:     "root",
	}

	pplVersion5 := model.PipelineVersion{
		FsID:         "root-fsname5",
		FsName:       "fsname5",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_5",
		UserName:     "root",
	}

	pplID1, pplVersionID1, err := storage.Pipeline.CreatePipeline(ctx.Logging(), &ppl1, &pplVersion1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplVersion1.Pk, int64(1))
	assert.Equal(t, pplVersionID1, pplVersion1.ID)
	assert.Equal(t, pplVersionID1, "1")
	assert.Equal(t, pplVersion1.PipelineID, ppl1.ID)

	pplID2, pplVersionID2, err := storage.Pipeline.UpdatePipeline(ctx.Logging(), &ppl1, &pplVersion2)
	assert.Nil(t, err)
	assert.Equal(t, pplID2, ppl1.ID)

	assert.Equal(t, pplVersion2.Pk, int64(2))
	assert.Equal(t, pplVersionID2, pplVersion2.ID)
	assert.Equal(t, pplVersionID2, "2")

	pplID3, pplVersionID3, err := storage.Pipeline.UpdatePipeline(ctx.Logging(), &ppl1, &pplVersion3)
	assert.Nil(t, err)
	assert.Equal(t, pplID3, ppl1.ID)

	assert.Equal(t, pplVersion3.Pk, int64(3))
	assert.Equal(t, pplVersionID3, pplVersion3.ID)
	assert.Equal(t, pplVersionID3, "3")

	pplID4, pplVersionID4, err := storage.Pipeline.CreatePipeline(ctx.Logging(), &ppl2, &pplVersion4)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.Pk, int64(2))
	assert.Equal(t, pplID4, ppl2.ID)
	assert.Equal(t, pplID4, "ppl-000002")

	assert.Equal(t, pplVersion4.Pk, int64(4))
	assert.Equal(t, pplVersionID4, pplVersion4.ID)
	assert.Equal(t, pplVersionID4, "1")
	assert.Equal(t, pplVersion4.PipelineID, ppl2.ID)

	pplID5, pplVersionID5, err := storage.Pipeline.UpdatePipeline(ctx.Logging(), &ppl2, &pplVersion5)
	assert.Nil(t, err)
	assert.Equal(t, pplID5, ppl2.ID)

	assert.Equal(t, pplVersion5.Pk, int64(5))
	assert.Equal(t, pplVersionID5, pplVersion5.ID)
	assert.Equal(t, pplVersionID5, "2")

	// test delete pipeline version 失败，pipeline记录不存在
	err = DeletePipelineVersion(ctx, "ppl-000003", "1")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000003] version[1] failed. err:pipeline[ppl-000003] not exist", err.Error())

	// test delete pipeline version 失败，用户没有权限
	ctx = &logger.RequestContext{UserName: "user2"}
	err = DeletePipelineVersion(ctx, "ppl-000001", "1")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] version[1] failed. Access denied for user[user2]", err.Error())

	// delete pipeline version 失败，pipeline version不存在，删除失败
	ctx = &logger.RequestContext{UserName: "user1"}
	err = DeletePipelineVersion(ctx, "ppl-000001", "4")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] version[4] failed. err:pipeline[ppl-000001] version[4] not exist", err.Error())

	// test delete pipeline version 成功
	err = DeletePipelineVersion(ctx, "ppl-000001", "1")
	assert.Nil(t, err)

	// 再次删除，pipeline不存在，删除失败
	err = DeletePipelineVersion(ctx, "ppl-000001", "1")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] version[1] failed. err:pipeline[ppl-000001] version[1] not exist", err.Error())

	// test delete pipeline 成功，root用户也能删除别人创建的pipeline
	err = DeletePipelineVersion(ctx, "ppl-000001", "2")
	assert.Nil(t, err)

	err = DeletePipelineVersion(ctx, "ppl-000001", "2")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000001] version[2] failed. err:pipeline[ppl-000001] version[2] not exist", err.Error())

	// test delete pipeline version 成功，root用户能够删除自己创建的pipeline
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = DeletePipelineVersion(ctx, "ppl-000002", "1")
	assert.Nil(t, err)

	err = DeletePipelineVersion(ctx, "ppl-000002", "1")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000002] version[1] failed. err:pipeline[ppl-000002] version[1] not exist", err.Error())

	// 当pipeline只有一个version时，不能删除version，只能删除pipeline
	err = DeletePipelineVersion(ctx, "ppl-000002", "2")
	assert.NotNil(t, err)
	assert.Equal(t, "delete pipeline[ppl-000002] version[2] failed. only one pipeline version left, pls delete pipeline instead", err.Error())

	// 删除version后重新更新pipeline，确保version_id是严格递增（包括被删除的部分）
	pplVersion6 := model.PipelineVersion{
		FsID:         "user1-fsname4",
		FsName:       "fsname4",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_4",
		UserName:     "user1",
	}

	pplID6, pplVersionID6, err := storage.Pipeline.UpdatePipeline(ctx.Logging(), &ppl1, &pplVersion6)
	assert.Nil(t, err)
	assert.Equal(t, pplID6, ppl1.ID)

	assert.Equal(t, pplVersion6.Pk, int64(6))
	assert.Equal(t, pplVersionID6, pplVersion6.ID)
	assert.Equal(t, pplVersionID6, "4")
}
