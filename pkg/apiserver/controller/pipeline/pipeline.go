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
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/run"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

type CreatePipelineRequest struct {
	FsName   string `json:"fsname"`
	YamlPath string `json:"yamlPath,omitempty"` // optional, use "./run.yaml" if not specified
	Name     string `json:"name,omitempty"`     // optional
	UserName string `json:"username,omitempty"` // optional, only for root user
}

type CreatePipelineResponse struct {
	ID   string `json:"pipelineID"`
	Name string `json:"name"`
}

type ListPipelineResponse struct {
	common.MarkerInfo
	PipelineList []models.Pipeline `json:"pipelineList"`
}

func CreatePipeline(ctx *logger.RequestContext, request CreatePipelineRequest) (CreatePipelineResponse, error) {
	// concatenate fsID
	var fsID string
	if common.IsRootUser(ctx.UserName) && request.UserName != "" {
		// root user can select fs under other users
		fsID = common.ID(request.UserName, request.FsName)
	} else {
		fsID = common.ID(ctx.UserName, request.FsName)
	}
	// read run.yaml
	pipelineYaml, err := handler.ReadFileFromFs(fsID, request.YamlPath, ctx.Logging())
	if err != nil {
		ctx.ErrorCode = common.IOOperationFailure
		ctx.Logging().Errorf("readFileFromFs[%s] from fs[%s] failed. err:%v", request.YamlPath, fsID, err)
		return CreatePipelineResponse{}, err
	}
	// parse yaml -> WorkflowSource
	// TODO validate pipeline containing multiple ws's
	wfs, err := schema.ParseWorkflowSource(pipelineYaml)
	if err != nil {
		ctx.ErrorCode = common.MalformedYaml
		ctx.Logging().Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", string(pipelineYaml), err)
		return CreatePipelineResponse{}, err
	}

	// request name > yaml name
	if request.Name != "" {
		wfs.Name = request.Name
	}
	// check duplicates in fs by md5
	yamlMd5 := common.GetMD5Hash(pipelineYaml)
	if err := validatePipeline(ctx, wfs.Name, yamlMd5, fsID); err != nil {
		ctx.Logging().Errorf("validate pipeline failed. err:%v", err)
		return CreatePipelineResponse{}, err
	}

	// create Pipeline in db after yaml validated
	ppl := models.Pipeline{
		ID:           "", // to be back-filled according to db pk
		Name:         wfs.Name,
		UserName:     ctx.UserName,
		FsName:       request.FsName,
		FsID:         fsID,
		PipelineYaml: string(pipelineYaml),
		PipelineMd5:  yamlMd5,
	}
	if err := ValidateWorkflowForPipeline(ppl); err != nil {
		ctx.ErrorCode = common.MalformedYaml
		ctx.Logging().Errorf("validateWorkflowForPipeline failed. err:%v", err)
		return CreatePipelineResponse{}, err
	}

	pipelineID, err := models.CreatePipeline(ctx.Logging(), &ppl)
	if err != nil {
		ctx.Logging().Errorf("create run failed inserting db. error:%s", err.Error())
		ctx.ErrorCode = common.InternalError
		return CreatePipelineResponse{}, err
	}
	ctx.Logging().Debugf("create pipeline[%s] successful", pipelineID)
	response := CreatePipelineResponse{
		ID:   pipelineID,
		Name: wfs.Name,
	}
	return response, nil
}

var ValidateWorkflowForPipeline = func(ppl models.Pipeline) error {
	// parse yaml -> WorkflowSource
	wfs, err := schema.ParseWorkflowSource([]byte(ppl.PipelineYaml))
	if err != nil {
		logger.Logger().Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", ppl.PipelineYaml, err)
		return err
	}

	// fill extra info
	param := map[string]interface{}{}
	extra := map[string]string{
		pplcommon.WfExtraInfoKeyUserName: ppl.UserName,
		pplcommon.WfExtraInfoKeyFsName:   ppl.FsName,
		pplcommon.WfExtraInfoKeyFsID:     ppl.FsID,
	}
	// validate
	wfCbs := pipeline.WorkflowCallbacks{
		UpdateRunCb: func(string, interface{}) bool { return true },
		LogCacheCb:  run.LogCacheFunc,
		ListCacheCb: run.ListCacheFunc,
	}
	wfPtr, err := pipeline.NewWorkflow(wfs, "validatePipeline", "", param, extra, wfCbs)
	if err != nil {
		logger.Logger().Errorf("NewWorkflow for pipeline[%s] failed. err:%v", ppl.Name, err)
		return err
	}
	if wfPtr == nil {
		err := fmt.Errorf("NewWorkflow ptr for pipeline[%s] is nil", ppl.Name)
		logger.Logger().Errorln(err.Error())
		return err
	}
	return nil
}

func validatePipeline(ctx *logger.RequestContext, name, md5, fsID string) error {
	// check name pattern
	if name != "" && !schema.CheckReg(name, common.RegPatternPipelineName) {
		ctx.ErrorCode = common.InvalidNamePattern
		ctx.Logging().Errorf("validate pipeline name[%s] pattern failed", name)
		return common.InvalidNamePatternError(name, common.ResourceTypePipeline, common.RegPatternPipelineName)
	}
	// check md5 duplicates in fs
	ppl, err := models.GetPipelineByMd5AndFs(md5, fsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		} else {
			ctx.ErrorCode = common.InternalError
			logger.Logger().Errorf("check duplicated pipeline md5[%s] in fs[%v] failed, error: %v",
				md5, fsID, err)
			return err
		}
	} else {
		ctx.ErrorCode = common.DuplicatedContent
		err := fmt.Errorf("please use existing pipeline[%s] with the same md5[%s] in fs[%s]. No need to re-create",
			ppl.ID, md5, fsID)
		logger.Logger().Errorln(err.Error())
		return err
	}
}

func GetPipelineByID(ctx *logger.RequestContext, pipelineID string) (models.Pipeline, error) {
	ppl, err := models.GetPipelineByID(pipelineID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("GetPipeline[%s]. err: %v", pipelineID, err)
		return models.Pipeline{}, err
	}
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		err := common.NoAccessError(ctx.UserName, common.ResourceTypePipeline, pipelineID)
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln(err.Error())
		return models.Pipeline{}, err
	}
	ppl.Decode()
	return ppl, nil
}

func ListPipeline(ctx *logger.RequestContext, marker string, maxKeys int, userFilter, fsFilter, nameFilter []string) (ListPipelineResponse, error) {
	ctx.Logging().Debugf("begin list pipeline.")
	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return ListPipelineResponse{}, err
		}
	}
	if !common.IsRootUser(ctx.UserName) {
		userFilter = []string{ctx.UserName}
	}
	pipelineList, err := models.ListPipeline(pk, maxKeys, userFilter, fsFilter, nameFilter)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("ListPipeline[%s-%s-%s]. err: %v", userFilter, fsFilter, nameFilter, err)
		return ListPipelineResponse{}, err
	}
	listPipelineResponse := ListPipelineResponse{
		PipelineList: []models.Pipeline{},
	}

	// get next marker
	listPipelineResponse.IsTruncated = false
	if len(pipelineList) > 0 {
		ppl := pipelineList[len(pipelineList)-1]
		if !isLastPipelinePk(ctx, ppl.Pk) {
			nextMarker, err := common.EncryptPk(ppl.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					ppl.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return ListPipelineResponse{}, err
			}
			listPipelineResponse.NextMarker = nextMarker
			listPipelineResponse.IsTruncated = true
		}
	}
	listPipelineResponse.MaxKeys = maxKeys
	for _, ppl := range pipelineList {
		if err := ppl.Decode(); err != nil {
			return ListPipelineResponse{}, err
		}
		listPipelineResponse.PipelineList = append(listPipelineResponse.PipelineList, ppl)
	}
	return listPipelineResponse, nil
}

func isLastPipelinePk(ctx *logger.RequestContext, pk int64) bool {
	lastPipeline, err := models.GetLastPipeline(ctx.Logging())
	if err != nil {
		ctx.Logging().Errorf("get last pipeline failed. error:[%s]", err.Error())
	}
	if lastPipeline.Pk == pk {
		return true
	}
	return false
}

func DeletePipeline(ctx *logger.RequestContext, id string) error {
	ctx.Logging().Debugf("begin delete pipeline: %s", id)

	ppl, err := models.GetPipelineByID(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.PipelineNotFound
			err := fmt.Errorf("delete pipeline[%s] failed. not exist", id)
			ctx.Logging().Errorf(err.Error())
			return err
		} else {
			ctx.ErrorCode = common.InternalError
			ctx.Logging().Errorf("delete pipeline[%s] failed. err:%v", id, err)
			return err
		}
	}

	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		ctx.ErrorCode = common.AccessDenied
		err := fmt.Errorf("delete pipeline[%s] failed. Access denied", id)
		ctx.Logging().Errorln(err.Error())
		return err
	}

	if err := models.HardDeletePipeline(ctx.Logging(), id); err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("models delete pipeline[%s] failed. error:%s", id, err.Error())
		return err
	}
	return nil
}
