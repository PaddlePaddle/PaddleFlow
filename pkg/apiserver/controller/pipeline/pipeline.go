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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

type CreatePipelineRequest struct {
	FsName   string `json:"fsname"`
	YamlPath string `json:"yamlPath"` // optional, use "./run.yaml" if not specified
	UserName string `json:"username"` // optional, only for root user
	Name     string `json:"name"`     // optional
	Desc     string `json:"desc"`     // optional
}

type CreatePipelineResponse struct {
	PipelineID       string `json:"pipelineID"`
	PipelineDetailPk int64  `json:"pipelineDetailPk"`
	Name             string `json:"name"`
}

type UpdatePipelineRequest struct {
	PipelineID string `json:"pipelineID"`
	FsName     string `json:"fsname"`
	YamlPath   string `json:"yamlPath"` // optional, use "./run.yaml" if not specified
	UserName   string `json:"username"` // optional, only for root user
	Desc       string `json:"desc"`     // optional
}

type UpdatePipelineResponse struct {
	PipelineID       string `json:"pipelineID"`
	PipelineDetailPk int64  `json:"pipelineDetailPk"`
}

type ListPipelineResponse struct {
	common.MarkerInfo
	PipelineList []PipelineBrief `json:"pipelineList"`
}

type GetPipelineResponse struct {
	common.MarkerInfo
	Pipeline           PipelineBrief         `json:"pipeline"`
	PipelineDetailList []PipelineDetailBrief `json:"pplDetailList"`
}

type GetPipelineDetailResponse struct {
	common.MarkerInfo
	Pipeline       PipelineBrief       `json:"pipeline"`
	PipelineDetail PipelineDetailBrief `json:"pipelineDetail"`
}

type PipelineBrief struct {
	ID         string `json:"pipelineID"`
	Name       string `json:"name"`
	Desc       string `json:"desc"`
	UserName   string `json:"username"`
	CreateTime string `json:"createTime"`
	UpdateTime string `json:"updateTime"`
}

func (pb *PipelineBrief) updateFromPipelineModel(pipeline models.Pipeline) {
	pb.ID = pipeline.ID
	pb.Name = pipeline.Name
	pb.Desc = pipeline.Desc
	pb.UserName = pipeline.UserName
	pb.CreateTime = pipeline.CreatedAt.Format("2006-01-02 15:04:05")
	pb.UpdateTime = pipeline.UpdatedAt.Format("2006-01-02 15:04:05")
}

type PipelineDetailBrief struct {
	Pk           int64  `json:"pipelineDetailPk"`
	PipelineID   string `json:"pipelineID"`
	FsName       string `json:"fsname"`
	YamlPath     string `json:"yamlPath"`
	PipelineYaml string `json:"pipelineYaml"`
	PipelineMd5  string `json:"pipelineMd5"`
	UserName     string `json:"username"`
	CreateTime   string `json:"createTime"`
	UpdateTime   string `json:"updateTime"`
}

func (pdb *PipelineDetailBrief) updateFromPipelineModel(pipelineDetail models.PipelineDetail) {
	pdb.Pk = pipelineDetail.Pk
	pdb.PipelineID = pipelineDetail.PipelineID
	pdb.FsName = pipelineDetail.FsName
	pdb.YamlPath = pipelineDetail.YamlPath
	pdb.PipelineYaml = pipelineDetail.PipelineYaml
	pdb.UserName = pipelineDetail.UserName
	pdb.CreateTime = pipelineDetail.CreatedAt.Format("2006-01-02 15:04:05")
	pdb.UpdateTime = pipelineDetail.UpdatedAt.Format("2006-01-02 15:04:05")
}

// todo: 是否需要校验pipeline名称格式
func CreatePipeline(ctx *logger.RequestContext, request CreatePipelineRequest, fsID string) (CreatePipelineResponse, error) {
	// read run.yaml
	pipelineYaml, err := handler.ReadFileFromFs(fsID, request.YamlPath, ctx.Logging())
	if err != nil {
		ctx.ErrorCode = common.IOOperationFailure
		errMsg := fmt.Sprintf("readFileFromFs[%s] from fs[%s] failed. err:%v", request.YamlPath, fsID, err)
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// validate pipeline and get name of pipeline
	pplName, err := validateWorkflowForPipeline(string(pipelineYaml), request.Name)
	if err != nil {
		ctx.ErrorCode = common.MalformedYaml
		errMsg := fmt.Sprintf("validateWorkflowForPipeline failed. err:%v", err)
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// 校验pipeline是否存在，一个用户不能创建同名pipeline
	_, err = models.GetPipeline(pplName, ctx.UserName)
	if err == nil {
		ctx.ErrorCode = common.DuplicatedName
		errMsg := fmt.Sprintf("CreatePipeline failed: user[%s] already has pipeline[%s], cannot create again!", ctx.UserName, pplName)
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("CreatePipeline failed: %s", err)
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// create Pipeline in db
	ppl := models.Pipeline{
		ID:       "", // to be back-filled according to db pk
		Name:     pplName,
		Desc:     request.Desc,
		UserName: ctx.UserName,
	}

	yamlMd5 := common.GetMD5Hash(pipelineYaml)
	pplDetail := models.PipelineDetail{
		Pipeline:     ppl,
		DetailType:   pplcommon.PplDetailTypeNormal,
		FsID:         fsID,
		FsName:       request.FsName,
		YamlPath:     request.YamlPath,
		PipelineYaml: string(pipelineYaml),
		PipelineMd5:  yamlMd5,
		UserName:     ctx.UserName,
	}

	pplID, pplDetailPk, err := models.CreatePipeline(ctx.Logging(), &pplDetail)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("create pipeline run failed inserting db. error:%s", err.Error())
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	ctx.Logging().Debugf("create pipeline[%s] successful", pplID)
	response := CreatePipelineResponse{
		PipelineID:       pplID,
		PipelineDetailPk: pplDetailPk,
		Name:             pplName,
	}
	return response, nil
}

func UpdatePipeline(ctx *logger.RequestContext, request UpdatePipelineRequest, fsID string) (UpdatePipelineResponse, error) {
	// read run.yaml
	pipelineYaml, err := handler.ReadFileFromFs(fsID, request.YamlPath, ctx.Logging())
	if err != nil {
		ctx.ErrorCode = common.IOOperationFailure
		errMsg := fmt.Sprintf("readFileFromFs[%s] from fs[%s] failed. err:%v", request.YamlPath, fsID, err)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// validate pipeline and get name of pipeline
	pplName, err := validateWorkflowForPipeline(string(pipelineYaml), "")
	if err != nil {
		ctx.ErrorCode = common.MalformedYaml
		errMsg := fmt.Sprintf("validateWorkflowForPipeline failed. err:%v", err)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// 校验pipeline是否存在，不能更新不存在的pipeline
	ppl, err := models.GetPipelineByID(request.PipelineID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.DuplicatedName
			errMsg := fmt.Sprintf("UpdatePipeline failed: pipeline[%s] not created for user[%s], pls create first!", pplName, ctx.UserName)
			ctx.Logging().Errorf(errMsg)
			return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
		} else {
			ctx.ErrorCode = common.InternalError
			errMsg := fmt.Sprintf("UpdatePipeline failed for pipeline[%s], err: [%s]", pplName, err.Error())
			ctx.Logging().Errorf(errMsg)
			return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
		}
	}

	// 校验用户对pipeline记录的权限
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		ctx.ErrorCode = common.AccessDenied
		err := common.NoAccessError(ctx.UserName, common.ResourceTypePipeline, request.PipelineID)
		ctx.Logging().Errorln(err.Error())
		return UpdatePipelineResponse{}, err
	}

	// 校验待更新的pipeline name，和数据库中pipeline name一致
	if ppl.Name != pplName {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("update pipeline failed, pplname[%s] in yaml not the same as [%s] of pipeline[%s]", pplName, ppl.Name, request.PipelineID)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	ppl.Desc = request.Desc
	yamlMd5 := common.GetMD5Hash(pipelineYaml)
	pplDetail := models.PipelineDetail{
		Pipeline:     ppl,
		DetailType:   pplcommon.PplDetailTypeNormal,
		FsID:         fsID,
		FsName:       request.FsName,
		YamlPath:     request.YamlPath,
		PipelineYaml: string(pipelineYaml),
		PipelineMd5:  yamlMd5,
		UserName:     ctx.UserName,
	}

	pplID, pplDetailPk, err := models.UpdatePipeline(ctx.Logging(), &pplDetail)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("update pipeline failed inserting db. error:%s", err.Error())
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	ctx.Logging().Debugf("update pipeline[%s] successful, pplDetailPk[%d]", pplID, pplDetailPk)
	response := UpdatePipelineResponse{
		PipelineID:       pplID,
		PipelineDetailPk: pplDetailPk,
	}
	return response, nil
}

// todo: 为了校验pipeline，需要准备的内容太多，需要简化校验逻辑
func validateWorkflowForPipeline(pipelineYaml string, pplName string) (name string, err error) {
	// parse yaml -> WorkflowSource
	// TODO： validate pipeline containing multiple ws's
	wfs, err := schema.ParseWorkflowSource([]byte(pipelineYaml))
	if err != nil {
		logger.Logger().Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", pipelineYaml, err)
		return "", err
	}

	// request name > yaml name
	// todo: 是否需要校验pipeline中的name，和参数中传的name，保持一致
	if pplName != "" && pplName != wfs.Name {
		errMsg := fmt.Sprintf("pplName[%s] in request is not the same as name[%s] in pipeline yaml.", pplName, wfs.Name)
		logger.Logger().Errorf(errMsg)
		return "", fmt.Errorf(errMsg)
	}

	// fill extra info
	param := map[string]interface{}{}
	extra := map[string]string{
		pplcommon.WfExtraInfoKeyUserName: "",
		pplcommon.WfExtraInfoKeyFsName:   "",
		pplcommon.WfExtraInfoKeyFsID:     "",
	}

	// validate
	wfCbs := pipeline.WorkflowCallbacks{
		UpdateRunCb: func(string, interface{}) bool { return true },
		LogCacheCb:  LogCacheFunc,
		ListCacheCb: ListCacheFunc,
	}

	// todo：这里为了校验，还要传特殊的run name（validatePipeline），可以想办法简化校验逻辑
	wfPtr, err := pipeline.NewWorkflow(wfs, "validatePipeline", "", param, extra, wfCbs)
	if err != nil {
		logger.Logger().Errorf("NewWorkflow for pipeline[%s] failed. err:%v", pplName, err)
		return "", err
	}
	if wfPtr == nil {
		err := fmt.Errorf("NewWorkflow ptr for pipeline[%s] is nil", pplName)
		logger.Logger().Errorln(err.Error())
		return "", err
	}
	return wfs.Name, nil
}

func checkAndGetDetailType(detailType string) (string, error) {
	if detailType == "" {
		detailType = pplcommon.PplDetailTypeNormal
	}

	if detailType != pplcommon.PplDetailTypeNormal && detailType != pplcommon.PplDetailTypeSchedule {
		errMsg := fmt.Sprintf("pipeline detail type[%s] not correct", detailType)
		return "", fmt.Errorf(errMsg)
	} else {
		return detailType, nil
	}
}

func ListPipeline(ctx *logger.RequestContext, marker string, maxKeys int, userFilter, nameFilter []string) (ListPipelineResponse, error) {
	ctx.Logging().Debugf("begin list pipeline.")

	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.ErrorCode = common.InvalidMarker
			errMsg := fmt.Sprintf("DecryptPk marker[%s] failed. err:[%s]", marker, err.Error())
			ctx.Logging().Errorf(errMsg)
			return ListPipelineResponse{}, fmt.Errorf(errMsg)
		}
	}

	// 只有root用户才能设置userFilter，否则只能查询当前普通用户创建的pipeline列表
	if !common.IsRootUser(ctx.UserName) {
		userFilter = []string{ctx.UserName}
	}

	pipelineList, err := models.ListPipeline(pk, maxKeys, userFilter, nameFilter)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("ListPipeline[%d-%s-%s] failed. err: %v", maxKeys, userFilter, nameFilter, err)
		return ListPipelineResponse{}, err
	}

	listPipelineResponse := ListPipelineResponse{
		PipelineList: []PipelineBrief{},
	}

	// get next marker
	listPipelineResponse.IsTruncated = false
	if len(pipelineList) > 0 {
		ppl := pipelineList[len(pipelineList)-1]
		isLastPk, err := isLastPipelinePk(ctx, ppl.Pk)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			errMsg := fmt.Sprintf("get last pipeline Pk failed. err:[%s]", err.Error())
			ctx.Logging().Errorf(errMsg)
			return ListPipelineResponse{}, fmt.Errorf(errMsg)
		}

		if !isLastPk {
			nextMarker, err := common.EncryptPk(ppl.Pk)
			if err != nil {
				ctx.ErrorCode = common.InternalError
				errMsg := fmt.Sprintf("EncryptPk error. pk:[%d] error:[%s]", ppl.Pk, err.Error())
				ctx.Logging().Errorf(errMsg)
				return ListPipelineResponse{}, fmt.Errorf(errMsg)
			}
			listPipelineResponse.NextMarker = nextMarker
			listPipelineResponse.IsTruncated = true
		}
	}

	listPipelineResponse.MaxKeys = maxKeys
	for _, ppl := range pipelineList {
		pplBrief := PipelineBrief{}
		pplBrief.updateFromPipelineModel(ppl)
		listPipelineResponse.PipelineList = append(listPipelineResponse.PipelineList, pplBrief)
	}
	return listPipelineResponse, nil
}

func isLastPipelinePk(ctx *logger.RequestContext, pk int64) (bool, error) {
	lastPipeline, err := models.GetLastPipeline(ctx.Logging())
	if err != nil {
		errMsg := fmt.Sprintf("get last pipeline failed. error:[%s]", err.Error())
		ctx.Logging().Errorf(errMsg)
		return false, fmt.Errorf(errMsg)
	}
	if lastPipeline.Pk == pk {
		return true, nil
	}
	return false, nil
}

func GetPipeline(ctx *logger.RequestContext, pipelineID, marker string, maxKeys int, fsFilter []string) (GetPipelineResponse, error) {
	ctx.Logging().Debugf("begin get pipeline.")
	getPipelineResponse := GetPipelineResponse{}

	// query pipeline
	ppl, err := models.GetPipelineByID(pipelineID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("get pipeline[%s] failed, err: %v", pipelineID, err)
		ctx.Logging().Errorf(errMsg)
		return getPipelineResponse, fmt.Errorf(errMsg)
	}
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		ctx.ErrorCode = common.AccessDenied
		err := common.NoAccessError(ctx.UserName, common.ResourceTypePipeline, pipelineID)
		ctx.Logging().Errorln(err.Error())
		return getPipelineResponse, err
	}
	getPipelineResponse.Pipeline.updateFromPipelineModel(ppl)

	// query pipeline detail
	var pk int64
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.ErrorCode = common.InvalidMarker
			errMsg := fmt.Sprintf("DecryptPk marker[%s] failed. err:[%s]", marker, err.Error())
			ctx.Logging().Errorf(errMsg)
			return getPipelineResponse, fmt.Errorf(errMsg)
		}
	}

	detailTypeFilter := []string{pplcommon.PplDetailTypeNormal}
	pipelineDetailList, err := models.ListPipelineDetail(pipelineID, pk, maxKeys, fsFilter, detailTypeFilter)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("get Pipeline detail[%s-%d-%d-%s]. err: %v", pipelineID, pk, maxKeys, fsFilter, err)
		return GetPipelineResponse{}, err
	}

	// get next marker
	getPipelineResponse.IsTruncated = false
	if len(pipelineDetailList) > 0 {
		pplDetail := pipelineDetailList[len(pipelineDetailList)-1]
		isLastPPlDetailPk, err := isLastPipelineDetailPk(ctx, pipelineID, pplDetail.Pk)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			errMsg := fmt.Sprintf("get last ppldetail for ppl[%s] failed. err:[%s]", pipelineID, err.Error())
			ctx.Logging().Errorf(errMsg)
			return GetPipelineResponse{}, fmt.Errorf(errMsg)
		}

		if !isLastPPlDetailPk {
			nextMarker, err := common.EncryptPk(pplDetail.Pk)
			if err != nil {
				ctx.ErrorCode = common.InternalError
				errMsg := fmt.Sprintf("EncryptPk error. pk:[%d] error:[%s]", pplDetail.Pk, err.Error())
				ctx.Logging().Errorf(errMsg)
				return getPipelineResponse, fmt.Errorf(errMsg)
			}
			getPipelineResponse.NextMarker = nextMarker
			getPipelineResponse.IsTruncated = true
		}
	}
	getPipelineResponse.MaxKeys = maxKeys
	for _, pplDetail := range pipelineDetailList {
		pipelineDetailBrief := PipelineDetailBrief{}
		pipelineDetailBrief.updateFromPipelineModel(pplDetail)
		getPipelineResponse.PipelineDetailList = append(getPipelineResponse.PipelineDetailList, pipelineDetailBrief)
	}
	return getPipelineResponse, nil
}

func GetPipelineDetail(ctx *logger.RequestContext, pipelineID string, pipelineDetailPk int64) (GetPipelineDetailResponse, error) {
	ctx.Logging().Debugf("begin get pipeline detail.")

	// query pipeline
	ppl, err := models.GetPipelineByID(pipelineID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("get pipeline[%s] failed, err: %v", pipelineID, err)
		ctx.Logging().Errorf(errMsg)
		return GetPipelineDetailResponse{}, fmt.Errorf(errMsg)
	}
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		ctx.ErrorCode = common.AccessDenied
		err := common.NoAccessError(ctx.UserName, common.ResourceTypePipeline, pipelineID)
		ctx.Logging().Errorln(err.Error())
		return GetPipelineDetailResponse{}, err
	}

	// query pipeline detail
	pplDetail, err := models.GetPipelineDetailByID(pipelineDetailPk)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("get pipeline detail[%d] failed, err: %v", pipelineDetailPk, err)
		ctx.Logging().Errorf(errMsg)
		return GetPipelineDetailResponse{}, fmt.Errorf(errMsg)
	}

	getPipelineDetailResponse := GetPipelineDetailResponse{}
	getPipelineDetailResponse.Pipeline.updateFromPipelineModel(ppl)
	getPipelineDetailResponse.PipelineDetail.updateFromPipelineModel(pplDetail)
	return getPipelineDetailResponse, nil
}

func isLastPipelineDetailPk(ctx *logger.RequestContext, pplID string, pk int64) (bool, error) {
	lastPipelineDetail, err := models.GetLastPipelineDetail(ctx.Logging(), pplID)

	if err != nil {
		errMsg := fmt.Sprintf("get last pipeline detail failed. error:[%s]", err.Error())
		ctx.Logging().Errorf(errMsg)
		return false, fmt.Errorf(errMsg)
	}
	if lastPipelineDetail.Pk == pk {
		return true, nil
	}
	return false, nil
}

func DeletePipeline(ctx *logger.RequestContext, id string) error {
	ctx.Logging().Debugf("begin delete pipeline: %s", id)

	ppl, err := models.GetPipelineByID(id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.PipelineNotFound
			errMsg := fmt.Sprintf("delete pipeline[%s] failed. not exist", id)
			ctx.Logging().Errorf(errMsg)
			return fmt.Errorf(errMsg)
		} else {
			ctx.ErrorCode = common.InternalError
			errMsg := fmt.Sprintf("delete pipeline[%s] failed. err:%v", id, err)
			ctx.Logging().Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		ctx.ErrorCode = common.AccessDenied
		errMsg := fmt.Sprintf("delete pipeline[%s] failed. Access denied", id)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	// 需要判断是否有周期调度运行中（单次任务不影响，因为run会直接保存yaml）
	scheduleList, err := models.ListSchedule(ctx.Logging(), id, 0, 0, 0, []string{}, []string{}, []string{}, []string{}, models.ScheduleNotFinalStatusList)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("models list schedule failed. err:[%s]", err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if len(scheduleList) > 0 {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline[%s] failed, there are running schedules, pls stop first", id)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	if err := models.DeletePipeline(ctx.Logging(), id, false); err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("models delete pipeline[%s] failed. error:%s", id, err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func DeletePipelineDetail(ctx *logger.RequestContext, pipelineID string, pipelineDetailPk int64) error {
	ctx.Logging().Debugf("begin delete pipeline detail[%d], with pipelineID[%s]", pipelineDetailPk, pipelineID)

	ppl, err := models.GetPipelineByID(pipelineID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.PipelineNotFound
			errMsg := fmt.Sprintf("delete pipeline detail failed. pipeline[%s] not exist", pipelineID)
			ctx.Logging().Errorf(errMsg)
			return fmt.Errorf(errMsg)
		} else {
			ctx.ErrorCode = common.InternalError
			errMsg := fmt.Sprintf("delete pipeline detail[%d] failed. err:%v", pipelineDetailPk, err)
			ctx.Logging().Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		ctx.ErrorCode = common.AccessDenied
		errMsg := fmt.Sprintf("delete pipeline detail[%d] of pipeline[%s] failed. Access denied", pipelineDetailPk, pipelineID)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	_, err = models.GetPipelineDetailByID(pipelineDetailPk)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.PipelineNotFound
			errMsg := fmt.Sprintf("delete pipeline detail failed. pipeline detail[%d] not exist", pipelineDetailPk)
			ctx.Logging().Errorf(errMsg)
			return fmt.Errorf(errMsg)
		} else {
			ctx.ErrorCode = common.InternalError
			errMsg := fmt.Sprintf("delete pipeline detail[%d] failed. err:%v", pipelineDetailPk, err)
			ctx.Logging().Errorf(errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	// 如果只有一个pipeline detail的话，直接删除pipeline本身
	detailTypeFilter := []string{pplcommon.PplDetailTypeNormal}
	count, err := models.CountPipelineDetail(pipelineID, detailTypeFilter)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline detail[%d] failed. err:%v", pipelineDetailPk, err)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if count == 1 {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline detail[%d] failed. only one pipeline detail left, pls delete pipeline instead", pipelineDetailPk)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	// 需要判断是否有周期调度运行中（单次任务不影响，因为run会直接保存yaml）
	scheduleList, err := models.ListSchedule(ctx.Logging(), pipelineID, pipelineDetailPk, 0, 0, []string{}, []string{}, []string{}, []string{}, models.ScheduleNotFinalStatusList)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("models list schedule failed. err:[%s]", err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if len(scheduleList) > 0 {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete detail[%d] of pipeline[%s] failed, there are running schedules, pls stop first", pipelineDetailPk, pipelineID)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	if err := models.DeletePipelineDetail(ctx.Logging(), pipelineDetailPk, false); err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline detail[%d] failed. error:%s", pipelineDetailPk, err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

func CheckPipelineDetailPermission(userName string, pipelineID string, pipelineDetailPk int64) (bool, error) {
	pipelineDetailList, err := models.GetPipelineDetail(pipelineID, pipelineDetailPk, "")
	if err != nil {
		errMsg := fmt.Sprintf("get pipeline detail of piprlineID[%s], pplDetailPk[%d] failed, err:[%s]", pipelineID, pipelineDetailPk, err.Error())
		return false, fmt.Errorf(errMsg)
	} else if len(pipelineDetailList) != 1 {
		errMsg := fmt.Sprintf("get pipeline detail of piprlineID[%s], pplDetailPk[%d] failed, [%d]records found", pipelineID, pipelineDetailPk, len(pipelineDetailList))
		return false, fmt.Errorf(errMsg)
	}

	if !common.IsRootUser(userName) && userName != pipelineDetailList[0].Pipeline.UserName {
		return false, nil
	}

	return true, nil
}
