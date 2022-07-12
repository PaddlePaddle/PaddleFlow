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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

type CreatePipelineRequest struct {
	FsName   string `json:"fsName"`
	YamlPath string `json:"yamlPath"` // optional, use "./run.yaml" if not specified
	UserName string `json:"username"` // optional, only for root user
	Desc     string `json:"desc"`     // optional
}

type CreatePipelineResponse struct {
	PipelineID       string `json:"pipelineID"`
	PipelineDetailID string `json:"pipelineDetailID"`
	Name             string `json:"name"`
}

type UpdatePipelineRequest struct {
	GlobalFsName string `json:"globalFsName"`
	YamlPath     string `json:"yamlPath"` // optional, use "./run.yaml" if not specified
	UserName     string `json:"username"` // optional, only for root user
	Desc         string `json:"desc"`     // optional
}

type UpdatePipelineResponse struct {
	PipelineID       string `json:"pipelineID"`
	PipelineDetailID string `json:"pipelineDetailID"`
}

type ListPipelineResponse struct {
	common.MarkerInfo
	PipelineList []PipelineBrief `json:"pipelineList"`
}

type GetPipelineResponse struct {
	Pipeline        PipelineBrief   `json:"pipeline"`
	PipelineDetails PipelineDetails `json:"pplDetails"`
}

type PipelineDetails struct {
	common.MarkerInfo
	PipelineDetailList []PipelineDetailBrief `json:"pplDetailList"`
}

type GetPipelineDetailResponse struct {
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
	ID           string `json:"pipelineDetailID"`
	PipelineID   string `json:"pipelineID"`
	GlobalFsName string `json:"globalFsName"`
	YamlPath     string `json:"yamlPath"`
	PipelineYaml string `json:"pipelineYaml"`
	UserName     string `json:"username"`
	CreateTime   string `json:"createTime"`
	UpdateTime   string `json:"updateTime"`
}

func (pdb *PipelineDetailBrief) updateFromPipelineDetailModel(pipelineDetail models.PipelineDetail) {
	pdb.ID = pipelineDetail.ID
	pdb.PipelineID = pipelineDetail.PipelineID
	pdb.GlobalFsName = pipelineDetail.FsName
	pdb.YamlPath = pipelineDetail.YamlPath
	pdb.PipelineYaml = pipelineDetail.PipelineYaml
	pdb.UserName = pipelineDetail.UserName
	pdb.CreateTime = pipelineDetail.CreatedAt.Format("2006-01-02 15:04:05")
	pdb.UpdateTime = pipelineDetail.UpdatedAt.Format("2006-01-02 15:04:05")
}

func CreatePipeline(ctx *logger.RequestContext, request CreatePipelineRequest) (CreatePipelineResponse, error) {
	// 校验desc长度
	if len(request.Desc) > util.MaxDescLength {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("desc too long, should be less than %d", util.MaxDescLength)
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// check user grant to fs
	if request.FsName == "" {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := "create pipeline failed. fsname shall not be empty"
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	fsID, err := CheckFsAndGetID(ctx.UserName, request.UserName, request.FsName)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorf(err.Error())
		return CreatePipelineResponse{}, err
	}

	if request.YamlPath == "" {
		request.YamlPath = "./run.yaml"
	}

	// read run.yaml
	pipelineYaml, err := handler.ReadFileFromFs(fsID, request.YamlPath, ctx.Logging())
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("readFileFromFs[%s] from fs[%s] failed. err:%v", request.YamlPath, fsID, err)
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// validate pipeline and get name of pipeline
	// 此处同样会校验pipeline name格式（正则表达式为：`^[A-Za-z_][A-Za-z0-9_]{1,49}$`）
	pplName, err := validateWorkflowForPipeline(string(pipelineYaml))
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
		errMsg := fmt.Sprintf("CreatePipeline failed: user[%s] already has pipeline[%s], cannot create again, use update instead!", ctx.UserName, pplName)
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
		FsID:         fsID,
		FsName:       request.FsName,
		YamlPath:     request.YamlPath,
		PipelineYaml: string(pipelineYaml),
		PipelineMd5:  yamlMd5,
		UserName:     ctx.UserName,
	}

	pplID, pplDetailID, err := models.CreatePipeline(ctx.Logging(), &ppl, &pplDetail)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("create pipeline run failed inserting db. error:%s", err.Error())
		ctx.Logging().Errorf(errMsg)
		return CreatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	ctx.Logging().Debugf("create pipeline[%s] successful", pplID)
	response := CreatePipelineResponse{
		PipelineID:       pplID,
		PipelineDetailID: pplDetailID,
		Name:             pplName,
	}
	return response, nil
}

func UpdatePipeline(ctx *logger.RequestContext, request UpdatePipelineRequest, pipelineID string) (UpdatePipelineResponse, error) {
	// 校验desc长度
	if len(request.Desc) > util.MaxDescLength {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("desc too long, should be less than %d", util.MaxDescLength)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// check user grant to fs
	if request.GlobalFsName == "" {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := "update pipeline failed. fsname shall not be empty"
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	fsID, err := CheckFsAndGetID(ctx.UserName, request.UserName, request.GlobalFsName)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorf(err.Error())
		return UpdatePipelineResponse{}, err
	}

	if request.YamlPath == "" {
		request.YamlPath = "./run.yaml"
	}

	// read run.yaml
	pipelineYaml, err := handler.ReadFileFromFs(fsID, request.YamlPath, ctx.Logging())
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("readFileFromFs[%s] from fs[%s] failed. err:%v", request.YamlPath, fsID, err)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// validate pipeline and get name of pipeline
	pplName, err := validateWorkflowForPipeline(string(pipelineYaml))
	if err != nil {
		ctx.ErrorCode = common.MalformedYaml
		errMsg := fmt.Sprintf("validateWorkflowForPipeline failed. err:%v", err)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	hasAuth, ppl, err := CheckPipelinePermission(ctx.UserName, pipelineID)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("update pipeline[%s] failed. err:%v", pipelineID, err)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	} else if !hasAuth {
		ctx.ErrorCode = common.AccessDenied
		errMsg := fmt.Sprintf("update pipeline[%s] failed. Access denied for user[%s]", pipelineID, ctx.UserName)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	// 校验待更新的pipeline name，和数据库中pipeline name一致
	if ppl.Name != pplName {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("update pipeline failed, pplname[%s] in yaml not the same as [%s] of pipeline[%s]", pplName, ppl.Name, pipelineID)
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	ppl.Desc = request.Desc
	yamlMd5 := common.GetMD5Hash(pipelineYaml)
	pplDetail := models.PipelineDetail{
		PipelineID:   pipelineID,
		FsID:         fsID,
		FsName:       request.GlobalFsName,
		YamlPath:     request.YamlPath,
		PipelineYaml: string(pipelineYaml),
		PipelineMd5:  yamlMd5,
		UserName:     ctx.UserName,
	}

	pplID, pplDetailID, err := models.UpdatePipeline(ctx.Logging(), &ppl, &pplDetail)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("update pipeline failed inserting db. error:%s", err.Error())
		ctx.Logging().Errorf(errMsg)
		return UpdatePipelineResponse{}, fmt.Errorf(errMsg)
	}

	ctx.Logging().Debugf("update pipeline[%s] successful, pplDetailID[%s]", pplID, pplDetailID)
	response := UpdatePipelineResponse{
		PipelineID:       pplID,
		PipelineDetailID: pplDetailID,
	}
	return response, nil
}

// todo: 为了校验pipeline，需要准备的内容太多，需要简化校验逻辑
func validateWorkflowForPipeline(pipelineYaml string) (name string, err error) {
	// parse yaml -> WorkflowSource
	wfs, err := schema.GetWorkflowSource([]byte(pipelineYaml))
	if err != nil {
		logger.Logger().Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", pipelineYaml, err)
		return "", err
	}

	// fill extra info
	param := map[string]interface{}{}
	extra := map[string]string{
		pplcommon.WfExtraInfoKeyUserName: "",
		pplcommon.WfExtraInfoKeyFsName:   "mock-fsname",
		pplcommon.WfExtraInfoKeyFsID:     "mock-fsid",
	}

	// validate
	wfCbs := pipeline.WorkflowCallbacks{
		UpdateRuntimeCb: func(string, interface{}) (int64, bool) { return 0, true },
		LogCacheCb:      LogCacheFunc,
		ListCacheCb:     ListCacheFunc,
	}

	// todo：这里为了校验，还要传特殊的run name（validatePipeline），可以想办法简化校验逻辑
	wfPtr, err := pipeline.NewWorkflow(wfs, "validatePipeline", param, extra, wfCbs)
	if err != nil {
		logger.Logger().Errorf("NewWorkflow for pipeline[%s] failed. err:%v", wfs.Name, err)
		return "", err
	}
	if wfPtr == nil {
		err := fmt.Errorf("NewWorkflow ptr for pipeline[%s] is nil", wfs.Name)
		logger.Logger().Errorln(err.Error())
		return "", err
	}
	return wfs.Name, nil
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
		if len(userFilter) != 0 {
			ctx.ErrorCode = common.InvalidArguments
			errMsg := fmt.Sprint("only root user can set userFilter!")
			ctx.Logging().Errorf(errMsg)
			return ListPipelineResponse{}, fmt.Errorf(errMsg)
		} else {
			userFilter = []string{ctx.UserName}
		}
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
		isLastPk, err := models.IsLastPipelinePk(ctx.Logging(), ppl.Pk, userFilter, nameFilter)
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

func GetPipeline(ctx *logger.RequestContext, pipelineID, marker string, maxKeys int, fsFilter []string) (GetPipelineResponse, error) {
	ctx.Logging().Debugf("begin get pipeline.")
	getPipelineResponse := GetPipelineResponse{}

	// query pipeline
	ppl, err := models.GetPipelineByID(pipelineID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.InvalidArguments
		} else {
			ctx.ErrorCode = common.InternalError
		}
		errMsg := fmt.Sprintf("get pipeline[%s] failed, err: %v", pipelineID, err)
		ctx.Logging().Errorf(errMsg)
		return GetPipelineResponse{}, fmt.Errorf(errMsg)
	}

	if !common.IsRootUser(ctx.UserName) && ctx.UserName != ppl.UserName {
		ctx.ErrorCode = common.AccessDenied
		err := common.NoAccessError(ctx.UserName, common.ResourceTypePipeline, pipelineID)
		ctx.Logging().Errorln(err.Error())
		return GetPipelineResponse{}, err
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
			return GetPipelineResponse{}, fmt.Errorf(errMsg)
		}
	}

	pipelineDetailList, err := models.ListPipelineDetail(pipelineID, pk, maxKeys, fsFilter)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("get Pipeline detail[%s-%d-%d-%s]. err: %v", pipelineID, pk, maxKeys, fsFilter, err)
		return GetPipelineResponse{}, err
	}

	// get next marker
	pipelineDetails := PipelineDetails{}
	pipelineDetails.IsTruncated = false
	if len(pipelineDetailList) > 0 {
		pplDetail := pipelineDetailList[len(pipelineDetailList)-1]
		isLastPPlDetailPk, err := models.IsLastPipelineDetailPk(ctx.Logging(), pipelineID, pplDetail.Pk, fsFilter)
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
				return GetPipelineResponse{}, fmt.Errorf(errMsg)
			}
			pipelineDetails.NextMarker = nextMarker
			pipelineDetails.IsTruncated = true
		}
	}
	pipelineDetails.MaxKeys = maxKeys
	pipelineDetails.PipelineDetailList = []PipelineDetailBrief{}
	for _, pplDetail := range pipelineDetailList {
		pipelineDetailBrief := PipelineDetailBrief{}
		pipelineDetailBrief.updateFromPipelineDetailModel(pplDetail)
		pipelineDetails.PipelineDetailList = append(pipelineDetails.PipelineDetailList, pipelineDetailBrief)
	}

	getPipelineResponse.PipelineDetails = pipelineDetails
	return getPipelineResponse, nil
}

func GetPipelineDetail(ctx *logger.RequestContext, pipelineID string, pipelineDetailID string) (GetPipelineDetailResponse, error) {
	ctx.Logging().Debugf("begin get pipeline detail.")

	// query pipeline
	hasAuth, ppl, pplDetail, err := CheckPipelineDetailPermission(ctx.UserName, pipelineID, pipelineDetailID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("get pipeline[%s] detail[%s] failed. err:%v", pipelineID, pipelineDetailID, err)
		ctx.Logging().Errorf(errMsg)
		return GetPipelineDetailResponse{}, fmt.Errorf(errMsg)
	} else if !hasAuth {
		ctx.ErrorCode = common.AccessDenied
		errMsg := fmt.Sprintf("get pipeline[%s] detail[%s] failed. Access denied for user[%s]", pipelineID, pipelineDetailID, ctx.UserName)
		ctx.Logging().Errorf(errMsg)
		return GetPipelineDetailResponse{}, fmt.Errorf(errMsg)
	}

	getPipelineDetailResponse := GetPipelineDetailResponse{}
	getPipelineDetailResponse.Pipeline.updateFromPipelineModel(ppl)
	getPipelineDetailResponse.PipelineDetail.updateFromPipelineDetailModel(pplDetail)
	return getPipelineDetailResponse, nil
}

func DeletePipeline(ctx *logger.RequestContext, pipelineID string) error {
	ctx.Logging().Debugf("begin delete pipeline: %s", pipelineID)

	hasAuth, _, err := CheckPipelinePermission(ctx.UserName, pipelineID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline[%s] failed. err:%v", pipelineID, err)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if !hasAuth {
		ctx.ErrorCode = common.AccessDenied
		errMsg := fmt.Sprintf("delete pipeline[%s] failed. Access denied for user[%s]", pipelineID, ctx.UserName)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	// 需要判断是否有周期调度运行中（单次任务不影响，因为run会直接保存yaml）
	scheduleList, err := models.ListSchedule(ctx.Logging(), 0, 0, []string{pipelineID}, []string{}, []string{}, []string{}, []string{}, models.ScheduleNotFinalStatusList)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("models list schedule failed. err:[%s]", err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if len(scheduleList) > 0 {
		ctx.ErrorCode = common.ActionNotAllowed
		errMsg := fmt.Sprintf("delete pipeline[%s] failed, there are running schedules, pls stop first", pipelineID)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	if err := models.DeletePipeline(ctx.Logging(), pipelineID); err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("models delete pipeline[%s] failed. error:%s", pipelineID, err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func DeletePipelineDetail(ctx *logger.RequestContext, pipelineID string, pipelineDetailID string) error {
	ctx.Logging().Debugf("begin delete pipeline detail[%s], with pipelineID[%s]", pipelineDetailID, pipelineID)
	hasAuth, _, _, err := CheckPipelineDetailPermission(ctx.UserName, pipelineID, pipelineDetailID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline[%s] detail[%s] failed. err:%v", pipelineID, pipelineDetailID, err)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if !hasAuth {
		ctx.ErrorCode = common.AccessDenied
		errMsg := fmt.Sprintf("delete pipeline[%s] detail[%s] failed. Access denied for user[%s]", pipelineID, pipelineDetailID, ctx.UserName)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	// 如果只有一个pipeline detail的话，直接删除pipeline本身
	count, err := models.CountPipelineDetail(pipelineID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline[%s] detail[%s] failed. err:%v", pipelineID, pipelineDetailID, err)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if count == 1 {
		ctx.ErrorCode = common.ActionNotAllowed
		errMsg := fmt.Sprintf("delete pipeline[%s] detail[%s] failed. only one pipeline detail left, pls delete pipeline instead", pipelineID, pipelineDetailID)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	// 需要判断是否有周期调度运行中（单次任务不影响，因为run会直接保存yaml）
	scheduleList, err := models.ListSchedule(ctx.Logging(), 0, 0, []string{pipelineID}, []string{pipelineDetailID}, []string{}, []string{}, []string{}, models.ScheduleNotFinalStatusList)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("models list schedule for pipeline[%s] detail[%s] failed. err:[%s]", pipelineID, pipelineDetailID, err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	} else if len(scheduleList) > 0 {
		ctx.ErrorCode = common.ActionNotAllowed
		errMsg := fmt.Sprintf("delete pipeline[%s] detail[%s] failed, there are running schedules, pls stop first", pipelineDetailID, pipelineID)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	if err := models.DeletePipelineDetail(ctx.Logging(), pipelineID, pipelineDetailID); err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("delete pipeline[%s] detail[%s] failed. error:%s", pipelineID, pipelineDetailID, err.Error())
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

func CheckPipelinePermission(userName string, pipelineID string) (bool, models.Pipeline, error) {
	ppl, err := models.GetPipelineByID(pipelineID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			errMsg := fmt.Sprintf("pipeline[%s] not exist", pipelineID)
			return false, models.Pipeline{}, fmt.Errorf(errMsg)
		} else {
			errMsg := fmt.Sprintf("get pipeline[%s] failed, err:[%s]", pipelineID, err.Error())
			return false, models.Pipeline{}, fmt.Errorf(errMsg)
		}
	}

	if !common.IsRootUser(userName) && userName != ppl.UserName {
		return false, models.Pipeline{}, nil
	}

	return true, ppl, nil
}

func CheckPipelineDetailPermission(userName string, pipelineID string, pipelineDetailID string) (bool, models.Pipeline, models.PipelineDetail, error) {
	hasAuth, ppl, err := CheckPipelinePermission(userName, pipelineID)
	if err != nil {
		return false, models.Pipeline{}, models.PipelineDetail{}, err
	} else if !hasAuth {
		return false, models.Pipeline{}, models.PipelineDetail{}, nil
	}

	pipelineDetail, err := models.GetPipelineDetail(pipelineID, pipelineDetailID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			errMsg := fmt.Sprintf("pipeline[%s] detail[%s] not exist", pipelineID, pipelineDetailID)
			return false, models.Pipeline{}, models.PipelineDetail{}, fmt.Errorf(errMsg)
		} else {
			errMsg := fmt.Sprintf("get pipeline[%s] detail[%s] failed, err:[%s]", pipelineID, pipelineDetailID, err.Error())
			return false, models.Pipeline{}, models.PipelineDetail{}, fmt.Errorf(errMsg)
		}
	}

	return true, ppl, pipelineDetail, nil
}
