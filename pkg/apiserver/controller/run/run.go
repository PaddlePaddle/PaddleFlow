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

package run

import (
	"encoding/base64"
	"errors"
	"fmt"

	"gopkg.in/yaml.v2"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/handler"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/pipeline"
	pplcommon "paddleflow/pkg/pipeline/common"
)

var wfMap = make(map[string]*pipeline.Workflow, 0)

type CreateRunRequest struct {
	FsName      string                 `json:"fsname"`
	UserName    string                 `json:"username,omitempty"`   // optional, only for root user
	Name        string                 `json:"name,omitempty"`       // optional
	Description string                 `json:"desc,omitempty"`       // optional
	Entry       string                 `json:"entry,omitempty"`      // optional
	Parameters  map[string]interface{} `json:"parameters,omitempty"` // optional
	DockerEnv   string                 `json:"dockerEnv,omitempty"`  // optional
	// run workflow source. priority: RunYamlRaw > PipelineID > RunYamlPath
	// 为了防止字符串或者不同的http客户端对run.yaml
	// 格式中的特殊字符串做特殊过滤处理导致yaml文件不正确，因此采用runYamlRaw采用base64编码传输
	Disabled    string `json:"disabled,omitempty"`    // optional
	RunYamlRaw  string `json:"runYamlRaw,omitempty"`  // optional. one of 3 sources of run. high priority
	PipelineID  string `json:"pipelineID,omitempty"`  // optional. one of 3 sources of run. medium priority
	RunYamlPath string `json:"runYamlPath,omitempty"` // optional. one of 3 sources of run. low priority
}

type CreateRunByJsonRequest struct {
	FsName         string                `json:"fsName"`
	UserName       string                `json:"userName,omitempty"` // optional, only for root user
	Description    string                `json:"desc,omitempty"`     // optional
	Disabled       string                `json:"disabled,omitempty"` // optional
	Name           string                `json:"name"`
	DockerEnv      string                `json:"dockerEnv,omitempty"`   // optional
	Parallelism    int                   `json:"parallelism,omitempty"` // optional
	EntryPoints    map[string]*RunStep   `json:"entryPoints"`
	PostProcess    map[string]*RunStep   `json:"postProcess,omitempty"`    // optional
	Cache          schema.Cache          `json:"cache,omitempty"`          // optional
	Queue          string                `json:"queue,omitempty"`          // optional
	Flavour        string                `json:"flavour,omitempty"`        // optional
	JobType        string                `json:"jobType,omitempty"`        // optional
	FailureOptions schema.FailureOptions `json:"failureOptions,omitempty"` // optional
	Env            map[string]string     `json:"env,omitempty"`            // optional
}

// used for API CreateRunJson to unmarshal steps in entryPoints and postProcess
type RunStep struct {
	Parameters map[string]interface{} `json:"parameters"`
	Command    string                 `json:"command"`
	Deps       string                 `json:"deps"`
	Artifacts  ArtifactsJson          `json:"artifacts"`
	Env        map[string]string      `json:"env"`
	Queue      string                 `json:"queue"`
	Flavour    string                 `json:"flavour"`
	JobType    string                 `json:"jobType"`
	Cache      schema.Cache           `json:"cache"`
	DockerEnv  string                 `json:"dockerEnv"`
}

// used for API CreateRunJson to unmarshal artifacts
type ArtifactsJson struct {
	Input  map[string]string `json:"input"`
	Output []string          `json:"output"`
}

type UpdateRunRequest struct {
	StopForce bool `json:"stopForce"`
}

type DeleteRunRequest struct {
	CheckCache bool `json:"checkCache"`
}

type CreateRunResponse struct {
	RunID string `json:"runID"`
}

type RunBrief struct {
	ID           string `json:"runID"`
	Name         string `json:"name"`
	Source       string `json:"source"` // pipelineID or yamlPath
	UserName     string `json:"username"`
	FsName       string `json:"fsname"`
	Message      string `json:"runMsg"`
	Status       string `json:"status"`
	CreateTime   string `json:"createTime"`
	ActivateTime string `json:"activateTime"`
}

type ListRunResponse struct {
	common.MarkerInfo
	RunList []RunBrief `json:"runList"`
}

func (b *RunBrief) modelToListResp(run models.Run) {
	b.ID = run.ID
	b.Name = run.Name
	b.Source = run.Source
	b.UserName = run.UserName
	b.FsName = run.FsName
	b.Message = run.Message
	b.Status = run.Status
	b.CreateTime = run.CreateTime
	b.ActivateTime = run.ActivateTime
}

func buildWorkflowSource(userName string, req CreateRunRequest, fsID string) (schema.WorkflowSource, string, string, error) {
	var source, runYaml string
	// retrieve source and runYaml
	if req.RunYamlRaw != "" { // high priority: wfs delivered by request
		// base64 decode
		// todo://后续将实际运行的run.yaml放入文件中
		sDec, err := base64.StdEncoding.DecodeString(req.RunYamlRaw)
		if err != nil {
			logger.Logger().Errorf("Decode raw runyaml is [%s] failed. err:%v", req.RunYamlRaw, err)
			return schema.WorkflowSource{}, "", "", err
		}
		runYaml = string(sDec)
		wfs := schema.WorkflowSource{}
		if err := yaml.Unmarshal([]byte(runYaml), &wfs); err != nil {
			logger.Logger().Errorf("Unmarshal runYaml to get source failed. yaml: %s \n, err:%v", runYaml, err)
			return schema.WorkflowSource{}, "", "", err
		}
		// 目前只保存用户提交的yaml，因此这里获得的yaml直接舍去
		source, _, err = getSourceAndYaml(wfs)
		if err != nil {
			logger.Logger().Errorf("get source and yaml by wrokFlowSource faild. err: %v", err)
			return schema.WorkflowSource{}, "", "", err
		}
	} else if req.PipelineID != "" { // medium priority: wfs in pipeline
		ppl, err := models.GetPipelineByID(req.PipelineID)
		if err != nil {
			logger.Logger().Errorf("GetPipelineByID[%s] failed. err:%v", req.PipelineID, err)
			return schema.WorkflowSource{}, "", "", err
		}
		if !common.IsRootUser(userName) && ppl.UserName != userName {
			err := common.NoAccessError(userName, common.ResourceTypePipeline, ppl.ID)
			logger.Logger().Errorf("buildWorkflowSource[%s] failed. err:%v", req.PipelineID, err)
			return schema.WorkflowSource{}, "", "", err
		}
		runYaml = ppl.PipelineYaml
		source = ppl.ID
	} else { // low priority: wfs in fs, read from runYamlPath
		if fsID == "" {
			err := fmt.Errorf("can not get runYaml without fs")
			logger.Logger().Errorf(err.Error())
			return schema.WorkflowSource{}, "", "", err
		}
		runYamlPath := req.RunYamlPath
		if runYamlPath == "" {
			runYamlPath = config.DefaultRunYamlPath
		}
		runYamlByte, err := handler.ReadFileFromFs(fsID, runYamlPath, logger.Logger())
		if err != nil {
			logger.Logger().Errorf("readFileFromFs from[%s] failed. err:%v", fsID, err)
			return schema.WorkflowSource{}, "", "", err
		}
		source = runYamlPath
		runYaml = string(runYamlByte)
	}
	// to wfs
	wfs, err := runYamlAndReqToWfs(runYaml, req)
	if err != nil {
		logger.Logger().Errorf("runYamlAndReqToWfs failed. err:%v", err)
		return schema.WorkflowSource{}, "", "", err
	}
	return wfs, source, runYaml, nil
}

// Used for API CreateRunJson, get wfs by json request.
func getWorkFlowSourceByReq(request *CreateRunByJsonRequest, bodyMap map[string]interface{}) (schema.WorkflowSource, error) {
	if len(request.EntryPoints) == 0 {
		err := fmt.Errorf("missing entryPoints")
		logger.Logger().Errorf(err.Error())
		return schema.WorkflowSource{}, err
	}
	if request.Env == nil {
		request.Env = map[string]string{}
	}

	request.Env[schema.EnvJobType] = request.JobType
	request.Env[schema.EnvJobQueueName] = request.Queue
	request.Env[schema.EnvJobFlavour] = request.Flavour

	entryPoints := parseRunSteps(request.EntryPoints, request)
	postProcess := parseRunSteps(request.PostProcess, request)
	failureOptions := schema.FailureOptions{Strategy: schema.FailureStrategyFailFast}
	if request.FailureOptions.Strategy != "" {
		failureOptions.Strategy = request.FailureOptions.Strategy
	}
	wfs := schema.WorkflowSource{
		Name:           request.Name,
		DockerEnv:      request.DockerEnv,
		Cache:          request.Cache,
		Parallelism:    request.Parallelism,
		EntryPoints:    entryPoints,
		PostProcess:    postProcess,
		Disabled:       request.Disabled,
		FailureOptions: failureOptions,
	}
	if err := transCacheJson2Yaml(bodyMap); err != nil {
		return schema.WorkflowSource{}, err
	}
	wfs.ValidateStepCacheByMap(bodyMap)
	return wfs, nil
}

func transCacheJson2Yaml(bodyMap map[string]interface{}) error {
	entryPoints, ok, err := unstructured.NestedFieldCopy(bodyMap, "entryPoints")
	if err != nil {
		return err
	}
	if !ok {
		err := fmt.Errorf("no entryPoints in body of request")
		return err
	}
	entryPointsMap := entryPoints.(map[string]interface{})
	for name, point := range entryPointsMap {
		pointMap := point.(map[string]interface{})
		// 检查用户是否有设置节点级别的Cache
		cache, ok, err := unstructured.NestedFieldCopy(pointMap, "cache")
		if err != nil {
			return err
		}
		if ok {
			cacheMap := cache.(map[string]interface{})
			// Enable字段的Json和Yaml形式一样，无需赋值
			// MaxExpiredTime赋值
			if value, ok := cacheMap["maxExpiredTime"]; ok {
				if err := unstructured.SetNestedField(cacheMap, value, schema.CacheAttributeMaxExpiredTime); err != nil {
					return err
				}
			}
			// FsScope赋值
			if value, ok := cacheMap["fsScope"]; ok {
				if err := unstructured.SetNestedField(cacheMap, value, schema.CacheAttributeFsScope); err != nil {
					return err
				}
			}
			if err := unstructured.SetNestedField(pointMap, cacheMap, "cache"); err != nil {
				return err
			}
			if err := unstructured.SetNestedField(entryPointsMap, pointMap, name); err != nil {
				return err
			}
		}
	}
	if err := unstructured.SetNestedField(bodyMap, entryPointsMap, schema.EntryPointsStr); err != nil {
		return err
	}
	return nil
}

// Used for API CreateRunJson, validates step by request and run params like cache, dockerEnv, env, and returns steps.
func parseRunSteps(steps map[string]*RunStep, request *CreateRunByJsonRequest) map[string]*schema.WorkflowSourceStep {
	workFlowSourceSteps := make(map[string]*schema.WorkflowSourceStep)
	for pointName, step := range steps {
		if step.Env == nil {
			step.Env = make(map[string]string)
		}
		step.Env[schema.EnvJobType] = step.JobType
		step.Env[schema.EnvJobQueueName] = step.Queue
		step.Env[schema.EnvJobFlavour] = step.Flavour

		// 对于每一个全局环境变量，检查节点是否有设置对应环境变量，如果没有则使用全局的
		for globalKey, globalValue := range request.Env {
			value, ok := step.Env[globalKey]
			if !ok || value == "" {
				step.Env[globalKey] = globalValue
			}
		}
		// DockerEnv字段替换检查
		if step.DockerEnv == "" {
			step.DockerEnv = request.DockerEnv
		}

		artifacts := parseArtifacts(step.Artifacts)

		workFlowSourceStep := schema.WorkflowSourceStep{
			Command:    step.Command,
			Deps:       step.Deps,
			Env:        step.Env,
			Artifacts:  artifacts,
			Cache:      step.Cache,
			DockerEnv:  step.DockerEnv,
			Parameters: step.Parameters,
		}
		workFlowSourceSteps[pointName] = &workFlowSourceStep
	}
	return workFlowSourceSteps
}

// transform artifacts in Json to common artifacts used in wfs
func parseArtifacts(atf ArtifactsJson) schema.Artifacts {
	outputAritfacts := map[string]string{}
	outputList := []string{}
	for _, outputName := range atf.Output {
		outputAritfacts[outputName] = ""
		outputList = append(outputList, outputName)
	}
	res := schema.Artifacts{
		Input:      atf.Input,
		Output:     outputAritfacts,
		OutputList: outputList,
	}
	return res
}

func getSourceAndYaml(wfs schema.WorkflowSource) (string, string, error) {
	yamlByte, err := yaml.Marshal(wfs)
	if err != nil {
		logger.Logger().Errorf("marshal workFlowSource to yaml faild. err: %v", err)
		return "", "", err
	}
	runYaml := string(yamlByte)
	source := common.GetMD5Hash(yamlByte)
	return source, runYaml, nil
}

func runYamlAndReqToWfs(runYaml string, req interface{}) (schema.WorkflowSource, error) {
	// parse yaml -> WorkflowSource
	wfs, err := schema.ParseWorkflowSource([]byte(runYaml))
	if err != nil {
		logger.Logger().Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", runYaml, err)
		return schema.WorkflowSource{}, err
	}

	// replace name & dockerEnv & disabled by request
	switch req := req.(type) {
	case CreateRunRequest:
		if req.Name != "" {
			wfs.Name = req.Name
		}
		if req.DockerEnv != "" {
			wfs.DockerEnv = req.DockerEnv
		}
		if req.Disabled != "" {
			wfs.Disabled = req.Disabled
		}
		return wfs, nil
	case CreateRunByJsonRequest:
		if req.Name != "" {
			wfs.Name = req.Name
		}
		if req.DockerEnv != "" {
			wfs.DockerEnv = req.DockerEnv
		}
		if req.Disabled != "" {
			wfs.Disabled = req.Disabled
		}
		return wfs, nil
	default:
		err := fmt.Errorf("can't handle request type [%v]", req)
		return schema.WorkflowSource{}, err
	}

}

func CreateRun(userName string, request *CreateRunRequest) (CreateRunResponse, error) {
	// concatenate fsID
	var fsID string
	if request.FsName != "" {
		if common.IsRootUser(userName) && request.UserName != "" {
			// root user can select fs under other users
			fsID = common.ID(request.UserName, request.FsName)
		} else {
			fsID = common.ID(userName, request.FsName)
		}
	}
	// todo://增加root用户判断fs是否存在
	// TODO:// validate flavour
	// TODO:// validate queue

	wfs, source, runYaml, err := buildWorkflowSource(userName, *request, fsID)
	if err != nil {
		logger.Logger().Errorf("buildWorkflowSource failed. error:%v", err)
		return CreateRunResponse{}, err
	}

	// check name pattern
	if wfs.Name != "" && !schema.CheckReg(wfs.Name, common.RegPatternRunName) {
		err := common.InvalidNamePatternError(wfs.Name, common.ResourceTypeRun, common.RegPatternRunName)
		logger.Logger().Errorf("create run failed as run name illegal. error:%v", err)
		return CreateRunResponse{}, err
	}
	// create run in db after run.yaml validated
	run := models.Run{
		ID:             "", // to be back filled according to db pk
		Name:           wfs.Name,
		Source:         source,
		UserName:       userName,
		FsName:         request.FsName,
		FsID:           fsID,
		Description:    request.Description,
		Parameters:     request.Parameters,
		RunYaml:        runYaml,
		WorkflowSource: wfs, // DockerEnv has not been replaced. done in func handleImageAndStartWf
		Entry:          request.Entry,
		Disabled:       request.Disabled,
		Status:         common.StatusRunInitiating,
	}
	response, err := ValidateAndStartRun(run, *request)
	return response, err
}

func CreateRunByJson(userName string, request *CreateRunByJsonRequest, bodyMap map[string]interface{}) (CreateRunResponse, error) {
	var fsID string
	if request.FsName != "" {
		if common.IsRootUser(userName) && request.UserName != "" {
			// root user can select fs under other users
			fsID = common.ID(request.UserName, request.FsName)
		} else {
			fsID = common.ID(userName, request.FsName)
		}
	}

	wfs, err := getWorkFlowSourceByReq(request, bodyMap)
	if err != nil {
		logger.Logger().Errorf("get WorkFlowSource by request failed. error:%v", err)
		return CreateRunResponse{}, err
	}

	source, runYaml, err := getSourceAndYaml(wfs)
	if err != nil {
		logger.Logger().Errorf("get source and yaml by workflowsource failed. error:%v", err)
		return CreateRunResponse{}, err
	}

	// check name pattern
	if wfs.Name != "" && !schema.CheckReg(wfs.Name, common.RegPatternRunName) {
		err := common.InvalidNamePatternError(wfs.Name, common.ResourceTypeRun, common.RegPatternRunName)
		logger.Logger().Errorf("create run failed as run name illegal. error:%v", err)
		return CreateRunResponse{}, err
	}
	// create run in db after run.yaml validated
	run := models.Run{
		ID:             "", // to be back filled according to db pk
		Name:           wfs.Name,
		Source:         source,
		UserName:       userName,
		FsName:         request.FsName,
		FsID:           fsID,
		Description:    request.Description,
		RunYaml:        runYaml,
		WorkflowSource: wfs, // DockerEnv has not been replaced. done in func handleImageAndStartWf
		Disabled:       request.Disabled,
		Status:         common.StatusRunInitiating,
	}
	response, err := ValidateAndStartRun(run, *request)
	return response, err
}

func ValidateAndStartRun(run models.Run, req interface{}) (CreateRunResponse, error) {
	if err := run.Encode(); err != nil {
		logger.Logger().Errorf("encode run failed. error:%s", err.Error())
		return CreateRunResponse{}, err
	}
	// validate workflow in func NewWorkflow
	if _, err := newWorkflowByRun(run); err != nil {
		logger.Logger().Errorf("validateAndInitWorkflow. err:%v", err)
		return CreateRunResponse{}, err
	}
	// create run in db and update run's ID by pk
	runID, err := models.CreateRun(logger.Logger(), &run)
	if err != nil {
		logger.Logger().Errorf("create run failed inserting db. error:%s", err.Error())
		return CreateRunResponse{}, err
	}
	// to wfs again to revise previous wf replacement
	wfs, err := runYamlAndReqToWfs(run.RunYaml, req)
	if err != nil {
		logger.Logger().Errorf("runYamlAndReqToWfs failed. err:%v", err)
		return CreateRunResponse{}, err
	}

	run.WorkflowSource = wfs
	// handler image
	if err := handleImageAndStartWf(run, false); err != nil {
		logger.Logger().Errorf("create run[%s] failed handleImageAndStartWf[%s-%s]. error:%s\n", runID, wfs.DockerEnv, run.FsID, err.Error())
	}
	logger.Logger().Debugf("create run successful. runID:%s\n", runID)
	response := CreateRunResponse{
		RunID: runID,
	}
	return response, nil
}

func ListRun(ctx *logger.RequestContext, marker string, maxKeys int, userFilter, fsFilter, runFilter, nameFilter []string) (ListRunResponse, error) {
	ctx.Logging().Debugf("begin list run.")
	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return ListRunResponse{}, err
		}
	}
	// normal user list its own
	if !common.IsRootUser(ctx.UserName) {
		userFilter = []string{ctx.UserName}
	}
	// model list
	runList, err := models.ListRun(ctx.Logging(), pk, maxKeys, userFilter, fsFilter, runFilter, nameFilter)
	if err != nil {
		ctx.Logging().Errorf("models list run failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
	}
	listRunResponse := ListRunResponse{RunList: []RunBrief{}}

	// get next marker
	listRunResponse.IsTruncated = false
	if len(runList) > 0 {
		run := runList[len(runList)-1]
		if !isLastRunPk(ctx, run.Pk) {
			nextMarker, err := common.EncryptPk(run.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					run.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return ListRunResponse{}, err
			}
			listRunResponse.NextMarker = nextMarker
			listRunResponse.IsTruncated = true
		}
	}
	listRunResponse.MaxKeys = maxKeys
	// append run briefs
	for _, run := range runList {
		briefRun := RunBrief{}
		briefRun.modelToListResp(run)
		listRunResponse.RunList = append(listRunResponse.RunList, briefRun)
	}
	return listRunResponse, nil
}

func isLastRunPk(ctx *logger.RequestContext, pk int64) bool {
	lastRun, err := models.GetLastRun(ctx.Logging())
	if err != nil {
		ctx.Logging().Errorf("get last run failed. error:[%s]", err.Error())
	}
	if lastRun.Pk == pk {
		return true
	}
	return false
}

func GetRunByID(ctx *logger.RequestContext, runID string) (models.Run, error) {
	ctx.Logging().Debugf("begin get run by id. runID:%s", runID)
	run, err := models.GetRunByID(ctx.Logging(), runID)
	if err != nil {
		ctx.ErrorCode = common.RunNotFound
		ctx.Logging().Errorln(err.Error())
		return models.Run{}, common.NotFoundError(common.ResourceTypeRun, runID)
	}
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != run.UserName {
		err := common.NoAccessError(ctx.UserName, common.ResourceTypeRun, runID)
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln(err.Error())
		return models.Run{}, err
	}
	return run, nil
}

func StopRun(ctx *logger.RequestContext, runID string, request UpdateRunRequest) error {
	ctx.Logging().Debugf("begin stop run. runID:%s", runID)
	// check run exist
	run, err := GetRunByID(ctx, runID)
	if err != nil {
		ctx.Logging().Errorf("stop run[%s] failed when getting run. error: %v", runID, err)
		return err
	}
	// check user access right
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != run.UserName {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorf("non-admin user[%s] has no access to stop run[%s]", ctx.UserName, runID)
		return err
	}
	// check run current status
	if run.Status == common.StatusRunTerminating && !request.StopForce ||
		common.IsRunFinalStatus(run.Status) {
		err := fmt.Errorf("cannot stop run[%s] as run is already in status[%s]", runID, run.Status)
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return err
	}

	wf, exist := wfMap[runID]
	if !exist {
		ctx.ErrorCode = common.InternalError
		err := fmt.Errorf("run[%s]'s workflow ptr is lost", runID)
		ctx.Logging().Errorln(err.Error())
		return err
	}
	if err := models.UpdateRunStatus(ctx.Logging(), runID, common.StatusRunTerminating); err != nil {
		ctx.ErrorCode = common.InternalError
		return errors.New("stop run failed updating db")
	}
	wf.Stop(request.StopForce)
	ctx.Logging().Debugf("close run succeed. runID:%s", runID)
	return nil
}

func RetryRun(ctx *logger.RequestContext, runID string) error {
	ctx.Logging().Debugf("begin retry run. runID:%s\n", runID)
	// check run exist
	run, err := GetRunByID(ctx, runID)
	if err != nil {
		ctx.Logging().Errorf("retry run[%s] failed when getting run. error: %v\n", runID, err)
		return err
	}
	// check user access right

	if !common.IsRootUser(ctx.UserName) && ctx.UserName != run.UserName {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorf("non-admin user[%s] has no access to retry run[%s]\n", ctx.UserName, runID)
		return err
	}
	// check run current status. If already succeeded or running/pending, no need to retry this run.
	// only failed or terminated runs can retry
	if !(run.Status == common.StatusRunFailed || run.Status == common.StatusRunTerminated) {
		err := fmt.Errorf("run[%s] has status[%s], no need to retry", runID, run.Status)
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return err
	}
	// reset run steps
	if err := resetRunSteps(&run); err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("resetRunSteps failed. err:%v\n", err)
		return err
	}
	// resume
	if err := resumeRun(run); err != nil {
		ctx.Logging().Errorf("retry run[%s] failed resumeRun. run:%+v. error:%s\n",
			runID, run, err.Error())
	}
	ctx.Logging().Debugf("retry run[%s] successful", runID)
	return nil
}

func DeleteRun(ctx *logger.RequestContext, id string, request *DeleteRunRequest) error {
	ctx.Logging().Debugf("begin delete run: %s", id)
	run, err := models.GetRunByID(ctx.Logging(), id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RunNotFound
			err := fmt.Errorf("delete run[%s] failed. not exist", id)
			ctx.Logging().Errorf(err.Error())
			return err
		} else {
			ctx.ErrorCode = common.InternalError
			ctx.Logging().Errorf("delete run[%s] failed. err:%v", id, err)
			return err
		}
	}
	// check permission
	if !common.IsRootUser(ctx.UserName) && ctx.UserName != run.UserName {
		ctx.ErrorCode = common.AccessDenied
		err := fmt.Errorf("delete run[%s] failed. Access denied", id)
		ctx.Logging().Errorln(err.Error())
		return err
	}
	// check final status
	if !common.IsRunFinalStatus(run.Status) {
		ctx.ErrorCode = common.ActionNotAllowed
		err := fmt.Errorf("run[%s] is in status[%s]. only runs in final status: %v can be deleted", run.ID, run.Status, common.RunFinalStatus)
		ctx.Logging().Errorln(err.Error())
		return err
	}
	// check cache
	runCacheIDList := run.GetRunCacheIDList()
	if request.CheckCache && len(runCacheIDList) > 0 {
		// 由于cache当前正要删除的Run的其他Run可能已经被删除了，所以需要检查实际存在的Run有哪些
		if runCachedList, _ := models.ListRun(ctx.Logging(), 0, 0, nil, nil, runCacheIDList, nil); len(runCachedList) > 0 {
			// 为了错误信息更友好，把实际还存在的Run的ID打印出来
			runExistIDList := make([]string, 0, len(runCachedList))
			for _, runCached := range runCachedList {
				runExistIDList = append(runExistIDList, runCached.ID)
			}

			err := fmt.Errorf("delete run[%s] failed. run deleting is cached by other Runs[%v].", id, runExistIDList)
			ctx.Logging().Errorf(err.Error())
			ctx.ErrorCode = common.InternalError
			return err
		}
	}

	// 删除pipeline run outputAtf (只有Fs不为空，才需要清理artifact。因为不使用Fs时，不允许定义outputAtf)
	if run.FsID != "" {
		resourceHandler, err := pipeline.NewResourceHandler(id, run.FsID, ctx.Logging())
		if err != nil {
			ctx.Logging().Errorf("delete run[%s] failed. Init handler failed. err: %v", id, err.Error())
			ctx.ErrorCode = common.InternalError
			return err
		}
		if err := resourceHandler.ClearResource(); err != nil {
			ctx.Logging().Errorf("delete run[%s] failed. Delete artifact failed. err: %v", id, err.Error())
			ctx.ErrorCode = common.InternalError
			return err
		}
	}

	// delete
	if err := models.DeleteRun(ctx.Logging(), id); err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("models delete run[%s] failed. error:%s", id, err.Error())
		return err
	}
	return nil
}

func InitAndResumeRuns() (*handler.ImageHandler, error) {
	imageHandler, err := handler.InitPFImageHandler()
	if err != nil {
		return nil, err
	}
	// do not handle resume errors
	resumeActiveRuns()
	return imageHandler, nil
}

// --------- internal funcs ---------//
func resumeActiveRuns() error {
	runList, err := models.ListRunsByStatus(logger.Logger(), common.RunActiveStatus)
	if err != nil {
		if database.GetErrorCode(err) == database.ErrorRecordNotFound {
			logger.LoggerForRun("").Infof("ResumeActiveRuns: no active runs to resume")
			return nil
		} else {
			logger.LoggerForRun("").Errorf("ResumeActiveRuns: failed listing runs. error:%v", err)
			return err
		}
	}
	go func() {
		for _, run := range runList {
			logger.LoggerForRun(run.ID).Debugf("ResumeActiveRuns: run[%s] with status[%s] begins to resume\n", run.ID, run.Status)
			if err := resumeRun(run); err != nil {
				logger.LoggerForRun(run.ID).Warnf("ResumeActiveRuns: run[%s] with status[%s] failed to resume. skipped.", run.ID, run.Status)
			}
		}
	}()
	return nil
}

func resumeRun(run models.Run) error {
	if run.RunCachedIDs != "" {
		// 由于非成功完成的Run也会被Cache，且重跑会直接对原始的Run记录进行修改，
		// 因此被Cache的Run不能重跑
		// TODO: 考虑将重跑逻辑又“直接修改Run记录”改为“根据该Run的设置，重新发起Run”
		err := fmt.Errorf("can not retry run cached.")
		logger.LoggerForRun(run.ID).Errorf(err.Error())
		return err
	}

	wfs, err := schema.ParseWorkflowSource([]byte(run.RunYaml))
	if err != nil {
		logger.LoggerForRun(run.ID).Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", run.RunYaml, err)
		return err
	}

	wfs.Name = run.Name
	if run.DockerEnv != "" {
		wfs.DockerEnv = run.DockerEnv
	}
	if run.Disabled != "" {
		wfs.Disabled = run.Disabled
	}
	// patch run.WorkflowSource to invoke func handleImageAndStartWf
	run.WorkflowSource = wfs
	if err := handleImageAndStartWf(run, true); err != nil {
		logger.LoggerForRun(run.ID).Errorf("resume run[%s] failed handleImageAndStartWf. DockerEnv[%s] fsID[%s]. error:%s\n",
			run.ID, run.WorkflowSource.DockerEnv, run.FsID, err.Error())
	}
	return nil
}

// handleImageAndStartWf patch run.WorkflowSource before invoke this func!
func handleImageAndStartWf(run models.Run, isResume bool) error {
	logEntry := logger.LoggerForRun(run.ID)
	logEntry.Debugf("start handleImageAndStartWf isResume:%t, run:%+v", isResume, run)
	if !handler.NeedHandleImage(run.WorkflowSource.DockerEnv) {
		// init workflow and start
		wfPtr, err := newWorkflowByRun(run)
		if err != nil {
			logEntry.Errorf("newWorkflowByRun failed. err:%v\n", err)
			return updateRunStatusAndMsg(run.ID, common.StatusRunFailed, err.Error())
		}
		if !isResume {
			err := models.UpdateRun(logEntry, run.ID,
				models.Run{DockerEnv: run.WorkflowSource.DockerEnv, Status: common.StatusRunPending})
			if err != nil {
				return err
			}
			// start workflow with image url
			wfPtr.Start()
			logEntry.Debugf("workflow started, run:%+v", run)
		} else {
			// set runtime and restart
			if err := wfPtr.SetWorkflowRuntime(run.Runtime, run.PostProcess); err != nil {
				logEntry.Errorf("SetWorkflowRuntime for run[%s] failed. error:%v\n", run.ID, err)
				return err
			}
			if len(run.Runtime) > 0 {
				// 确保在run_job表有对应的记录时，run记录中的status字段不是pending，进而防止多次创建run_job记录
				err := models.UpdateRun(logEntry, run.ID,
					models.Run{DockerEnv: run.WorkflowSource.DockerEnv, Status: common.StatusRunRunning})
				if err != nil {
					return err
				}
			} else {
				// 如果数据库中没有run_job记录，那么为防止Run为init状态，导致无法创建run_job记录，这里将Run的状态置为pending
				err := models.UpdateRun(logEntry, run.ID,
					models.Run{DockerEnv: run.WorkflowSource.DockerEnv, Status: common.StatusRunPending})
				if err != nil {
					return err
				}
			}
			wfPtr.Restart()
			logEntry.Debugf("workflow restarted, run:%+v", run)
		}
		return nil
	} else {
		imageIDs, err := models.ListImageIDsByFsID(logEntry, run.FsID)
		if err != nil {
			logEntry.Errorf("create run failed ListImageIDsByFsID[%s]. error:%s\n", run.FsID, err.Error())
			return updateRunStatusAndMsg(run.ID, common.StatusRunFailed, err.Error())
		}
		if err := handler.PFImageHandler.HandleImage(run.WorkflowSource.DockerEnv, run.ID, run.FsID,
			imageIDs, logEntry, handleImageCallbackFunc); err != nil {
			logEntry.Errorf("handle image failed. error:%s\n", err.Error())
			return updateRunStatusAndMsg(run.ID, common.StatusRunFailed, err.Error())
		}
	}
	return nil
}

func newWorkflowByRun(run models.Run) (*pipeline.Workflow, error) {
	extraInfo := map[string]string{
		pplcommon.WfExtraInfoKeySource:   run.Source,
		pplcommon.WfExtraInfoKeyFsID:     run.FsID,
		pplcommon.WfExtraInfoKeyUserName: run.UserName,
		pplcommon.WfExtraInfoKeyFsName:   run.FsName,
	}
	wfPtr, err := pipeline.NewWorkflow(run.WorkflowSource, run.ID, run.Entry, run.Parameters, extraInfo, workflowCallbacks)
	if err != nil {
		logger.LoggerForRun(run.ID).Warnf("NewWorkflow by run[%s] failed. error:%v\n", run.ID, err)
		return nil, err
	}
	if wfPtr == nil {
		err := fmt.Errorf("NewWorkflow ptr for run[%s] is nil", run.ID)
		logger.LoggerForRun(run.ID).Errorln(err.Error())
		return nil, err
	}
	if run.ID != "" { // validate has run.ID == "". do not record
		wfMap[run.ID] = wfPtr
	}
	return wfPtr, nil
}

func resetRunSteps(run *models.Run) error {
	if err := resetRuntimeSteps(run.Runtime); err != nil {
		err = fmt.Errorf("failed to retry run[%s], error: %v", run.ID, err)
		logger.LoggerForRun(run.ID).Errorf(err.Error())
		return err
	}

	if err := resetRuntimeSteps(run.PostProcess); err != nil {
		err = fmt.Errorf("failed to retry run[%s], error: %v", run.ID, err)
		logger.LoggerForRun(run.ID).Errorf(err.Error())
		return err
	}

	if err := run.Encode(); err != nil {
		logger.LoggerForRun(run.ID).Errorf("reset run steps encode failure. err: %v", err)
		return err
	}
	return models.UpdateRun(logger.LoggerForRun(run.ID), run.ID, *run)
}

func resetRuntimeSteps(runtime map[string]schema.JobView) error {
	for stepName, jobView := range runtime {
		if jobView.Status == schema.StatusJobCancelled ||
			jobView.Status == schema.StatusJobFailed ||
			jobView.Status == schema.StatusJobTerminated {
			jobView.JobID = ""
			jobView.Status = ""
			jobView.StartTime = ""
			jobView.EndTime = ""

			runtime[stepName] = jobView
		}
		if jobView.Status == schema.StatusJobRunning ||
			jobView.Status == schema.StatusJobTerminating {
			err := fmt.Errorf("step[%s] has invalid status[%s]", stepName, jobView.Status)
			return err
		}
	}
	return nil
}
