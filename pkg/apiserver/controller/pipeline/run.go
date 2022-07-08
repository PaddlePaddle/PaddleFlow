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
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	errors2 "github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

var wfMap = make(map[string]*pipeline.Workflow, 0)

const (
	JsonFsOptions   = "fsOptions"
	JsonUserName    = "userName"
	JsonDescription = "description"
	JsonFlavour     = "flavour"
	JsonQueue       = "queue"
	JsonJobType     = "jobType"
	JsonEnv         = "env"
)

type CreateRunRequest struct {
	GlobalFsName string                 `json:"globalFsName"`
	UserName     string                 `json:"username,omitempty"`   // optional, only for root user
	Name         string                 `json:"name,omitempty"`       // optional
	Description  string                 `json:"desc,omitempty"`       // optional
	Parameters   map[string]interface{} `json:"parameters,omitempty"` // optional
	DockerEnv    string                 `json:"dockerEnv,omitempty"`  // optional
	// run workflow source. priority: RunYamlRaw > PipelineID + PipelineDetailID > RunYamlPath
	// 为了防止字符串或者不同的http客户端对run.yaml
	// 格式中的特殊字符串做特殊过滤处理导致yaml文件不正确，因此采用runYamlRaw采用base64编码传输
	Disabled         string `json:"disabled,omitempty"`         // optional
	RunYamlRaw       string `json:"runYamlRaw,omitempty"`       // optional. one of 3 sources of run. high priority
	PipelineID       string `json:"pipelineID,omitempty"`       // optional. one of 3 sources of run. medium priority
	PipelineDetailID string `json:"pipelineDetailID,omitempty"` // optional. one of 3 sources of run. medium priority
	RunYamlPath      string `json:"runYamlPath,omitempty"`      // optional. one of 3 sources of run. low priority
	ScheduleID       string `json:"scheduleID"`
	ScheduledAt      string `json:"scheduledAt"`
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
	ID            string `json:"runID"`
	Name          string `json:"name"`
	Source        string `json:"source"` // pipelineID or yamlPath
	UserName      string `json:"username"`
	GlobalFsName  string `json:"globalFsName"`
	Description   string `json:"description"`
	ScheduleID    string `json:"scheduleID"`
	Message       string `json:"runMsg"`
	Status        string `json:"status"`
	ScheduledTime string `json:"scheduledTime"`
	CreateTime    string `json:"createTime"`
	ActivateTime  string `json:"activateTime"`
	UpdateTime    string `json:"updateTime"`
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
	b.GlobalFsName = run.GlobalFsName
	b.Description = run.Description
	b.ScheduleID = run.ScheduleID
	b.Message = run.Message
	b.Status = run.Status
	b.CreateTime = run.CreateTime
	b.ActivateTime = run.ActivateTime
	b.UpdateTime = run.UpdateTime

	if run.ScheduledAt.Valid {
		b.ScheduledTime = run.ScheduledAt.Time.Format("2006-01-02 15:04:05")
	} else {
		b.ScheduledTime = ""
	}
}

func buildWorkflowSource(ctx logger.RequestContext, req CreateRunRequest, fsID string) (schema.WorkflowSource, string, string, error) {
	var source, runYaml, requestId, userName string
	requestId, userName = ctx.RequestID, ctx.UserName

	trace_logger.Key(requestId).Infof("retrieve source and runYaml")
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
		wfs, err := schema.GetWorkflowSource([]byte(runYaml))
		if err != nil {
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
		hasAuth, _, err := CheckPipelinePermission(userName, req.PipelineID)
		if err != nil {
			logger.Logger().Errorf("buildWorkflowSource for pipeline[%s] failed. err:%v", req.PipelineID, err)
			return schema.WorkflowSource{}, "", "", err
		} else if !hasAuth {
			err := common.NoAccessError(userName, common.ResourceTypePipeline, req.PipelineID)
			logger.Logger().Errorf("buildWorkflowSource for pipeline[%s] failed. err:%v", req.PipelineID, err)
			return schema.WorkflowSource{}, "", "", err
		}

		// query pipeline detail
		var pplDetail models.PipelineDetail
		if req.PipelineDetailID == "" {
			pplDetail, err = models.GetLastPipelineDetail(req.PipelineID)
			return schema.WorkflowSource{}, "", "", err
		} else {
			pplDetail, err = models.GetPipelineDetail(req.PipelineID, req.PipelineDetailID)
			if err != nil {
				logger.Logger().Errorf("get detail[%s] of pipeline[%s]. err: %v", req.PipelineDetailID, req.PipelineID, err)
				return schema.WorkflowSource{}, "", "", err
			}
		}

		runYaml = pplDetail.PipelineYaml
		source = fmt.Sprintf("%s-%s", req.PipelineID, req.PipelineDetailID)
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

	// 检查 yaml 格式
	if err := yaml.UnmarshalStrict([]byte(runYaml), map[interface{}]interface{}{}); err != nil {
		logger.Logger().Errorf("runYaml format invalid. err:%v", err)
		return schema.WorkflowSource{}, "", "", err
	}

	// to wfs
	trace_logger.Key(requestId).Infof("run yaml and req to wfs")
	wfs, err := runYamlAndReqToWfs(runYaml, req)
	if err != nil {
		logger.Logger().Errorf("runYamlAndReqToWfs failed. err:%v", err)
		return schema.WorkflowSource{}, "", "", err
	}
	return wfs, source, runYaml, nil
}

// Used for API CreateRunJson, get wfs by json request.
func getWorkFlowSourceByJson(bodyMap map[string]interface{}) (schema.WorkflowSource, error) {
	// Json接口有，但runYaml没有的字段
	JsonAttrMap := map[string]interface{}{
		JsonFlavour: nil,
		JsonJobType: nil,
		JsonQueue:   nil,
		JsonEnv:     nil,

		//这2个字段，之前已经处理过，后续的Json解析逻辑无需处理，只需剔除即可
		JsonDescription: nil,
		JsonUserName:    nil,
	}

	// 先把Json接口有，但是Yaml接口没有的参数提取出来保存
	for key := range JsonAttrMap {
		JsonAttrMap[key] = bodyMap[key]
		delete(bodyMap, key)
	}

	// 将字段名由Json风格改为Yaml风格
	p := schema.Parser{}
	if err := p.TransJsonMap2Yaml(bodyMap); err != nil {
		return schema.WorkflowSource{}, err
	}

	// 获取yaml版的Wfs
	wfs, err := schema.GetWorkflowSourceByMap(bodyMap)
	if err != nil {
		logger.Logger().Errorf("get workflowSource in json failed, error: %s", err.Error())
		return schema.WorkflowSource{}, err
	}

	// 再处理Json接口特有的参数
	globalEnvMap, err := ParseJsonGlobalEnv(JsonAttrMap)
	if err != nil {
		return schema.WorkflowSource{}, err
	}

	// 处理components, entryPoints, postProcess
	// 全局Env替换节点Env，节点Env的优先级更高
	if err := processRunJsonComponents(wfs.Components, globalEnvMap); err != nil {
		logger.Logger().Errorf(err.Error())
		return schema.WorkflowSource{}, err
	}

	if err := processRunJsonComponents(wfs.EntryPoints.EntryPoints, globalEnvMap); err != nil {
		logger.Logger().Errorf(err.Error())
		return schema.WorkflowSource{}, err
	}

	postMap := map[string]schema.Component{}
	for k, v := range wfs.PostProcess {
		postMap[k] = v
	}
	if err := processRunJsonComponents(postMap, globalEnvMap); err != nil {
		logger.Logger().Errorf(err.Error())
		return schema.WorkflowSource{}, err
	}

	return wfs, nil
}

func ParseJsonGlobalEnv(jsonAttrMap map[string]interface{}) (map[string]string, error) {
	resMap := map[string]string{}

	for key, value := range jsonAttrMap {
		switch key {
		case JsonFlavour:
			if value != nil {
				value, ok := value.(string)
				if !ok {
					return nil, fmt.Errorf("[flavour] should be string type")
				}
				if _, ok := resMap[schema.EnvJobFlavour]; !ok {
					resMap[schema.EnvJobFlavour] = value
				}
			}
		case JsonQueue:
			if value != nil {
				value, ok := value.(string)
				if !ok {
					return nil, fmt.Errorf("[queue] should be string type")
				}
				if _, ok := resMap[schema.EnvJobQueueName]; !ok {
					resMap[schema.EnvJobQueueName] = value
				}
			}
		case JsonJobType:
			if value != nil {
				value, ok := value.(string)
				if !ok {
					return nil, fmt.Errorf("[jobType] should be string type")
				}
				if _, ok := resMap[schema.EnvJobType]; !ok {
					resMap[schema.EnvJobType] = value
				}
			}
		case JsonEnv:
			if value != nil {
				value, ok := value.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("[env] should be map type")
				}
				for envKey, envValue := range value {
					envValue := envValue.(string)
					if !ok {
						return nil, fmt.Errorf("value of env should be string type")
					}
					resMap[envKey] = envValue
				}
			}
		}
	}
	return resMap, nil
}

// 该函数主要完成CreateRunJson接口中，各类变量的全局替换操作
func processRunJsonComponents(components map[string]schema.Component, envMap map[string]string) error {
	for _, component := range components {
		if dag, ok := component.(*schema.WorkflowSourceDag); ok {
			if err := processRunJsonComponents(dag.EntryPoints, envMap); err != nil {
				return err
			}
		} else if step, ok := component.(*schema.WorkflowSourceStep); ok {
			if step.Env == nil {
				step.Env = map[string]string{}
			}
			if step.Parameters == nil {
				step.Parameters = map[string]interface{}{}
			}
			// Reference节点无需替换
			if step.Reference.Component == "" {
				// 全局Env与节点Env合并，全局优先级高于节点
				for globalKey, globalValue := range envMap {
					step.Env[globalKey] = globalValue
				}
			}
		} else {
			return fmt.Errorf("component is not dag or step")
		}
	}
	return nil
}

func getSourceAndYaml(wfs schema.WorkflowSource) (string, string, error) {
	yamlByte, err := yaml.Marshal(wfs)
	if err != nil {
		logger.Logger().Errorf("marshal workFlowSource to yaml faild. err: %v", err)
		return "", "", err
	}
	// 由于wfs中的EntryPoints字段的类型为WorkflowSourceDag，而非map，因此直接marshal得到的yaml格式有误，不可用
	// 需要将yaml转成map，进行处理后再转回yaml
	yamlMap, err := schema.RunYaml2Map(yamlByte)
	if err != nil {
		return "", "", err
	}

	// 提取 entry_points
	entryPointsMap, ok, err := unstructured.NestedFieldNoCopy(yamlMap, "entry_points", "entry_points")
	if err != nil {
		return "", "", fmt.Errorf("get entry_points map from yamlMap failed, error: %s", err.Error())
	}
	if !ok {
		logger.Logger().Warnf("runYaml Map doesn't have entry_points")
	}
	if err := unstructured.SetNestedField(yamlMap, entryPointsMap, "entry_points"); err != nil {
		return "", "", err
	}
	resYamlByte, err := yaml.Marshal(yamlMap)
	if err != nil {
		return "", "", err
	}

	runYaml := string(resYamlByte)
	source := common.GetMD5Hash(resYamlByte)
	return source, runYaml, nil
}

func runYamlAndReqToWfs(runYaml string, req CreateRunRequest) (schema.WorkflowSource, error) {
	// parse yaml -> WorkflowSource
	logger.Logger().Infof("debug: run yaml is :\n %s", runYaml)
	wfs, err := schema.GetWorkflowSource([]byte(runYaml))
	if err != nil {
		logger.Logger().Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", runYaml, err)
		return schema.WorkflowSource{}, err
	}

	// replace name & dockerEnv & disabled by request
	if req.Name != "" {
		wfs.Name = req.Name
	}
	if req.DockerEnv != "" {
		wfs.DockerEnv = req.DockerEnv
	}
	if req.Disabled != "" {
		wfs.Disabled = req.Disabled
	}
	if req.GlobalFsName != "" {
		wfs.FsOptions.GlobalFsName = req.GlobalFsName
	}
	return wfs, nil
}

func CreateRun(ctx logger.RequestContext, request *CreateRunRequest) (CreateRunResponse, error) {
	// concatenate globalFsID
	globalFsID := ""
	requestId := ctx.RequestID
	ctxUserName := ctx.UserName // 这是实际发送请求的用户，由Token决定，全局不会改变
	userName := ctxUserName     // 这是进行后续fs操作的用户，root用户可以设置为其他普通用户
	if common.IsRootUser(ctxUserName) && request.UserName != "" {
		// root user can select fs under other users
		userName = request.UserName
	}

	if request.GlobalFsName != "" {
		globalFsID = common.ID(userName, request.GlobalFsName)
	}

	// TODO:// validate flavour
	// TODO:// validate queue

	trace_logger.Key(requestId).Infof("build workflow source for run: %+v", request)
	wfs, source, runYaml, err := buildWorkflowSource(ctx, *request, globalFsID)
	if err != nil {
		logger.Logger().Errorf("buildWorkflowSource failed. error:%v", err)
		return CreateRunResponse{}, err
	}

	// 如果request里面的fsID为空，那么需要判断yaml（通过PipelineID或Raw上传的）中有无指定GlobalFs，有则生成fsID
	if request.GlobalFsName == "" && wfs.FsOptions.GlobalFsName != "" {
		globalFsID = common.ID(userName, wfs.FsOptions.GlobalFsName)
	}

	trace_logger.Key(requestId).Infof("check name reg pattern: %s", wfs.Name)
	// check name pattern
	if wfs.Name != "" && !schema.CheckReg(wfs.Name, common.RegPatternRunName) {
		err := common.InvalidNamePatternError(wfs.Name, common.ResourceTypeRun, common.RegPatternRunName)
		logger.Logger().Errorf("create run failed as run name illegal. error:%v", err)
		return CreateRunResponse{}, err
	}

	scheduledAt := sql.NullTime{}
	if request.ScheduledAt == "" {
		scheduledAt = sql.NullTime{Valid: false}
	} else {
		scheduledAt.Valid = true
		scheduledAt.Time, err = time.ParseInLocation("2006-01-02 15:04:05", request.ScheduledAt, time.Local)
		if err != nil {
			errMsg := fmt.Sprintf("scheduledAt[%s] format not correct, should be YYYY-MM-DD hh:mm:ss", request.ScheduledAt)
			return CreateRunResponse{}, fmt.Errorf(errMsg)
		}
	}

	// create run in db after run.yaml validated
	run := models.Run{
		ID:             "", // to be back filled according to db pk
		Name:           wfs.Name,
		Source:         source,
		UserName:       ctxUserName,
		GlobalFsName:   request.GlobalFsName,
		GlobalFsID:     globalFsID,
		Description:    request.Description,
		Parameters:     request.Parameters,
		RunYaml:        runYaml,
		WorkflowSource: wfs, // DockerEnv has not been replaced. done in func handleImageAndStartWf
		Disabled:       request.Disabled,
		ScheduleID:     request.ScheduleID,
		ScheduledAt:    scheduledAt,
		Status:         common.StatusRunInitiating,
	}
	trace_logger.Key(requestId).Infof("validate and start run: %+v", run)
	response, err := ValidateAndStartRun(ctx, run, userName, *request)
	return response, err
}

func CreateRunByJson(ctx logger.RequestContext, bodyMap map[string]interface{}) (CreateRunResponse, error) {
	requestId := ctx.RequestID

	// 从request body中提取部分信息，这些信息与workflow没有直接关联
	var reqFsName string
	var reqUserName string
	var reqDescription string

	parser := schema.Parser{}
	fsMap, ok := bodyMap[JsonFsOptions].(map[string]interface{})
	if ok {
		fsOptions := schema.FsOptions{}
		if err := parser.ParseFsOptions(fsMap, &fsOptions); err != nil {
			logger.Logger().Errorf("check fsOptions failed, error: %s", err.Error())
			return CreateRunResponse{}, err
		}
		reqFsName = fsOptions.GlobalFsName
	}
	if _, ok := bodyMap[JsonUserName].(string); ok {
		reqUserName = bodyMap[JsonUserName].(string)
	}
	if _, ok := bodyMap[JsonDescription].(string); ok {
		reqDescription = bodyMap[JsonDescription].(string)
	}

	globalFsID := ""
	ctxUserName := ctx.UserName // 这是实际发送请求的用户，由Token决定，全局不会改变
	userName := ctxUserName     // 这是进行后续fs操作的用户，root用户可以设置为其他普通用户
	if reqFsName != "" {
		if common.IsRootUser(ctxUserName) && reqUserName != "" {
			// root user can select fs under other users
			userName = reqUserName
		}
		globalFsID = common.ID(userName, reqFsName)
	}

	trace_logger.Key(requestId).Infof("get workflow source for run: %+v", bodyMap)
	wfs, err := getWorkFlowSourceByJson(bodyMap)
	if err != nil {
		logger.Logger().Errorf("get WorkFlowSource by request failed. error:%v", err)
		return CreateRunResponse{}, err
	}

	trace_logger.Key(requestId).Infof("get source and yaml for run: %+v", bodyMap)
	source, runYaml, err := getSourceAndYaml(wfs)
	if err != nil {
		logger.Logger().Errorf("get source and yaml by workflowsource failed. error:%v", err)
		return CreateRunResponse{}, err
	}

	trace_logger.Key(requestId).Infof("check name reg pattern: %s", wfs.Name)
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
		UserName:       ctxUserName,
		GlobalFsName:   reqFsName,
		GlobalFsID:     globalFsID,
		Description:    reqDescription,
		RunYaml:        runYaml,
		WorkflowSource: wfs, // DockerEnv has not been replaced. done in func handleImageAndStartWf
		Disabled:       wfs.Disabled,
		Status:         common.StatusRunInitiating,
	}
	trace_logger.Key(requestId).Infof("validate and start run: %+v", run)
	response, err := ValidateAndStartRun(ctx, run, userName, CreateRunRequest{})
	return response, err
}

func ValidateAndStartRun(ctx logger.RequestContext, run models.Run, userName string, req CreateRunRequest) (CreateRunResponse, error) {
	requestId := ctx.RequestID
	if requestId == "" {
		errMsg := "get requestID failed"
		logger.Logger().Errorf("validate and start run failed. error:%s", errMsg)
		return CreateRunResponse{}, errors.New(errMsg)
	}

	// 给所有Step的fsMount和fsScope的fsID赋值
	fsIDs, err := run.WorkflowSource.ProcessFsAndGetAllIDs(userName)
	if err != nil {
		logger.Logger().Errorf("process fs failed. error: %s", err.Error())
		return CreateRunResponse{}, err
	}

	//检查fs权限
	if run.GlobalFsID != "" {
		fsIDs = append(fsIDs, run.GlobalFsID)
	}

	for _, id := range fsIDs {
		fsService := fs.GetFileSystemService()
		hasPermission, err := fsService.HasFsPermission(run.UserName, id)
		if err != nil {
			err := fmt.Errorf("check fs permission failed with userName[%s] and fsID[%s]. error: %s",
				run.UserName, id, err.Error())
			logger.Logger().Errorf(err.Error())
			return CreateRunResponse{}, err
		}
		if !hasPermission {
			err := fmt.Errorf("user[%s] has no permission to fs[%s]", run.UserName, id)
			logger.Logger().Errorf(err.Error())
			return CreateRunResponse{}, err
		}
	}

	trace_logger.Key(requestId).Infof("encode run")
	if err := run.Encode(); err != nil {
		logger.Logger().Errorf("encode run failed. error:%s", err.Error())
		return CreateRunResponse{}, err
	}

	trace_logger.Key(requestId).Infof("validate and init workflow")
	// validate workflow in func NewWorkflow
	if _, err := newWorkflowByRun(run); err != nil {
		logger.Logger().Errorf("validateAndInitWorkflow. err:%v", err)
		return CreateRunResponse{}, err
	}

	// generate run id here
	trace_logger.Key(requestId).Infof("create run in db")
	// create run in db and update run's ID by pk
	runID, err := models.CreateRun(logger.Logger(), &run)
	if err != nil {
		logger.Logger().Errorf("create run failed inserting db. error:%s", err.Error())
		return CreateRunResponse{}, err
	}

	// update trace logger key
	_ = trace_logger.UpdateKey(requestId, runID)
	trace_logger.Key(runID).Infof("create run in db success")

	trace_logger.Key(runID).Infof("run yaml and req to wfs")
	// to wfs again to revise previous wf replacement
	// wfs, err := runYamlAndReqToWfs(run.RunYaml, req)
	// if err != nil {
	// 	logger.Logger().Errorf("runYamlAndReqToWfs failed. err:%v", err)
	// 	return CreateRunResponse{}, err
	// }
	// run.WorkflowSource = wfs
	defer func() {
		if info := recover(); info != nil {
			errmsg := fmt.Sprintf("StartWf failed, %v", info)
			logger.LoggerForRun(runID).Errorf(errmsg)
			if err := updateRunStatusAndMsg(runID, common.StatusRunFailed, errmsg); err != nil {
				logger.LoggerForRun(runID).Errorf("set run status as failed after StartWf panic failed")
			}
		}
	}()

	trace_logger.Key(runID).Infof("handle image and start wf: %+v", run)
	// handler image
	if err := handleImageAndStartWf(run, false); err != nil {
		logger.Logger().Errorf("create run[%s] failed handleImageAndStartWf[%s-%s]. error:%s\n", runID, run.WorkflowSource.DockerEnv, run.GlobalFsID, err.Error())
	}
	logger.Logger().Debugf("create run successful. runID:%s\n", runID)
	response := CreateRunResponse{
		RunID: runID,
	}
	return response, nil
}

func ListRun(ctx *logger.RequestContext, marker string, maxKeys int, userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIDFilter []string) (ListRunResponse, error) {
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
	runList, err := models.ListRun(ctx.Logging(), pk, maxKeys, userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIDFilter)
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

func GetRunByID(logEntry *log.Entry, userName string, runID string) (models.Run, error) {
	logEntry.Debugf("begin get run by id. runID:%s", runID)
	run, err := models.GetRunByID(logEntry, runID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			err = common.NotFoundError(common.ResourceTypeRun, runID)
		}
		logEntry.Errorln(err.Error())
		return models.Run{}, err
	}

	if !common.IsRootUser(userName) && userName != run.UserName {
		err := common.NoAccessError(userName, common.ResourceTypeRun, runID)
		logEntry.Errorln(err.Error())
		return models.Run{}, err
	}
	return run, nil
}

func StopRun(logEntry *log.Entry, userName, runID string, request UpdateRunRequest) error {
	logEntry.Debugf("begin stop run. runID:%s", runID)
	// check run exist && check user access right
	run, err := GetRunByID(logEntry, userName, runID)
	if err != nil {
		logEntry.Errorf("stop run[%s] failed when getting run. error: %v", runID, err)
		return err
	}

	// check run current status
	if run.Status == common.StatusRunTerminating && !request.StopForce ||
		common.IsRunFinalStatus(run.Status) {
		err := fmt.Errorf("cannot stop run[%s] as run is already in status[%s]", runID, run.Status)
		logEntry.Errorln(err.Error())
		return err
	}

	wf, exist := wfMap[runID]
	if !exist {
		err := fmt.Errorf("run[%s]'s workflow ptr is lost", runID)
		logEntry.Errorln(err.Error())
		return err
	}
	if err := models.UpdateRunStatus(logEntry, runID, common.StatusRunTerminating); err != nil {
		err = fmt.Errorf("stop run[%s] failed updating db, %s", runID, err.Error())
	}
	wf.Stop(request.StopForce)
	logEntry.Debugf("stop run succeed. runID:%s", runID)
	return nil
}

func RetryRun(ctx *logger.RequestContext, runID string) error {
	ctx.Logging().Debugf("begin retry run. runID:%s\n", runID)
	// check run exist && check user access right
	run, err := GetRunByID(ctx.Logging(), ctx.UserName, runID)
	if err != nil {
		ctx.Logging().Errorf("retry run[%s] failed when getting run. error: %v\n", runID, err)
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

	// check run exist && check user access right
	run, err := GetRunByID(ctx.Logging(), ctx.UserName, id)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		err := fmt.Errorf("delete run[%s] failed when getting run, %s", id, err.Error())
		ctx.Logging().Errorf(err.Error())
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
		if runCachedList, _ := models.ListRun(ctx.Logging(), 0, 0, nil, nil, runCacheIDList, nil, nil, nil); len(runCachedList) > 0 {
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
	if run.GlobalFsID != "" {
		resourceHandler, err := pplcommon.NewResourceHandler(id, run.GlobalFsID, ctx.Logging())
		if err != nil {
			ctx.Logging().Errorf("delete run[%s] failed. InitTraceLoggerManager handler failed. err: %v", id, err.Error())
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
		if errors2.GetErrorCode(err) == errors2.ErrorRecordNotFound {
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

	wfs, err := schema.GetWorkflowSource([]byte(run.RunYaml))
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
			run.ID, run.WorkflowSource.DockerEnv, run.GlobalFsID, err.Error())
	}
	return nil
}

// handleImageAndStartWf patch run.WorkflowSource before invoke this func!
func handleImageAndStartWf(run models.Run, isResume bool) error {
	logEntry := logger.LoggerForRun(run.ID)
	logEntry.Debugf("start handleImageAndStartWf isResume:%t, run:%+v", isResume, run)
	trace_logger.Key(run.ID).Debugf("start handleImageAndStartWf isResume:%t, run:%+v", isResume, run)
	if !handler.NeedHandleImage(run.WorkflowSource) {
		// init workflow and start
		trace_logger.Key(run.ID).Infof("init workflow and start")
		wfPtr, err := newWorkflowByRun(run)
		if err != nil {
			logEntry.Errorf("newWorkflowByRun failed. err:%v\n", err)
			return updateRunStatusAndMsg(run.ID, common.StatusRunFailed, err.Error())
		}
		if !isResume {
			// start workflow with image url
			trace_logger.Key(run.ID).Infof("start workflow with image url")
			wfPtr.Start()
			logEntry.Debugf("workflow started, run:%+v", run)
		} else {
			// set runtime and restart
			trace_logger.Key(run.ID).Infof("resume workflow, set runtime and restart")
			wfPtr.Restart(run.Runtime, run.PostProcess)
			logEntry.Debugf("workflow restarted, run:%+v", run)
		}
		return models.UpdateRun(logEntry, run.ID,
			models.Run{DockerEnv: run.WorkflowSource.DockerEnv, Status: common.StatusRunPending})
	} else {
		return fmt.Errorf("image as tar file is not supported for now")
	}
}

func newWorkflowByRun(run models.Run) (*pipeline.Workflow, error) {
	extraInfo := map[string]string{
		pplcommon.WfExtraInfoKeySource:   run.Source,
		pplcommon.WfExtraInfoKeyFsID:     run.GlobalFsID,
		pplcommon.WfExtraInfoKeyUserName: run.UserName,
		pplcommon.WfExtraInfoKeyFsName:   run.GlobalFsName,
	}
	wfPtr, err := pipeline.NewWorkflow(run.WorkflowSource, run.ID, run.Parameters, extraInfo, workflowCallbacks)
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
