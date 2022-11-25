package log

import (
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

// GetMixedLogRequest can request job log or k8s pod/deploy events and log
type GetMixedLogRequest struct {
	Name         string
	Namespace    string
	ResourceType string
	Framework    string

	LineLimit      string
	SizeLimit      int64
	IsReadFromTail bool
	ClusterName    string
	ClusterInfo    model.ClusterInfo
}

// GetMixedLogResponse return mixed logs
type GetMixedLogResponse struct {
	ResourceName string               `json:"name"`
	Resourcetype string               `json:"type"`
	TaskList     []schema.TaskLogInfo `json:"taskList"`
	Events       []string             `json:"eventList"`
}

// GetPFJobLogs todo to be merged with GetLogs
// return mixed log by events and logs
func GetPFJobLogs(ctx *logger.RequestContext, request GetMixedLogRequest) (schema.JobLogInfo, error) {
	ctx.Logging().Debugf("Get k8s logs by request: %v", request)
	switch schema.Framework(request.Framework) {
	case schema.FrameworkStandalone, schema.FrameworkSpark, schema.FrameworkPaddle, schema.FrameworkTF,
		schema.FrameworkPytorch, schema.FrameworkMXNet, schema.FrameworkRay:
		// todo call runtimeSvc.GetJobLog()
		ctx.Logging().Warnf("todo")
	default:
		err := fmt.Errorf("job %s framework %s unsupport", request.Name, request.Framework)
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorln(err)
		return schema.JobLogInfo{}, err
	}
	return schema.JobLogInfo{}, nil
}

// GetLogs return mixed logs
func GetLogs(ctx *logger.RequestContext, request GetMixedLogRequest) (schema.JobLogInfo, error) {
	ctx.Logging().Debugf("Get mixed logs by request: %+v", request)
	runtimeSvc, err := runtime.GetOrCreateRuntime(request.ClusterInfo)
	if err != nil {
		err = fmt.Errorf("get cluster client failed. error:%s", err.Error())
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln(err)
		return schema.JobLogInfo{}, err
	}
	schemaReq := schema.MixedLogRequest{
		Name:           request.Name,
		Namespace:      request.Namespace,
		ResourceType:   request.ResourceType,
		Framework:      request.Framework,
		LineLimit:      request.LineLimit,
		SizeLimit:      request.SizeLimit,
		IsReadFromTail: request.IsReadFromTail,
	}
	response, err := runtimeSvc.GetLog(schema.JobLogRequest{}, schemaReq)
	if err != nil {
		err = fmt.Errorf("get mixed logs failed. error: %v", err)
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorln(err)
		return schema.JobLogInfo{}, err
	}
	response.Resourcetype = request.ResourceType
	response.ResourceName = fmt.Sprintf("%s/%s", request.Namespace, request.Name)
	return response, nil
}
