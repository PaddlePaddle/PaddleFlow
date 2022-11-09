package log

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	log "github.com/sirupsen/logrus"
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
	ClusterInfo    model.ClusterInfo
}

// GetMixedLogResponse return mixed logs
type GetMixedLogResponse struct {
	ResourceName string               `json:"name"`
	Resourcetype string               `json:"type"`
	TaskList     []schema.TaskLogInfo `json:"taskList"`
	Events       []string             `json:"eventList"`
}

// GetK8sLog return mixed log by events and logs
func GetK8sLog(ctx *logger.RequestContext, request GetMixedLogRequest) (schema.MixedLogResponse, error) {
	log.Debugf("Get k8s logs by request: %v", request)
	if request.Framework != "" {
		log.Debugf("Get %s job logs by request: %v", request.Framework, request)
		// todo call runtimeSvc.GetJobLog()
		ctx.ErrorCode = common.InvalidArguments
		//common.RenderErrWithMessage(writer, ctx.RequestID, ctx.ErrorCode, err.Error())
		ctx.Logging().Errorf("job framework %s unsupport show log %s.", request.Framework, request.Name)
		return schema.MixedLogResponse{}, nil
	}
	return GetMixedLogs(ctx, request)
}

// GetMixedLogs return mixed logs
func GetMixedLogs(ctx *logger.RequestContext, request GetMixedLogRequest) (schema.MixedLogResponse, error) {
	log.Debugf("Get mixed logs by request: %v", request)
	runtimeSvc, err := runtime.GetOrCreateRuntime(request.ClusterInfo)
	if err != nil {
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorf("get cluster client failed. error:%s.", err.Error())
		return schema.MixedLogResponse{}, err
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
	response, err := runtimeSvc.GetMixedLog(schemaReq)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorf("get mixed logs failed. error:%s.", err.Error())
		return schema.MixedLogResponse{}, err
	}
	return response, nil
}
