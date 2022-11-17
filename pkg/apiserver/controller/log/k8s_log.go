package log

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
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
}

// GetMixedLogResponse return mixed logs
type GetMixedLogResponse struct {
	ResourceName string               `json:"name"`
	Resourcetype string               `json:"type"`
	TaskList     []schema.TaskLogInfo `json:"taskList"`
	Events       []string             `json:"eventList"`
}

// GetPFJobLogs return mixed log by events and logs
func GetPFJobLogs(ctx *logger.RequestContext, request GetMixedLogRequest) (schema.MixedLogResponse, error) {
	log.Debugf("Get k8s logs by request: %v", request)
	switch schema.Framework(request.Framework) {
	case schema.FrameworkStandalone, schema.FrameworkSpark, schema.FrameworkPaddle, schema.FrameworkTF,
		schema.FrameworkPytorch, schema.FrameworkMXNet, schema.FrameworkRay:
		// todo call runtimeSvc.GetJobLog()
		log.Errorf("todo")
	default:
		err := fmt.Errorf("job %s framework %s unsupport", request.Name, request.Framework)
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorln(err)
		return schema.MixedLogResponse{}, err
	}
	return schema.MixedLogResponse{}, nil
}

// GetKubernetesResourceLogs return mixed logs
func GetKubernetesResourceLogs(ctx *logger.RequestContext, request GetMixedLogRequest) (schema.MixedLogResponse, error) {
	log.Debugf("Get mixed logs by request: %v", request)
	clusterInfo, err := storage.Cluster.GetClusterByName(request.ClusterName)
	if err != nil {
		err = fmt.Errorf("get cluster %s failed. err:%v", request.ClusterName, err)
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorln(err)
		return schema.MixedLogResponse{}, err
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		err = fmt.Errorf("get cluster client failed. error:%s", err.Error())
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln(err)
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
		err = fmt.Errorf("get mixed logs failed. error: %v", err)
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorln(err)
		return schema.MixedLogResponse{}, err
	}
	response.Resourcetype = request.ResourceType
	response.ResourceName = fmt.Sprintf("%s/%s", request.Namespace, request.Name)
	return response, nil
}
