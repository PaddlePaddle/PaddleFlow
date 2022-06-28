/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package job

import (
	"encoding/json"
	"time"

	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

var (
	WSManager = WebsocketManager{
		Connections:   make(map[string]*Connection),
		BroadcastChan: make(chan GetJobResponse, 1000),
	}
	UpdateTime = time.Now()
)

type DistributedJobSpec struct {
	Framework schema.Framework `json:"framework,omitempty"`
	Members   []models.Member  `json:"members,omitempty"`
}

type ListJobRequest struct {
	Queue     string            `json:"queue,omitempty"`
	Status    string            `json:"status,omitempty"`
	Timestamp int64             `json:"timestamp,omitempty"`
	StartTime string            `json:"startTime,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Marker    string            `json:"marker"`
	MaxKeys   int               `json:"maxKeys"`
}

type ListJobResponse struct {
	common.MarkerInfo
	JobList []*GetJobResponse `json:"jobList"`
}

type GetJobResponse struct {
	CreateSingleJobRequest `json:",inline"`
	DistributedJobSpec     `json:",inline"`
	Status                 string                  `json:"status"`
	Message                string                  `json:"message"`
	AcceptTime             string                  `json:"acceptTime"`
	StartTime              string                  `json:"startTime"`
	FinishTime             string                  `json:"finishTime"`
	Runtime                *RuntimeInfo            `json:"runtime,omitempty"`
	DistributedRuntime     *DistributedRuntimeInfo `json:"distributedRuntime,omitempty"`
	WorkflowRuntime        *WorkflowRuntimeInfo    `json:"workflowRuntime,omitempty"`
	UpdateTime             time.Time               `json:"-"`
}

type RuntimeInfo struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	ID        string `json:"id,omitempty"`
	Status    string `json:"status,omitempty"`
	NodeName  string `json:"nodeName"`
}

type DistributedRuntimeInfo struct {
	Name      string        `json:"name,omitempty"`
	Namespace string        `json:"namespace,omitempty"`
	ID        string        `json:"id,omitempty"`
	Status    string        `json:"status,omitempty"`
	Runtimes  []RuntimeInfo `json:"runtimes,omitempty"`
}

type WorkflowRuntimeInfo struct {
	Name      string                   `json:"name,omitempty"`
	Namespace string                   `json:"namespace,omitempty"`
	ID        string                   `json:"id,omitempty"`
	Status    string                   `json:"status,omitempty"`
	Nodes     []DistributedRuntimeInfo `json:"nodes,omitempty"`
}

func ListJob(ctx *logger.RequestContext, request ListJobRequest) (*ListJobResponse, error) {
	ctx.Logging().Debugf("begin list job.")
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}

	var pk int64
	var err error
	if request.Marker != "" {
		pk, err = common.DecryptPk(request.Marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				request.Marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return nil, err
		}
	}

	timestampStr := ""
	if request.Timestamp != 0 {
		timestampStr = time.Unix(request.Timestamp, 0).Format(models.TimeFormat)
	}
	queueID := ""
	if request.Queue != "" {
		var queue models.Queue
		queue, err = models.GetQueueByName(request.Queue)
		if err != nil {
			ctx.Logging().Errorf("get queue by queueName[%s] failed, error:[%s]", request.Queue, err.Error())
			ctx.ErrorCode = common.QueueNameNotFound
			return nil, err
		}
		queueID = queue.ID
	}
	// model list
	jobList, err := models.ListJob(pk, request.MaxKeys, queueID, request.Status, request.StartTime, timestampStr, common.UserRoot, request.Labels)
	if err != nil {
		ctx.Logging().Errorf("models list job failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
		return nil, err
	}
	listJobResponse := ListJobResponse{JobList: []*GetJobResponse{}}

	// get next marker
	listJobResponse.IsTruncated = false
	if len(jobList) > 0 {
		job := jobList[len(jobList)-1]
		if !isLastJobPk(ctx, job.Pk) {
			nextMarker, err := common.EncryptPk(job.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					job.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return nil, err
			}
			listJobResponse.NextMarker = nextMarker
			listJobResponse.IsTruncated = true
		}
	}
	listJobResponse.MaxKeys = request.MaxKeys
	// append run briefs
	for _, j := range jobList {
		response, err := convertJobToResponse(j, false)
		if err != nil {
			ctx.Logging().Errorf("list job[%s] convert job to response failed, error:[%s]", j.ID, err.Error())
			return nil, err
		}
		listJobResponse.JobList = append(listJobResponse.JobList, &response)
	}
	return &listJobResponse, nil
}

func GetJob(ctx *logger.RequestContext, jobID string) (*GetJobResponse, error) {
	if err := CheckPermission(ctx); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorln(err.Error())
		return nil, err
	}

	job, err := models.GetJobByID(jobID)
	if err != nil {
		ctx.ErrorCode = common.JobNotFound
		ctx.Logging().Errorln(err.Error())
		return nil, common.NotFoundError(common.ResourceTypeJob, jobID)
	}
	response, err := convertJobToResponse(job, true)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func isLastJobPk(ctx *logger.RequestContext, pk int64) bool {
	lastJob, err := models.GetLastJob()
	if err != nil {
		ctx.Logging().Errorf("get last job failed. error:[%s]", err.Error())
	}
	if lastJob.Pk == pk {
		return true
	}
	return false
}

func convertJobToResponse(job models.Job, runtimeFlag bool) (GetJobResponse, error) {
	response := GetJobResponse{}
	b, err := json.Marshal(job)
	if err != nil {
		log.Errorf("model job[%s] convert to json string failed, error:[%s]", job.ID, err.Error())
		return response, err
	}
	err = json.Unmarshal(b, &response)
	if err != nil {
		log.Errorf("json string convert to job[%s] response failed, error:[%s]", job.ID, err.Error())
		return response, err
	}

	response.AcceptTime = job.CreatedAt.Format(models.TimeFormat)
	if job.ActivatedAt.Valid {
		response.StartTime = job.ActivatedAt.Time.Format(models.TimeFormat)
	}
	if schema.IsImmutableJobStatus(job.Status) {
		response.FinishTime = job.UpdatedAt.Format(models.TimeFormat)
	}
	response.ID = job.ID
	response.Name = job.Name
	response.SchedulingPolicy = SchedulingPolicy{
		Queue:    job.Config.GetQueueName(),
		Priority: job.Config.Priority,
	}
	if job.Config != nil {
		response.Labels = job.Config.Labels
		response.Annotations = job.Config.Annotations
	}
	// process runtime info && member
	switch job.Type {
	case string(schema.TypeSingle):
		if runtimeFlag && job.RuntimeInfo != nil {
			runtimes, err := getTaskRuntime(job.ID)
			if err != nil || len(runtimes) < 1 {
				return response, err
			}
			response.Runtime = &runtimes[0]
		}
		var jobSpec JobSpec
		if err := json.Unmarshal([]byte(job.ConfigJson), &jobSpec); err != nil {
			log.Errorf("parse job[%s] config failed, error:[%s]", job.ID, err.Error())
			return response, err
		}
		response.CreateSingleJobRequest.JobSpec = jobSpec
	case string(schema.TypeDistributed):
		if runtimeFlag && job.RuntimeInfo != nil {
			k8sMeta, err := parseK8sMeta(job.RuntimeInfo)
			if err != nil {
				log.Errorf("parse distributed job[%s] runtimeinfo job meta failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			statusByte, err := json.Marshal(job.RuntimeInfo.(map[string]interface{})["status"])
			if err != nil {
				log.Errorf("parse distributed job[%s] status failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			runtimes, err := getTaskRuntime(job.ID)
			if err != nil {
				return response, err
			}
			response.DistributedRuntime = &DistributedRuntimeInfo{
				ID:        string(k8sMeta.UID),
				Name:      k8sMeta.Name,
				Namespace: k8sMeta.Namespace,
				Status:    string(statusByte),
				Runtimes:  runtimes,
			}
		}
		members := make([]models.Member, 0)
		if job.Members != nil {
			if err := json.Unmarshal([]byte(job.MembersJson), &members); err != nil {
				log.Errorf("parse job[%s] member failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
		}
		response.DistributedJobSpec = DistributedJobSpec{
			Framework: job.Framework,
			Members:   members,
		}
	case string(schema.TypeWorkflow):
		if runtimeFlag && job.RuntimeInfo != nil {
			k8sMeta, err := parseK8sMeta(job.RuntimeInfo)
			if err != nil {
				log.Errorf("parse workflow job[%s] runtimeinfo job meta failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			statusByte, err := json.Marshal(job.RuntimeInfo.(map[string]interface{})["status"])
			if err != nil {
				log.Errorf("parse workflow job[%s] status failed, error:[%s]", job.ID, err.Error())
				return response, err
			}
			nodeRuntimes, err := getNodeRuntime(job.ID)
			if err != nil {
				return response, err
			}
			response.WorkflowRuntime = &WorkflowRuntimeInfo{
				Name:      k8sMeta.Name,
				Namespace: k8sMeta.Namespace,
				ID:        string(k8sMeta.UID),
				Status:    string(statusByte),
				Nodes:     nodeRuntimes,
			}
		}
	}
	return response, nil
}

func parseK8sMeta(runtimeInfo interface{}) (metav1.ObjectMeta, error) {
	var k8sMeta metav1.ObjectMeta
	metaData := runtimeInfo.(map[string]interface{})["metadata"]
	metaDataByte, err := json.Marshal(metaData)
	if err != nil {
		log.Errorf("k8s object meta convert to json string failed, error:[%s]", err.Error())
		return k8sMeta, err
	}
	err = json.Unmarshal(metaDataByte, &k8sMeta)
	if err != nil {
		log.Errorf("json string convert to k8s object meta failed, error:[%s]", err.Error())
		return k8sMeta, err
	}
	return k8sMeta, nil
}

func getTaskRuntime(jobID string) ([]RuntimeInfo, error) {
	tasks, err := models.ListByJobID(jobID)
	if err != nil {
		log.Errorf("list job[%s] tasks failed, error:[%s]", jobID, err.Error())
		return nil, err
	}
	runtimes := make([]RuntimeInfo, 0)
	for _, task := range tasks {
		runtime := RuntimeInfo{
			ID:        task.ID,
			Name:      task.Name,
			Namespace: task.Namespace,
			Status:    task.ExtRuntimeStatusJSON,
			NodeName:  task.NodeName,
		}
		runtimes = append(runtimes, runtime)
	}
	return runtimes, nil
}

func getNodeRuntime(jobID string) ([]DistributedRuntimeInfo, error) {
	nodeRuntimes := make([]DistributedRuntimeInfo, 0)
	nodeList, err := models.ListJobByParentID(jobID)
	if err != nil {
		log.Errorf("list job[%s] nodes failed, error:[%s]", jobID, err.Error())
		return nil, err
	}
	for _, node := range nodeList {
		k8sNodeMeta, err := parseK8sMeta(node.RuntimeInfo)
		if err != nil {
			log.Errorf("parse workflow node[%s] runtimeinfo job meta failed, error:[%s]", node.ID, err.Error())
		}
		nodeStatusByte, err := json.Marshal(node.RuntimeInfo.(map[string]interface{})["status"])
		if err != nil {
			log.Errorf("parse workflow node[%s] status failed, error:[%s]", node.ID, err.Error())
			return nil, err
		}
		taskRuntime, err := getTaskRuntime(node.ID)
		if err != nil {
			log.Errorf("get node[%s] task runtime failed, error:[%s]", node.ID, err.Error())
			return nil, err
		}
		nodeRuntime := DistributedRuntimeInfo{
			Name:      k8sNodeMeta.Name,
			Namespace: k8sNodeMeta.Namespace,
			ID:        string(k8sNodeMeta.UID),
			Status:    string(nodeStatusByte),
			Runtimes:  taskRuntime,
		}
		nodeRuntimes = append(nodeRuntimes, nodeRuntime)
	}
	return nodeRuntimes, nil
}
