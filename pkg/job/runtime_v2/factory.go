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

package runtime_v2

import (
	"fmt"
	"sync"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type RuntimeService interface {
	Name() string
	// Init create client for runtime
	Init() error
	// Client return runtime service client
	Client() framework.RuntimeClientInterface
	// SyncController start sync controller
	SyncController(stopCh <-chan struct{})

	// SubmitJob submit job to cluster
	SubmitJob(job *api.PFJob) error
	// StopJob stop job on cluster
	StopJob(job *api.PFJob) error
	// UpdateJob update job on cluster
	UpdateJob(job *api.PFJob) error
	// DeleteJob delete job from cluster
	DeleteJob(job *api.PFJob) error
	// GetJobLog get log for job
	GetJobLog(jobLogRequest schema.JobLogRequest) (schema.JobLogInfo, error)

	// CreateQueue create a queue on cluster
	CreateQueue(q *api.QueueInfo) error
	// DeleteQueue delete a queue on cluster
	DeleteQueue(q *api.QueueInfo) error
	// UpdateQueue update a queue on cluster
	UpdateQueue(q *api.QueueInfo) error

	ListNodeQuota() (schema.QuotaSummary, []schema.NodeQuotaInfo, error)

	framework.JobGetter
	framework.QueueGetter
}

var PFRuntimeMap sync.Map

func newClusterConfig(cluster model.ClusterInfo) schema.Cluster {
	return schema.Cluster{
		Name: cluster.Name,
		ID:   cluster.ID,
		Type: cluster.ClusterType,
		ClientOpt: schema.ClientOptions{
			Master: cluster.Endpoint,
			Config: cluster.Credential,
			QPS:    1000,
			Burst:  1000,
		},
	}
}

func UpdateRuntime(clusterInfo model.ClusterInfo) error {
	_, err := CreateRuntime(clusterInfo)
	return err
}

func GetOrCreateRuntime(clusterInfo model.ClusterInfo) (RuntimeService, error) {
	if runtimeS, ok := PFRuntimeMap.Load(clusterInfo.ID); ok {
		return runtimeS.(RuntimeService), nil
	}

	return CreateRuntime(clusterInfo)
}

// CreateRuntime create RuntimeService and stored in Cache
func CreateRuntime(clusterInfo model.ClusterInfo) (RuntimeService, error) {
	var runtimeSvc RuntimeService
	var err error
	cluster := newClusterConfig(clusterInfo)
	switch cluster.Type {
	case schema.LocalType:
		//runtimeSvc = NewLocalRuntime(cluster)
	case schema.KubernetesType:
		runtimeSvc = NewKubeRuntime(cluster)
	default:
		return nil, fmt.Errorf("cluster type[%s] is not support", cluster.Type)
	}
	if err = runtimeSvc.Init(); err != nil {
		return nil, fmt.Errorf("init client for cluster[%s] faield, err: %v", clusterInfo.ID, err)
	}
	PFRuntimeMap.Store(clusterInfo.ID, runtimeSvc)
	return runtimeSvc, nil
}
