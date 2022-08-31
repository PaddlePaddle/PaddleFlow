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

package framework

import (
	"context"

	"k8s.io/client-go/util/workqueue"

	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

// JobGetter return FrameworkJobInterface
type JobGetter interface {
	Job(framework string) JobInterface
}

type JobInterface interface {
	// Submit PaddleFlow Server operate on cluster
	Submit(ctx context.Context, job *api.PFJob) error

	Stop(ctx context.Context, job *api.PFJob) error

	Update(ctx context.Context, job *api.PFJob) error

	Delete(ctx context.Context, job *api.PFJob) error

	GetLog(ctx context.Context, jobLogRequest pfschema.JobLogRequest) (pfschema.JobLogInfo, error)

	// RegisterJobListener register jobListener to notify PaddleFlow Server when job is updated
	RegisterJobListener(ctx context.Context, jobQueue workqueue.RateLimitingInterface, any interface{}) error
	// RegisterTaskListener register taskListener to notify PaddleFlow Server when task is updated
	RegisterTaskListener(ctx context.Context, taskQueue workqueue.RateLimitingInterface, any interface{}) error
}

// QueueGetter return RuntimeQueueInterface
type QueueGetter interface {
	Queue(quotaType string) QueueInterface
}

// QueueInterface defines Queue operator methods
type QueueInterface interface {
	// PaddleFlow Server operate on Cluster
	Create(ctx context.Context, q *api.QueueInfo) error

	Delete(ctx context.Context, q *api.QueueInfo) error

	Update(ctx context.Context, q *api.QueueInfo) error
	// Cluster notify PaddleFlow Server when queue is updated
	QueueEvent(ctx context.Context, ch <-chan struct{}) error
}

type RuntimeClientInterface interface {
	Cluster() string

	ClusterID() string

	Get(namespace string, name string, fv pfschema.FrameworkVersion) (interface{}, error)

	Create(resource interface{}, fv pfschema.FrameworkVersion) error

	Delete(namespace string, name string, fv pfschema.FrameworkVersion) error

	Patch(namespace, name string, fv pfschema.FrameworkVersion, data []byte) error

	Update(resource interface{}, fv pfschema.FrameworkVersion) error

	// RegisterListeners register job/task listeners
	RegisterListeners(jobQueue, taskQueue workqueue.RateLimitingInterface) error

	StartLister(stopCh <-chan struct{})

	// ListNodeQuota resource api for cluster nodes
	ListNodeQuota(ctx context.Context) (pfschema.QuotaSummary, []pfschema.NodeQuotaInfo, error)
}
