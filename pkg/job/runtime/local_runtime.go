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

package runtime

import (
	"fmt"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type LocRuntime struct {
	schema.Cluster
}

func NewLocalRuntime(cluster schema.Cluster) RuntimeService {
	return &LocRuntime{
		Cluster: cluster,
	}
}

func (l *LocRuntime) Init() error {
	return nil
}

func (l *LocRuntime) Name() string {
	return fmt.Sprintf("local runtime for cluster: %s", l.Cluster.Name)
}

func (l *LocRuntime) SubmitJob(job *api.PFJob) error {
	// TODO: submit local job
	return nil
}

func (l *LocRuntime) StopJob(job *api.PFJob) error {
	// TODO: stop local job
	return nil
}

func (l *LocRuntime) UpdateJob(job *api.PFJob) error {
	// TODO: update local job
	return nil
}

func (l *LocRuntime) DeleteJob(job *api.PFJob) error {
	// TODO: kill local job
	return nil
}

func (l *LocRuntime) SyncJob(stopCh <-chan struct{}) {
	// TODO: add local job sync
}

func (l *LocRuntime) GCJob(stopCh <-chan struct{}) {
	// TODO: add local job gc
}

func (l *LocRuntime) SyncQueue(stopCh <-chan struct{}) {
	// TODO: add local queue sync
}

func (l *LocRuntime) CreateQueue(q *model.Queue) error {
	// TODO: add create queue
	return nil
}

func (l *LocRuntime) DeleteQueue(q *model.Queue) error {
	// TODO: add delete queue
	return nil
}

func (l *LocRuntime) CloseQueue(q *model.Queue) error {
	// TODO: add close queue
	return nil
}

func (l *LocRuntime) UpdateQueue(q *model.Queue) error {
	// TODO: add update queue
	return nil
}

func (l *LocRuntime) ListNodeQuota() (schema.QuotaSummary, []schema.NodeQuotaInfo, error) {
	// TODO: add ListNodeQuota
	return schema.QuotaSummary{}, []schema.NodeQuotaInfo{}, nil
}

func (l *LocRuntime) GetJobLog(jobLogRequest schema.JobLogRequest) (schema.JobLogInfo, error) {
	// TODO
	return schema.JobLogInfo{}, nil
}
