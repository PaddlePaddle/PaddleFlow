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
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var taskStatus = `{"phase":"Succeeded","conditions":[{"type":"Initialized","status":"True","lastProbeTime":null,
"lastTransitionTime":"2023-03-02T09:43:55Z","reason":"PodCompleted"},
{"type":"Ready","status":"False","lastProbeTime":null,"lastTransitionTime":"2023-03-02T10:43:57Z",
"reason":"PodCompleted"},{"type":"ContainersReady","status":"False","lastProbeTime":null,
"lastTransitionTime":"2023-03-02T10:43:57Z","reason":"PodCompleted"},
{"type":"PodScheduled","status":"True","lastProbeTime":null,"lastTransitionTime":"2023-03-02T09:43:55Z"}],
"hostIP":"127.0.0.1","podIP":"10.233.64.222","podIPs":[{"ip":"10.233.64.222"}],"startTime":"2023-03-02T09:43:55Z",
"containerStatuses":[{"name":"job-20220101xyz","state":{"terminated":{"exitCode":0,"reason":"Completed",
"startedAt":"2023-03-02T09:43:57Z","finishedAt":"2023-03-02T10:43:57Z",
"containerID":"docker://8517d2e225a5e580470d56c7e039208b538cb78b942cdabb028e235d1aee54b6"}},
"lastState":{},"ready":false,"restartCount":0,"image":"nginx:latest",
"imageID":"docker-pullable://nginx@sha256:1708fdec7d93bc9869d269fc20148b84110ecb75a2f4f7ad6bbb590cacbc729f",
"containerID":"docker://8517d2e225a5e580470d56c7e039208b538cb78b942cdabb028e235d1aee54b6","started":false}],
"qosClass":"Guaranteed"}`

func TestGenerateLogURL(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Log: config.JobLogConfig{
				ServiceHost: "127.0.0.1",
				ServicePort: "8080",
			},
		},
	}

	testCases := []struct {
		name        string
		task        model.JobTask
		containerID string
		expectURL   string
	}{
		{
			name: "get container id from JobTask.LogURL",
			task: model.JobTask{
				ID:                   "test-task-id-1",
				JobID:                "test-job-id",
				LogURL:               "34c608b1a2ffedab37a04481e153b9b273a31bfd4dd859b87d417b06c60723fe",
				ExtRuntimeStatusJSON: taskStatus,
			},
			containerID: "34c608b1a2ffedab37a04481e153b9b273a31bfd4dd859b87d417b06c60723fe",
			expectURL:   "http://127.0.0.1:8080/v1/containers/%s/log?jobID=test-job-id&token=%s&t=%d",
		},
		{
			name: "generate log url success",
			task: model.JobTask{
				ID:                   "test-task-id-2",
				JobID:                "test-job-id",
				ExtRuntimeStatusJSON: taskStatus,
			},
			containerID: "8517d2e225a5e580470d56c7e039208b538cb78b942cdabb028e235d1aee54b6",
			expectURL:   "http://127.0.0.1:8080/v1/containers/%s/log?jobID=test-job-id&token=%s&t=%d",
		},
	}

	driver.InitMockDB()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// init db
			err := storage.Job.UpdateTask(&tc.task)
			assert.Equal(t, nil, err)
			task, err := storage.Job.GetTaskByID(tc.task.ID)
			assert.Equal(t, nil, err)
			// generate log url
			url := GenerateLogURL(task)
			tokenStr, timeStamp := getLogToken(task.JobID, tc.containerID)
			token := md5.Sum([]byte(tokenStr))
			expectURL := fmt.Sprintf(tc.expectURL, tc.containerID, hex.EncodeToString(token[:]), timeStamp)
			assert.Equal(t, expectURL, url)
			t.Logf("log url %s", expectURL)

			// test multi update
			tc.task.LogURL = ""
			err = storage.Job.UpdateTask(&tc.task)
			assert.Equal(t, nil, err)
			task, err = storage.Job.GetTaskByID(tc.task.ID)
			assert.Equal(t, nil, err)
			t.Logf("after second update, task logURL: %v", task.LogURL)
		})
	}
}

func initMockJob(t *testing.T) {
	time1 := time.Now()
	testParentID := "test-parent-id"
	jobs := []model.Job{
		{
			ID:          "job-00001",
			Name:        "test-job-1",
			UserName:    "user1",
			Type:        string(schema.TypeSingle),
			RuntimeInfo: corev1.PodSpec{},
			QueueID:     "test-queue-1",
			Status:      schema.StatusJobRunning,
			Framework:   schema.FrameworkStandalone,
			Config: &schema.Conf{
				KindGroupVersion: schema.StandaloneKindGroupVersion,
			},
			ParentJob: testParentID,
			CreatedAt: time1,
			UpdatedAt: time1.Add(2 * time.Second),
			ActivatedAt: sql.NullTime{
				Time:  time1.Add(2 * time.Second),
				Valid: true,
			},
		},
		{
			ID:          "job-00002",
			Name:        "test-job-2",
			UserName:    "user1",
			Type:        string(schema.TypeDistributed),
			RuntimeInfo: corev1.PodSpec{},
			QueueID:     "test-queue-1",
			Status:      schema.StatusJobRunning,
			Framework:   schema.FrameworkPaddle,
			Config: &schema.Conf{
				KindGroupVersion: schema.PaddleKindGroupVersion,
			},
			ParentJob: testParentID,
			CreatedAt: time1.Add(2 * time.Second),
			UpdatedAt: time1.Add(4 * time.Second),
			ActivatedAt: sql.NullTime{
				Time:  time1.Add(4 * time.Second),
				Valid: true,
			},
		},
		{
			ID:          "job-00003",
			Name:        "test-job-3",
			UserName:    "user1",
			Type:        string(schema.TypeDistributed),
			RuntimeInfo: corev1.PodSpec{},
			QueueID:     "test-queue-1",
			Status:      schema.StatusJobRunning,
			Framework:   schema.FrameworkAITJ,
			Config: &schema.Conf{
				KindGroupVersion: schema.AITrainingKindGroupVersion,
			},
			CreatedAt: time1.Add(2 * time.Second),
			UpdatedAt: time1.Add(4 * time.Second),
			ActivatedAt: sql.NullTime{
				Time:  time1.Add(4 * time.Second),
				Valid: true,
			},
		},
		{
			ID:        "job-00004",
			Name:      "test-job-4",
			UserName:  "user1",
			Type:      string(schema.TypeDistributed),
			QueueID:   "test-queue-1",
			Status:    schema.StatusJobRunning,
			Framework: schema.FrameworkSpark,
			Config: &schema.Conf{
				KindGroupVersion: schema.SparkKindGroupVersion,
			},
			CreatedAt: time1.Add(2 * time.Second),
			UpdatedAt: time1.Add(4 * time.Second),
			ActivatedAt: sql.NullTime{
				Time:  time1.Add(4 * time.Second),
				Valid: true,
			},
		},
		{
			ID:       "job-00005",
			Name:     "test-job-5",
			UserName: "user1",
			Type:     string(schema.TypeWorkflow),
			QueueID:  "test-queue-1",
			Status:   schema.StatusJobRunning,
			Config: &schema.Conf{
				KindGroupVersion: schema.WorkflowKindGroupVersion,
			},
			CreatedAt: time1.Add(2 * time.Second),
			UpdatedAt: time1.Add(4 * time.Second),
			ActivatedAt: sql.NullTime{
				Time:  time1.Add(4 * time.Second),
				Valid: true,
			},
		},
		{
			ID:       "job-00006",
			Name:     "test-job-6",
			UserName: "user1",
			Type:     string(schema.TypeDistributed),
			QueueID:  "test-queue-1",
			Status:   schema.StatusJobRunning,
			Config: &schema.Conf{
				KindGroupVersion: schema.MPIKindGroupVersion,
			},
			CreatedAt: time1.Add(2 * time.Second),
			UpdatedAt: time1.Add(4 * time.Second),
			ActivatedAt: sql.NullTime{
				Time:  time1.Add(4 * time.Second),
				Valid: true,
			},
		},
	}

	for i := range jobs {
		err := storage.Job.CreateJob(&jobs[i])
		assert.Equal(t, nil, err)
	}
	// init job task
	jobTasks := []model.JobTask{
		{
			ID:        "job-00001-task",
			JobID:     "job-00001",
			Name:      "job-123456",
			Namespace: "default",
			NodeName:  "node-001",
			LogURL:    "container-123456",
		},
		{
			ID:    "job-00002-task",
			JobID: "job-00002",
			Name:  "job-100000",
			Annotations: map[string]string{
				"paddle-resource": "ps",
			},
			Namespace: "default",
			NodeName:  "node-001",
			LogURL:    "container-123456,11",
		},
		{
			ID:    "job-00002-worker-1",
			JobID: "job-00002",
			Name:  "job-100000-worker-1",
			Annotations: map[string]string{
				"paddle-resource": "worker",
			},
			Namespace: "default",
			NodeName:  "node-001",
			LogURL:    "container-123456,11",
		},
		{
			ID:        "job-00003-task",
			JobID:     "job-00003",
			Name:      "job-100001-trainer-3",
			Namespace: "default",
			NodeName:  "node-001",
		},
		{
			ID:        "job-00004-task",
			JobID:     "job-00004",
			Name:      "job-10004-driver-0",
			Namespace: "default",
			NodeName:  "node-001",
		},
		{
			ID:        "job-00006-task",
			JobID:     "job-00006",
			Name:      "job-10006-launcher",
			Namespace: "default",
			NodeName:  "node-001",
		},
	}
	for i := range jobTasks {
		err := storage.Job.UpdateTask(&jobTasks[i])
		assert.Equal(t, nil, err)
	}

	// init mock queue
	q1 := &model.Queue{
		Model: model.Model{
			ID: "test-queue-1",
		},
		Name:      "test-queue-1-name",
		ClusterId: "test-cluster-1",
		Status:    schema.StatusQueueOpen,
	}
	err := storage.Queue.CreateQueue(q1)
	assert.Equal(t, nil, err)
	// init mock cluster
	c1 := &model.ClusterInfo{
		Model: model.Model{
			ID: "test-cluster-1",
		},
	}
	err = storage.Cluster.CreateCluster(c1)
	assert.Equal(t, nil, err)
}

func TestListJob(t *testing.T) {
	timeStamp := time.Now().Unix()
	testCases := []struct {
		name           string
		request        ListJobRequest
		err            error
		wantedJobCount int
	}{
		{
			name: "list job with status filter",
			request: ListJobRequest{
				Status: string(schema.StatusJobRunning),
			},
			err:            nil,
			wantedJobCount: 6,
		},
		{
			name: "list job with timestamp",
			request: ListJobRequest{
				Timestamp: timeStamp,
			},
			err:            nil,
			wantedJobCount: 6,
		},
		{
			name: "list job with queue",
			request: ListJobRequest{
				Queue: "test-queue-1-name",
			},
			err:            nil,
			wantedJobCount: 6,
		},
	}

	driver.InitMockDB()
	initMockJob(t)

	ctx := &logger.RequestContext{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			listResp, err := ListJob(ctx, tc.request)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.wantedJobCount, len(listResp.JobList))
			t.Logf("list job: %v", listResp)
		})
	}
}

func TestGetJob(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{
		Job: config.JobConfig{
			Log: config.JobLogConfig{
				ServiceHost: "127.0.0.1",
				ServicePort: "8080",
			},
		},
	}

	testCases := []struct {
		name  string
		ctx   *logger.RequestContext
		jobID string
		err   error
	}{
		{
			name: "get single job",
			ctx: &logger.RequestContext{
				UserName: "root",
			},
			jobID: "job-00001",
			err:   nil,
		},
		{
			name: "get distributed paddle job",
			ctx: &logger.RequestContext{
				UserName: "root",
			},
			jobID: "job-00002",
			err:   nil,
		},
		{
			name: "get distributed aitj job",
			ctx: &logger.RequestContext{
				UserName: "root",
			},
			jobID: "job-00003",
			err:   nil,
		},
		{
			name: "get spark job",
			ctx: &logger.RequestContext{
				UserName: "root",
			},
			jobID: "job-00004",
			err:   nil,
		},
		{
			name: "get mpi job",
			ctx: &logger.RequestContext{
				UserName: "root",
			},
			jobID: "job-00006",
			err:   nil,
		},
		{
			name: "get workflow",
			ctx: &logger.RequestContext{
				UserName: "root",
			},
			jobID: "job-00005",
			err:   nil,
		},
	}

	driver.InitMockDB()
	initMockJob(t)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			job, err := GetJob(tc.ctx, tc.jobID)
			assert.Equal(t, tc.err, err)
			t.Logf("job info %v", job)
		})
	}
}
