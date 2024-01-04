/*
Copyright (c) 2024 PaddlePaddle Authors. All Rights Reserve.

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

package kuberuntime

import (
	"fmt"
	corev1 "k8s.io/api/core/v1"
	"testing"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestBuildPodTemplateSpec(t *testing.T) {
	schedulerName := "testSchedulerName"
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = schedulerName

	testCases := []struct {
		testName string
		jobID    string
		podSpec  *corev1.PodTemplateSpec
		task     schema.Member
		err      error
	}{
		{
			testName: "pod affinity is nil",
			podSpec:  &corev1.PodTemplateSpec{},
			task: schema.Member{
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					FileSystem: schema.FileSystem{
						ID:        "fs-root-test1",
						Name:      "test",
						Type:      "s3",
						MountPath: "/home/work/mnt",
					},
				},
			},
			err: nil,
		},
		{
			testName: "wrong flavour nil",
			podSpec:  &corev1.PodTemplateSpec{},
			task: schema.Member{
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					Flavour:   schema.Flavour{Name: "", ResourceInfo: schema.ResourceInfo{CPU: "4a", Mem: "4Gi"}},
				},
			},
			err: fmt.Errorf("quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'"),
		},
		{
			testName: "replicaSpec is nil",
			podSpec:  nil,
			task: schema.Member{
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					FileSystem: schema.FileSystem{
						ID:        "fs-root-test1",
						Name:      "test",
						Type:      "s3",
						MountPath: "/home/work/mnt",
					},
				},
			},
			err: fmt.Errorf("podTemplateSpec or task is nil"),
		},
	}

	driver.InitMockDB()
	for _, tt := range testCases {
		t.Run(tt.testName, func(t *testing.T) {
			NewPodTemplateSpecBuilder(tt.podSpec, tt.jobID).Build(tt.task)
			t.Logf("builder pod tempalte spec: %v", tt.podSpec)
		})
	}
}

func TestKubeflowReplicaSpec(t *testing.T) {
	schedulerName := "testSchedulerName"
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = schedulerName

	testCases := []struct {
		testName    string
		jobID       string
		replicaSpec *kubeflowv1.ReplicaSpec
		task        schema.Member
		err         error
	}{
		{
			testName:    "pod affinity is nil",
			replicaSpec: &kubeflowv1.ReplicaSpec{},
			task: schema.Member{
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					FileSystem: schema.FileSystem{
						ID:        "fs-root-test1",
						Name:      "test",
						Type:      "s3",
						MountPath: "/home/work/mnt",
					},
				},
			},
			err: nil,
		},
		{
			testName:    "replicaSpec is nil",
			replicaSpec: nil,
			task: schema.Member{
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					FileSystem: schema.FileSystem{
						ID:        "fs-root-test1",
						Name:      "test",
						Type:      "s3",
						MountPath: "/home/work/mnt",
					},
				},
			},
			err: fmt.Errorf("build kubeflow replica spec failed, err: replicaSpec or task is nil"),
		},
	}

	driver.InitMockDB()
	for _, tt := range testCases {
		t.Run(tt.testName, func(t *testing.T) {
			NewKubeflowJobBuilder(tt.jobID, nil, tt.replicaSpec).ReplicaSpec(tt.task)
			t.Logf("builder replica spec: %v", tt.replicaSpec)
		})
	}
}
