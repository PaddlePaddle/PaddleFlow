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
	"testing"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestPodSpecBuilder(t *testing.T) {
	schedulerName := "testSchedulerName"
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = schedulerName

	testCases := []struct {
		testName string
		jobID    string
		podSpec  *corev1.PodSpec
		task     schema.Member
		err      error
	}{
		{
			testName: "pod affinity is nil",
			podSpec:  &corev1.PodSpec{},
			jobID:    "test-job-1",
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
		},
		{
			testName: "pod has affinity",
			podSpec: &corev1.PodSpec{
				Affinity: &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{
											Key:      "kubernetes.io/hostname",
											Operator: corev1.NodeSelectorOpIn,
											Values:   []string{"instance1"},
										},
									},
								},
							},
						},
					},
				},
			},
			jobID: "test-job-2",
			task: schema.Member{
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					FileSystem: schema.FileSystem{
						ID:        "fs-root-test2",
						Name:      "test",
						Type:      "s3",
						MountPath: "/home/work/mnt",
					},
				},
			},
		},
		{
			testName: "create job with flavour",
			podSpec: &corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Command: []string{"sleep", "60"},
					},
				},
			},
			jobID: "test-job-3",
			task: schema.Member{
				Replicas: 1,
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					Image:     "test-image",
					Command:   "/bin/bash",
					Args:      []string{"-c", "sleep 3600"},
					FileSystem: schema.FileSystem{
						ID:        "fs-root-test2",
						Name:      "test",
						Type:      "s3",
						MountPath: "/home/work/mnt",
					},
					Flavour: schema.Flavour{
						Name: "",
						ResourceInfo: schema.ResourceInfo{
							CPU: "2",
							Mem: "4Gi",
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "1",
							},
						},
					},
				},
			},
		},
		{
			testName: "create job with limit flavour",
			podSpec: &corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Command: []string{"sleep", "60"},
					},
				},
			},
			jobID: "test-job-4",
			task: schema.Member{
				Replicas: 1,
				Conf: schema.Conf{
					Name:      "test-task-1",
					QueueName: "test-queue",
					Priority:  "NORMAL",
					Image:     "test-image",
					Command:   "/bin/bash",
					Args:      []string{"-c", "sleep 3600"},
					FileSystem: schema.FileSystem{
						ID:        "fs-root-test2",
						Name:      "test",
						Type:      "s3",
						MountPath: "/",
					},
					Flavour: schema.Flavour{
						Name: "",
						ResourceInfo: schema.ResourceInfo{
							CPU: "2",
							Mem: "4Gi",
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "1",
							},
						},
					},
					LimitFlavour: schema.Flavour{
						Name: "",
						ResourceInfo: schema.ResourceInfo{
							CPU: "10",
							Mem: "40Gi",
							ScalarResources: map[schema.ResourceName]string{
								"nvidia.com/gpu": "1",
							},
						},
					},
				},
			},
		},
	}

	driver.InitMockDB()
	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			err := storage.FsCache.Add(&model.FSCache{
				FsID:      testCase.task.Conf.FileSystem.ID,
				CacheDir:  "./xx",
				NodeName:  "instance1",
				ClusterID: "xxx",
			})
			assert.Equal(t, nil, err)
			NewPodSpecBuilder(testCase.podSpec, testCase.jobID).Build(testCase.task)
			t.Logf("build pod spec: %v", testCase.podSpec)
		})
	}
}

func TestPodTemplateSpecBuilder(t *testing.T) {
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
			testName: "pod resource limit  is none",
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
					LimitFlavour: schema.Flavour{
						Name: "none",
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
