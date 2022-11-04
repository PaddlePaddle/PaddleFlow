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

package kuberuntime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestBuildKubeMetadata(t *testing.T) {
	metadata := &metav1.ObjectMeta{}
	pfjob := &api.PFJob{
		ID:        "test-job-id",
		Name:      "test-job-name",
		Namespace: "default",
		Annotations: map[string]string{
			"test-anno1": "anno-value",
			"test-anno2": "anno-value2",
		},
		Labels: map[string]string{
			"label1": "value1",
		},
		QueueName: "default-queue",
	}

	BuildJobMetadata(metadata, pfjob)

	assert.Equal(t, pfjob.ID, metadata.Name)
	assert.Equal(t, pfjob.Namespace, metadata.Namespace)
	assert.Equal(t, len(pfjob.Labels)+3, len(metadata.Labels))
}

func TestBuildSchedulingPolicy(t *testing.T) {
	schedulerName := "testSchedulerName"
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = schedulerName

	// test BuildScheduling Policy
	pod := &corev1.Pod{}
	priorityClass := "NORMAL"
	err := buildPriorityAndScheduler(&pod.Spec, priorityClass)
	assert.Equal(t, nil, err)
	assert.Equal(t, schema.PriorityClassNormal, pod.Spec.PriorityClassName)
	assert.Equal(t, schedulerName, pod.Spec.SchedulerName)
}

func TestBuildPodSpec(t *testing.T) {
	schedulerName := "testSchedulerName"
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = schedulerName

	testCases := []struct {
		testName string
		podSpec  *corev1.PodSpec
		task     schema.Member
		err      error
	}{
		{
			testName: "pod affinity is nil",
			podSpec:  &corev1.PodSpec{},
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
	}

	driver.InitMockDB()
	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			storage.FsCache.Add(&model.FSCache{
				FsID:      testCase.task.Conf.FileSystem.ID,
				CacheDir:  "./xx",
				NodeName:  "instance1",
				ClusterID: "xxx",
			})
			err := BuildPodSpec(testCase.podSpec, testCase.task)
			assert.Equal(t, testCase.err, err)
		})
	}
}
