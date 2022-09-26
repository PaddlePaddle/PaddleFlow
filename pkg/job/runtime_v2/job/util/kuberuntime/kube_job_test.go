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
