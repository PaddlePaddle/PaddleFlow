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
	"fmt"
	"net/http/httptest"
	"testing"

	kubeflowv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
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
			err := storage.FsCache.Add(&model.FSCache{
				FsID:      testCase.task.Conf.FileSystem.ID,
				CacheDir:  "./xx",
				NodeName:  "instance1",
				ClusterID: "xxx",
			})
			assert.Equal(t, nil, err)
			err = BuildPodSpec(testCase.podSpec, testCase.task)
			assert.Equal(t, testCase.err, err)
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
			err := KubeflowReplicaSpec(tt.replicaSpec, tt.jobID, &tt.task)
			assert.Equal(t, tt.err, err)
		})
	}
}

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
			err := BuildPodTemplateSpec(tt.podSpec, tt.jobID, &tt.task)
			assert.Equal(t, tt.err, err)
		})
	}
}

func TestGenerateResourceRequirements(t *testing.T) {
	schedulerName := "testSchedulerName"
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = schedulerName
	type args struct {
		flavour      schema.Flavour
		limitFlavour schema.Flavour
	}
	testCases := []struct {
		testName string
		args     args
		err      error
	}{
		{
			testName: "success",
			args: args{
				flavour: schema.Flavour{
					ResourceInfo: schema.ResourceInfo{
						CPU: "1",
						Mem: "1",
					},
					Name: "flavour1",
				},
				limitFlavour: schema.Flavour{
					ResourceInfo: schema.ResourceInfo{
						CPU: "2",
						Mem: "2",
					},
					Name: "flavour1",
				},
			},
			err: nil,
		},
		{
			testName: "success2",
			args: args{
				flavour: schema.Flavour{
					ResourceInfo: schema.ResourceInfo{
						CPU: "1",
						Mem: "1",
					},
					Name: "flavour1",
				},
			},
			err: nil,
		},
		{
			testName: "negative resources not permitted",
			args: args{
				flavour: schema.Flavour{
					ResourceInfo: schema.ResourceInfo{
						CPU: "-1",
						Mem: "1",
					},
					Name: "flavour1",
				},
				limitFlavour: schema.Flavour{
					ResourceInfo: schema.ResourceInfo{
						CPU: "1",
						Mem: "1",
					},
					Name: "flavour1",
				},
			},
			err: fmt.Errorf("negative resources not permitted: map[cpu:-1 memory:1]"),
		},
		{
			testName: "limitFlavour negative resources not permitted",
			args: args{
				flavour: schema.Flavour{
					ResourceInfo: schema.ResourceInfo{
						CPU: "1",
						Mem: "1",
					},
					Name: "flavour1",
				},
				limitFlavour: schema.Flavour{
					ResourceInfo: schema.ResourceInfo{
						CPU: "-1",
						Mem: "1",
					},
					Name: "flavour1",
				},
			},
			err: fmt.Errorf("negative resources not permitted: map[cpu:-1 memory:1]"),
		},
	}

	driver.InitMockDB()
	for _, tt := range testCases {
		t.Run(tt.testName, func(t *testing.T) {
			res, err := GenerateResourceRequirements(tt.args.flavour, tt.args.limitFlavour)
			assert.Equal(t, tt.err, err)
			if err == nil {
				t.Logf("res is %#v", res)
			}
		})
	}

}

func TestUpdateKubeJob(t *testing.T) {
	mockJobID := "test-job-id"
	pgKindGroupVersion := schema.NewKindGroupVersion(k8s.PodGroupGVK.Kind,
		k8s.PodGroupGVK.Group, k8s.PodGroupGVK.Version)
	testCases := []struct {
		name     string
		job      *api.PFJob
		jobObj   interface{}
		dbJob    *model.Job
		podgroup interface{}
		kv       schema.KindGroupVersion
		wantErr  error
	}{
		{
			name: "update single job priority",
			dbJob: &model.Job{
				ID: mockJobID,
				RuntimeInfo: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							schedulingv1beta1.KubeGroupNameAnnotationKey: "podgroup-xxxx",
						},
					},
				},
			},
			job: &api.PFJob{
				ID:                mockJobID,
				Namespace:         "default",
				Framework:         schema.FrameworkStandalone,
				PriorityClassName: schema.PriorityClassHigh,
			},
			podgroup: &schedulingv1beta1.PodGroup{
				TypeMeta: metav1.TypeMeta{
					Kind:       pgKindGroupVersion.Kind,
					APIVersion: pgKindGroupVersion.GroupVersion(),
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "podgroup-xxxx",
					Namespace: "default",
				},
				Status: schedulingv1beta1.PodGroupStatus{
					Phase: schedulingv1beta1.PodGroupInqueue,
				},
			},
			kv: schema.StandaloneKindGroupVersion,
		},
		{
			name: "update spark job priority",
			dbJob: &model.Job{
				ID:        "test-spark-job",
				Framework: schema.FrameworkSpark,
			},
			job: &api.PFJob{
				ID:                "test-spark-job",
				Namespace:         "default",
				Framework:         schema.FrameworkSpark,
				PriorityClassName: schema.PriorityClassHigh,
			},
			podgroup: &schedulingv1beta1.PodGroup{
				TypeMeta: metav1.TypeMeta{
					Kind:       pgKindGroupVersion.Kind,
					APIVersion: pgKindGroupVersion.GroupVersion(),
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-test-spark-job-pg",
					Namespace: "default",
				},
				Status: schedulingv1beta1.PodGroupStatus{
					Phase: schedulingv1beta1.PodGroupInqueue,
				},
			},
			kv: schema.SparkKindGroupVersion,
		},
		{
			name: "update paddle job priority",
			dbJob: &model.Job{
				ID:        "test-paddle-job",
				Framework: schema.FrameworkPaddle,
			},
			job: &api.PFJob{
				ID:                "test-paddle-job",
				Namespace:         "default",
				Framework:         schema.FrameworkPaddle,
				PriorityClassName: schema.PriorityClassHigh,
			},
			podgroup: &schedulingv1beta1.PodGroup{
				TypeMeta: metav1.TypeMeta{
					Kind:       pgKindGroupVersion.Kind,
					APIVersion: pgKindGroupVersion.GroupVersion(),
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-paddle-job",
					Namespace: "default",
				},
				Status: schedulingv1beta1.PodGroupStatus{
					Phase: schedulingv1beta1.PodGroupInqueue,
				},
			},
			kv: schema.PaddleKindGroupVersion,
		},
		//{
		//	name: "update paddle job annotations failed",
		//	dbJob: &model.Job{
		//		ID:        "test-paddle-job",
		//		Framework: schema.FrameworkPaddle,
		//	},
		//	job: &api.PFJob{
		//		ID:        "test-paddle-job",
		//		Namespace: "default",
		//		Framework: schema.FrameworkPaddle,
		//		Annotations: map[string]string{
		//			"ab": "b1",
		//		},
		//	},
		//	jobObj: &paddlejobv1.PaddleJob{
		//		TypeMeta: metav1.TypeMeta{
		//			Kind:       schema.PaddleKindGroupVersion.Kind,
		//			APIVersion: schema.PaddleKindGroupVersion.GroupVersion(),
		//		},
		//		ObjectMeta: metav1.ObjectMeta{
		//			Name:      "test-paddle-job",
		//			Namespace: "default",
		//			Annotations: map[string]string{
		//				"kk": "ka",
		//			},
		//		},
		//	},
		//	kv: schema.PaddleKindGroupVersion,
		//},
	}

	driver.InitMockDB()
	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeClient := client.NewFakeKubeRuntimeClient(server)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := storage.Job.CreateJob(tc.dbJob)
			assert.Equal(t, nil, err)
			if tc.podgroup != nil {
				err = kubeClient.Create(tc.podgroup, pgKindGroupVersion)
				assert.Equal(t, nil, err)
			}
			if tc.jobObj != nil {
				err = kubeClient.Create(tc.jobObj, tc.kv)
				assert.Equal(t, nil, err)
			}
			err = UpdateKubeJob(tc.job, kubeClient, tc.kv)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
