/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

package aitraining

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	v1 "github.com/PaddlePaddle/PaddleFlow/pkg/apis/aitj-operator/v1"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const jobExtensionTmp = `apiVersion: kongming.cce.baiudbce.com/v1
kind: AITrainingJob
metadata:
  name: job-horovod-test
  namespace: default
spec:
  # 任务结束时，pod的清理策略，All表示所有pod，none表示不清理
  cleanPodPolicy: All
  # 完成策略，All表示所有pod完成即任务完成，Any表示任何pod完成即任务完成
  completePolicy: Any
  # 失败策略，All表示所有pod失败即任务失败，Any表示任何pod完成即任务完成
  failPolicy: Any
  # 支持horovod与paddle框架
  frameworkType: paddle
  # 弹性选项，true表示开启弹性，false不开启，开启时需开启trainer容器的容错选项
  faultTolerant: true
  plugin:
    ssh:
      - ""
    discovery:
      - ""
  priority: normal
  replicaSpecs:
    trainer:
      completePolicy: None
      failPolicy: None
      # 容错配置，控制器将会以下面的配置作为容错判断条件进行容错
      faultTolerantPolicy:
        # 程序退出码
        - exitCodes: 129,10001,127,137,143,129
          restartPolicy: ExitCode
          restartScope: Pod
        # 集群异常事件
        - exceptionalEvent: "nodeNotReady,PodForceDeleted"
          restartPolicy: OnNodeFail
          restartScope: Pod
      # 开启弹性的最大副本数
      maxReplicas: 5
      # 开启弹性的最小副本数
      minReplicas: 1
      replicaType: worker
      replicas: 0
      restartLimit: 100
      restartPolicy: OnNodeFailWithExitCode
      restartTimeLimit: 60
      restartTimeout: 864000
      template:
        metadata:
          creationTimestamp: null
        spec:
          containers:
            - command:
                - /bin/bash
                - -c
                - /usr/sbin/sshd && sleep 40000
              image: registry.baidubce.com/cce-plugin-dev/horovod:v0.1.0
              imagePullPolicy: Always
              name: aitj-0
              securityContext:
                capabilities:
                  add:
                    - SYS_ADMIN
              volumeMounts:
                - mountPath: /dev/shm
                  name: cache-volume
          dnsPolicy: ClusterFirstWithHostNet
          terminationGracePeriodSeconds: 300
          volumes:
            - emptyDir:
                medium: Memory
                sizeLimit: 100Gi
              name: cache-volume
  schedulerName: volcano
`

func TestKubeAITrainingJob_Submit(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	defaultJobYamlPath := "../../../../../config/server/default/job/job_template.yaml"
	config.InitJobTemplate(defaultJobYamlPath)

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)
	// mock db
	driver.InitMockDB()

	testCases := []struct {
		name    string
		job     *api.PFJob
		wantErr error
		wantMsg string
	}{
		{
			name: "submit custom AITrainingJob successfully",
			job: &api.PFJob{
				ID: "test_custom_job_1",
				Conf: schema.Conf{
					QueueName: "test-queue",
					Priority:  schema.EnvJobNormalPriority,
				},
				Tasks: []schema.Member{
					{
						Replicas: 2,
						Role:     schema.RoleWorker,
					},
				},
				ExtensionTemplate: []byte(jobExtensionTmp),
			},
			wantErr: nil,
		},
		{
			name: "submit custom AITrainingJob failed",
			job: &api.PFJob{
				ID: "test_custom_job_2",
				Conf: schema.Conf{
					QueueName: "test-queue",
					Priority:  schema.EnvJobNormalPriority,
				},
				Tasks: []schema.Member{
					{
						Replicas: 1,
						Role:     schema.RolePServer,
					},
				},
				ExtensionTemplate: []byte(jobExtensionTmp),
			},
			wantErr: fmt.Errorf("role pserver is not supported"),
		},
		{
			name: "submit built-in AITrainingJob",
			job: &api.PFJob{
				ID:      "test_built_in_job_1",
				JobType: schema.TypeDistributed,
				JobMode: schema.EnvJobModeCollective,
				Conf: schema.Conf{
					QueueName: "test-queue",
					Priority:  "high",
				},
				Tasks: []schema.Member{
					{
						Replicas: 2,
						Role:     schema.RoleWorker,
						Conf: schema.Conf{
							Flavour: schema.Flavour{
								Name: "",
								ResourceInfo: schema.ResourceInfo{
									CPU: "2",
									Mem: "4Gi",
								},
							},
						},
					},
				},
			},
			wantErr: nil,
		},
	}

	aitjJob := New(kubeRuntimeClient)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := aitjJob.Submit(context.TODO(), tc.job)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}

func TestKubeAITrainingJob_JobStatus(t *testing.T) {
	testCases := []struct {
		name    string
		obj     interface{}
		wantErr error
	}{
		{
			name: "AI Training job is pending",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.AITrainingKindGroupVersion.Kind,
					"apiVersion": schema.AITrainingKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": v1.TrainingJobPhaseCreating,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "AI Training job is running",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.AITrainingKindGroupVersion.Kind,
					"apiVersion": schema.AITrainingKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": v1.TrainingJobPhaseRunning,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "AI Training job is terminating",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.AITrainingKindGroupVersion.Kind,
					"apiVersion": schema.AITrainingKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": v1.TrainingJobPhaseTerminating,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "AI Training job is success",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.AITrainingKindGroupVersion.Kind,
					"apiVersion": schema.AITrainingKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": v1.TrainingJobPhaseSucceeded,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "AI Training job is failed",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.AITrainingKindGroupVersion.Kind,
					"apiVersion": schema.AITrainingKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": v1.TrainingJobPhaseFailed,
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "AI Training job status is invalid",
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       schema.AITrainingKindGroupVersion.Kind,
					"apiVersion": schema.AITrainingKindGroupVersion.GroupVersion(),
					"status": map[string]interface{}{
						"phase": "xxx",
					},
				},
			},
			wantErr: fmt.Errorf("unexpected ai training job status: xxx"),
		},
	}

	aiTrainingJob := KubeAITrainingJob{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, err := aiTrainingJob.JobStatus(tc.obj)
			assert.Equal(t, tc.wantErr, err)
			t.Logf("job status: %v", status)
		})
	}
}
