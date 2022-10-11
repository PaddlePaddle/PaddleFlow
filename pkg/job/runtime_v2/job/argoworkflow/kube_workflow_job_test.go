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

package argoworkflow

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var extArgoWorkflowYaml = `
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  annotations:
    ArgoWorkflow/GraphNodeInfo: '{"nodeInfoMap":{},"runnableNodeCount":1}'
  labels:
    workflowProcessStatus: Completed
    workflows.argoproj.io/completed: "true"
    workflows.argoproj.io/phase: Failed
  name: gra-6f09hm6s2vysg932
  namespace: cr-8j4pcualv0e0fzqm
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/cluster-role
            operator: In
            values:
            - slave
            - master-compute
  arguments: {}
  entrypoint: template-dag
  podGC:
    strategy: OnPodSuccess
  templates:
  - dag:
      failFast: false
      tasks:
      - arguments:
          parameters:
          - name: node_uid
            value: node1650263805756--version1650266057714
          - name: resource_requests_cpu
            value: "1"
          - name: resource_requests_memory
            value: 2048Mi
        name: task-node1650263805756
    inputs: {}
    metadata: {}
    name: template-dag
    outputs: {}
`

func TestWorkflowJob(t *testing.T) {
	config.GlobalServerConfig = &config.ServerConfig{}
	config.GlobalServerConfig.Job.SchedulerName = "testSchedulerName"
	defaultJobYamlPath := "../../../../../config/server/default/job/job_template.yaml"
	config.InitJobTemplate(defaultJobYamlPath)

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	kubeRuntimeClient := client.NewFakeKubeRuntimeClient(server)
	// mock db
	driver.InitMockDB()
	// create kubernetes resource with dynamic client
	tests := []struct {
		caseName  string
		jobObj    *api.PFJob
		expectErr error
	}{
		{
			caseName: "create workflow job",
			jobObj: &api.PFJob{
				ID:                "wf-test1",
				Namespace:         "default",
				JobType:           schema.TypeWorkflow,
				Conf:              schema.Conf{},
				ExtensionTemplate: []byte(extArgoWorkflowYaml),
			},
			expectErr: nil,
		},
	}

	argoWorkflowJob := New(kubeRuntimeClient)
	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			err := argoWorkflowJob.Submit(context.TODO(), test.jobObj)
			assert.Equal(t, test.expectErr, err)
			if err != nil {
				t.Logf("create job failed, err: %v", err)
			} else {
				jobObj, err := kubeRuntimeClient.Get(test.jobObj.Namespace, test.jobObj.ID, KubeArgoWorkflowFwVersion)
				if err != nil {
					t.Errorf(err.Error())
				} else {
					t.Logf("obj=%#v", jobObj)
				}
			}
		})
	}
}
