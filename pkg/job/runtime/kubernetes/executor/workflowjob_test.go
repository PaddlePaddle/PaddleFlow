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

package executor

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
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

	var server = httptest.NewServer(k8s.DiscoveryHandlerFunc)
	defer server.Close()
	dynamicClient := newFakeDynamicClient(server)
	// mock db
	driver.InitMockDB()
	// create kubernetes resource with dynamic client
	tests := []struct {
		caseName string
		jobObj   *api.PFJob
		wantErr  bool
		wantMsg  string
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
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.caseName, func(t *testing.T) {
			p, err := NewKubeJob(test.jobObj, dynamicClient)
			assert.Nil(t, err)
			assert.NotNil(t, p)
			t.Logf("NewKubeJob: %+v", p)

			jobID, err := p.CreateJob()
			if test.wantErr && assert.Error(t, err) {
				assert.Equal(t, test.wantMsg, err.Error())
			} else if !test.wantErr && assert.NotEmpty(t, jobID) {
				_, err = Get(test.jobObj.Namespace, test.jobObj.ID, k8s.ArgoWorkflowGVK, dynamicClient)
				if !assert.NoError(t, err) {
					t.Errorf(err.Error())
					return
				}
			} else {
				t.Errorf("error case, %v", err)
			}
		})
	}
}
