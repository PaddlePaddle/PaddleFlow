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

package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

func newTestAPIV1Client(t *testing.T, handler http.HandlerFunc) (*APIV1Client, *httptest.Server) {
	server := httptest.NewServer(handler)
	serverURL, err := url.Parse(server.URL)
	assert.NoError(t, err)
	port, err := strconv.Atoi(serverURL.Port())
	assert.NoError(t, err)
	client, err := NewForConfig(&core.PaddleFlowClientConfiguration{
		Host:                       serverURL.Hostname(),
		Port:                       port,
		ConnectionTimeoutInSeconds: 5,
	})
	assert.NoError(t, err)
	return client, server
}

func TestCreateBatch(t *testing.T) {
	client, server := newTestAPIV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/paddleflow/v1/job/batch", r.URL.Path)
		assert.Equal(t, "test-token", r.Header.Get(common.HeaderKeyAuthorization))

		var request CreateBatchJobRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		assert.Equal(t, "test-batch-job", request.Name)
		assert.Equal(t, "test-queue", request.SchedulingPolicy.Queue)
		assert.Equal(t, "batch/v1", request.ExtensionTemplate["apiVersion"])
		assert.Equal(t, "Job", request.ExtensionTemplate["kind"])

		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"id":"job-batch-0001"}`))
		assert.NoError(t, err)
	})
	defer server.Close()

	result, err := client.Job().CreateBatch(context.TODO(), &CreateBatchJobRequest{
		CommonJobInfo: CommonJobInfo{
			Name: "test-batch-job",
			SchedulingPolicy: SchedulingPolicy{
				Queue: "test-queue",
			},
		},
		ExtensionTemplate: map[string]interface{}{
			"apiVersion": "batch/v1",
			"kind":       "Job",
			"spec": map[string]interface{}{
				"completions": 2,
				"template": map[string]interface{}{
					"spec": map[string]interface{}{
						"restartPolicy": "Never",
						"containers": []interface{}{
							map[string]interface{}{"name": "main", "image": "busybox"},
						},
					},
				},
			},
		},
	}, "test-token")
	assert.NoError(t, err)
	assert.Equal(t, "job-batch-0001", result.ID)
}

func TestCreateBatchWithNilRequest(t *testing.T) {
	client, server := newTestAPIV1Client(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request %s", r.URL.Path)
	})
	defer server.Close()

	result, err := client.Job().CreateBatch(context.TODO(), nil, "test-token")
	assert.Error(t, err)
	assert.Nil(t, result)
}
