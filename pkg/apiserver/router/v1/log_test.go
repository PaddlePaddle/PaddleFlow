package v1

import (
	"fmt"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/log"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"
)

func TestLogRouter_GetJobLog(t *testing.T) {
	mockResponse := pfschema.MixedLogResponse{}
	patch5 := gomonkey.ApplyFuncReturn(log.GetKubernetesResourceLogs, mockResponse, nil)
	defer patch5.Reset()

	type Req struct {
		Name         string
		Namespace    string
		ClusterName  string
		ResourceType string
		Framework    string

		LineLimit    string
		SizeLimit    string
		readFromTail bool
	}
	router, _, baseURL := MockInitJob(t)
	type args struct {
		ctx    *logger.RequestContext
		req    *Req
		router *chi.Mux
	}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx:    &logger.RequestContext{UserName: MockNonRootUser2},
				req:    &Req{},
				router: router,
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "wrong sizelimit",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					Name:         MockJobID,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    "",
					LineLimit:    "200",
					SizeLimit:    "100K",
					readFromTail: true,
				},
				router: router,
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "normal",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					Name:         MockJobID,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    "",
					LineLimit:    "200",
					SizeLimit:    "100k",
					readFromTail: true,
				},
				router: router,
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "wrong line limit",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					Name:         MockJobID,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    "",
					LineLimit:    "abc",
					SizeLimit:    "100k",
					readFromTail: true,
				},
				router: router,
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "nil sizeLimit",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					Name:         MockJobID,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    "",
					LineLimit:    "100",
					SizeLimit:    "",
					readFromTail: true,
				},
				router: router,
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.args.req
			path := fmt.Sprintf("%s/log/job?name=%s&clusterName=%s&namespace=%s&readFromTail=%t&lineLimit=%s&sizeLimit=%s&type=%s&framework=%s",
				baseURL, r.Name, r.ClusterName, r.Namespace, r.readFromTail, r.LineLimit, r.SizeLimit, r.ResourceType, r.Framework)
			res, err := PerformGetRequest(tt.args.router, path)
			assert.NoError(t, err)
			t.Logf("get logs of Job %v", res)
			if tt.wantErr {
				assert.NotEqual(t, res.Code, 200)
			} else {
				assert.Equal(t, tt.responseCode, res.Code)
			}
		})
	}

}
