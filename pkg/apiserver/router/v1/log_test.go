package v1

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-chi/chi"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	kuberuntime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/stretchr/testify/assert"
)

func TestLogRouter_GetJobLog(t *testing.T) {
	router, _, baseURL := MockInitJob(t)
	//initCluster(t)
	res, err := PerformPostRequest(router, baseURL+"/job/single", MockCreateJobRequest)
	assert.NoError(t, err)
	t.Logf("create Job %v", res)

	// init queue
	rts := &kuberuntime.KubeRuntime{}
	var p2 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "Init", func() error {
		return nil
	})
	defer p2.Reset()

	var p3 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "GetQueueUsedQuota", func(*api.QueueInfo) (*resources.Resource, error) {
		return resources.EmptyResource(), nil
	})
	defer p3.Reset()

	type Req struct {
		Name         string
		Namespace    string
		JobID        string
		ClusterName  string
		ResourceType string
		Framework    string

		LineLimit    string
		SizeLimit    string
		readFromTail bool
	}
	type args struct {
		ctx          *logger.RequestContext
		req          *Req
		router       *chi.Mux
		getMixedMock bool
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
				ctx:          &logger.RequestContext{UserName: MockNonRootUser2},
				req:          &Req{},
				router:       router,
				getMixedMock: true,
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
				router:       router,
				getMixedMock: true,
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "wrong sizelimit negative",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					Name:         MockJobID,
					Namespace:    "default",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    "",
					LineLimit:    "-200",
					SizeLimit:    "-100k",
					readFromTail: true,
				},
				router:       router,
				getMixedMock: true,
			},
			wantErr:      false,
			responseCode: 200,
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
				getMixedMock: true,
				router:       router,
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
				getMixedMock: true,
				router:       router,
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
				getMixedMock: true,
				router:       router,
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "jobID not found",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					JobID:        "wrong",
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    "",
					LineLimit:    "100",
					SizeLimit:    "",
					readFromTail: true,
				},
				getMixedMock: true,
				router:       router,
			},
			wantErr:      true,
			responseCode: 400,
		},
		{
			name: "jobID is not nil",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					JobID:        MockCreateJobRequest.ID,
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    string(pfschema.FrameworkStandalone),
					LineLimit:    "100",
					SizeLimit:    "",
					readFromTail: true,
				},
				getMixedMock: true,
				router:       router,
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "jobID is not nil but framework unsupport",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					JobID:        MockCreateJobRequest.ID,
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    "",
					LineLimit:    "100",
					SizeLimit:    "",
					readFromTail: true,
				},
				getMixedMock: false,
				router:       router,
			},
			wantErr:      true,
			responseCode: 200,
		},
		{
			name: "jobID is not nil but get mixed logs failed",
			args: args{
				ctx: &logger.RequestContext{UserName: mockUserName},
				req: &Req{
					JobID:        MockCreateJobRequest.ID,
					ClusterName:  MockClusterName,
					ResourceType: string(pfschema.TypePodJob),
					Framework:    string(pfschema.FrameworkStandalone),
					LineLimit:    "100",
					SizeLimit:    "",
					readFromTail: true,
				},
				getMixedMock: false,
				router:       router,
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.getMixedMock {
				var p1 = gomonkey.ApplyPrivateMethod(reflect.TypeOf(rts), "GetLog",
					func(jobLogRequest pfschema.JobLogRequest, mixedLogRequest pfschema.MixedLogRequest) (pfschema.JobLogInfo, error) {
						return pfschema.JobLogInfo{}, nil
					})
				defer p1.Reset()
			}
			r := tt.args.req
			path := fmt.Sprintf("%s/log/job?jobID=%s&name=%s&clusterName=%s&namespace=%s&readFromTail=%t&lineLimit=%s&sizeLimit=%s&type=%s&framework=%s",
				baseURL, r.JobID, r.Name, r.ClusterName, r.Namespace, r.readFromTail, r.LineLimit, r.SizeLimit, r.ResourceType, r.Framework)
			res, err = PerformGetRequest(tt.args.router, path)
			t.Logf("get logs of Job %v", res)
			if tt.wantErr {
				assert.NotEqual(t, 200, res.Code)
				t.Logf("err:%v", err)
			} else {
				assert.Equal(t, tt.responseCode, res.Code)
			}
		})
	}

}
