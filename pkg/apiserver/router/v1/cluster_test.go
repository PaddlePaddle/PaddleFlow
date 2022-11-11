package v1

import (
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/cluster"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestListClusterQuotaV2(t *testing.T) {
	router, routerNonRoot, baseURL := MockInitJob(t)
	driver.InitCache("debug")
	type args struct {
		ctx    *logger.RequestContext
		req    interface{}
		router *chi.Mux
	}
	ctx := &logger.RequestContext{UserName: "testusername"}
	tests := []struct {
		name         string
		args         args
		wantErr      bool
		responseCode int
	}{
		{
			name: "empty request",
			args: args{
				ctx:    ctx,
				router: router,
				req:    &cluster.ListClusterResourcesRequest{},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "wrong request",
			args: args{
				ctx:    ctx,
				router: router,
				req:    "abc",
			},
			wantErr:      false,
			responseCode: 400,
		},
		{
			name: "routerNonRoot",
			args: args{
				ctx:    ctx,
				router: routerNonRoot,
				req: &cluster.ListClusterResourcesRequest{
					PageSize: 1,
					PageNo:   1,
				},
			},
			wantErr:      false,
			responseCode: 403,
		},
		{
			name: "PageSize and PageNo",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize: 1,
					PageNo:   1,
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "normal",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize:        1,
					PageNo:          1,
					ClusterNameList: []string{},
					Labels:          "a=b",
					LabelType:       model.ObjectTypeNode,
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "normal",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize:        1,
					PageNo:          1,
					ClusterNameList: []string{},
					Labels:          "a=b",
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
		{
			name: "normal",
			args: args{
				ctx:    ctx,
				router: router,
				req: &cluster.ListClusterResourcesRequest{
					PageSize:        1,
					PageNo:          1,
					ClusterNameList: []string{},
					Labels:          "a=b",
					LabelType:       model.ObjectTypePod,
				},
			},
			wantErr:      false,
			responseCode: 200,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := PerformPostRequest(tt.args.router, baseURL+"/cluster/resource", tt.args.req)
			assert.NoError(t, err)
			t.Logf("list Job %v", res)
			if tt.wantErr {
				assert.NotEqual(t, res.Code, 200)
			} else {
				assert.Equal(t, tt.responseCode, res.Code)
			}
		})
	}
}
