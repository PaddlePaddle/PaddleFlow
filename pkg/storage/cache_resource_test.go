package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

const (
	MockClusterName = "fakeClusterName"
	MockClusterID   = "fakeClusterID"
)

func TestResourceList(t *testing.T) {
	initMockCache()

	type args struct {
		ClusterNameList []string
		labels          string
		labelType       string
		pageNo          int
		pageSize        int
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "nil request",
			args:    args{},
			wantErr: false,
		},
		{
			name: "all clusters",
			args: args{
				ClusterNameList: []string{MockClusterName},
				labels:          "",
				labelType:       "",
				pageNo:          1,
				pageSize:        2,
			},
			wantErr: false,
		},
		{
			name: "filter by node label",
			args: args{
				ClusterNameList: []string{MockClusterName},
				labels:          "a=b",
				labelType:       model.ObjectTypeNode,
				pageNo:          1,
				pageSize:        1,
			},
			wantErr: false,
		},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("no.%d name=%s args=[%#v], wantError=%v", index, tt.name, tt.args, tt.wantErr)
			response, err := ResourceCache.ListResouces(tt.args.ClusterNameList,
				tt.args.labels,
				tt.args.labelType,
				tt.args.pageNo,
				tt.args.pageSize)
			t.Logf("case[%s] list cluster quota by labels, response=%+v", tt.name, response)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				t.Logf("response: %+v", response)
			}
		})
	}

}
