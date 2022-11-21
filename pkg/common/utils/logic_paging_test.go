package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
)

func TestLogPage_Paging(t *testing.T) {
	type args struct {
		req string
		lg  LogPage
	}
	tests := []struct {
		name            string
		args            args
		hasNextPage     bool
		truncated       bool
		expectedContent string
	}{
		{
			name: "empty request",
			args: args{
				req: "",
				lg: LogPage{
					LogFilePosition: common.BeginFilePosition,
					LineLimit:       100,
					SizeLimit:       1000000000,
				},
			},
			hasNextPage:     false,
			truncated:       false,
			expectedContent: "",
		},
		{
			name: "BeginFilePosition with line limit",
			args: args{
				req: "1\n2\n3\n4\n5\n6\n7\n8\n9\n10",
				lg: LogPage{
					LogFilePosition: common.BeginFilePosition,
					LineLimit:       2,
					SizeLimit:       10000,
				},
			},
			hasNextPage:     true,
			truncated:       false,
			expectedContent: "1\n2\n",
		},
		{
			name: "BeginFilePosition with size limit",
			args: args{
				req: "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n",
				lg: LogPage{
					LogFilePosition: common.BeginFilePosition,
					LineLimit:       200,
					SizeLimit:       10,
				},
			},
			hasNextPage:     true,
			truncated:       false,
			expectedContent: "1\n2\n3\n4\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("case %#v", tt)
			res := tt.args.lg.Paging(tt.args.req, 0)
			assert.Equal(t, tt.expectedContent, res)
			assert.Equal(t, tt.hasNextPage, tt.args.lg.HasNextPage)
			assert.Equal(t, tt.truncated, tt.args.lg.Truncated)
		})
	}
}
