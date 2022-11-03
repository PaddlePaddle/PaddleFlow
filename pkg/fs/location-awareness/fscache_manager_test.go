package location_awareness

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_diskUse(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "get ok",
			args: args{
				path: "./",
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return false
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := diskUse(tt.args.path)
			if !tt.wantErr(t, err, fmt.Sprintf("diskUse(%v)", tt.args.path)) {
				return
			}
			assert.Equalf(t, tt.want, got, "diskUse(%v)", tt.args.path)
		})
	}
}
