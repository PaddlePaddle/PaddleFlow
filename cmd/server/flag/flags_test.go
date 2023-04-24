package flag

import (
	"reflect"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
)

func TestFilesystemFlags(t *testing.T) {
	type args struct {
		fsConf *config.FsServerConf
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "test1",
			args: args{
				fsConf: &config.FsServerConf{},
			},
			want: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FilesystemFlags(tt.args.fsConf); !reflect.DeepEqual(len(got), tt.want) {
				t.Errorf("FilesystemFlags() = %v, want %v", got, tt.want)
			}
		})
	}
}
