package flag

import (
	"reflect"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fuse"
)

func TestBasicFlags(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{
			name: "num",
			want: 7,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BasicFlags(); !reflect.DeepEqual(len(got), tt.want) {
				t.Errorf("BasicFlags() = %v, want %v", len(got), tt.want)
			}
		})
	}
}

func TestCacheFlags(t *testing.T) {
	type args struct {
		fuseConf *fuse.FuseConfig
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "cache num",
			args: args{
				fuseConf: fuse.FuseConf,
			},
			want: 12,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CacheFlags(tt.args.fuseConf); !reflect.DeepEqual(len(got), tt.want) {
				t.Errorf("CacheFlags() = %v, want %v", len(got), tt.want)
			}
		})
	}
}
