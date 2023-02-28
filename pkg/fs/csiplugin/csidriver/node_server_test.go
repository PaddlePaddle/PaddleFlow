package csidriver

import (
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/mount"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func Test_mountVolume(t *testing.T) {
	type args struct {
		volumeID  string
		mountInfo mount.Info
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "mount volume",
			args: args{
				volumeID: "test",
				mountInfo: mount.Info{
					TargetPath: "/tmp/test",
					FS: model.FileSystem{
						Type:          common.CFSType,
						SubPath:       "/test",
						ServerAddress: "127.0.0.1:8999",
					},
					SourcePath: "/tmp/test",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := mountVolume(tt.args.volumeID, tt.args.mountInfo); (err != nil) != tt.wantErr {
				t.Errorf("mountVolume() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
