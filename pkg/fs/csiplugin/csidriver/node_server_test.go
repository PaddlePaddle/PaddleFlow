package csidriver

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"golang.org/x/net/context"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/mount"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
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

func Test_nodeServer_NodePublishVolume(t *testing.T) {
	type fields struct {
		nodeId            string
		DefaultNodeServer *csicommon.DefaultNodeServer
	}
	type args struct {
		ctx context.Context
		req *csi.NodePublishVolumeRequest
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *csi.NodePublishVolumeResponse
		wantErr bool
	}{
		{
			name: "no server address",
			fields: fields{
				nodeId:            "test",
				DefaultNodeServer: &csicommon.DefaultNodeServer{},
			},
			args: args{
				ctx: context.Background(),
				req: &csi.NodePublishVolumeRequest{
					TargetPath: "./testdir",
					VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Mount{},
					},
				},
			},
			wantErr: true,
		},
	}
	// patch utils.GetK8sClient()
	p := gomonkey.ApplyFunc(utils.GetK8sClient, func() (utils.Client, error) {
		return nil, nil
	})
	defer p.Reset()

	// patch  mount.ConstructMountInfo
	p = gomonkey.ApplyFunc(mount.ConstructMountInfo, func(serverAddress, fsInfoBase64, fsCacheBase64, targetPath string, k8sClient utils.Client, readOnly bool) (mount.Info, error) {
		return mount.Info{}, fmt.Errorf("construct mount info error")
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := &nodeServer{
				nodeId:            tt.fields.nodeId,
				DefaultNodeServer: tt.fields.DefaultNodeServer,
			}
			got, err := ns.NodePublishVolume(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("NodePublishVolume() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NodePublishVolume() got = %v, want %v", got, tt.want)
			}
		})
	}
}
