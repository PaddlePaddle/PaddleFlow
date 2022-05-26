/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package csidriver

import (
	"os"
	"path"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/client/pfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/mount"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/io"
	mountUtil "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/mount"
)

const (
	pfsFSID   = "pfs.fs.id"
	pfsServer = "pfs.server"
)

type nodeServer struct {
	nodeId string
	*csicommon.DefaultNodeServer
	credentialInfo credentials
}

func (ns *nodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	nscap := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	}
	return &csi.NodeGetCapabilitiesResponse{Capabilities: []*csi.NodeServiceCapability{nscap}}, nil
}

func (ns *nodeServer) NodePublishVolume(ctx context.Context,
	req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	log.Infof("Node publish volume request [%+v]", *req)
	targetPath := req.GetTargetPath()
	if exist, err := io.Exist(targetPath); err != nil {
		log.Errorf("check path[%s] exist failed: %v", targetPath, err)
		return &csi.NodePublishVolumeResponse{}, status.Error(codes.Internal, err.Error())
	} else if !exist {
		if err := os.MkdirAll(targetPath, 0750); err != nil {
			log.Errorf("create targetPath[%s] failed: %v", targetPath, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	volumeContext := req.GetVolumeContext()
	fsID := volumeContext[pfsFSID]
	server := volumeContext[pfsServer]
	volumeID := req.VolumeId

	mountInfo := pfs.GetMountInfo(fsID, server, req.GetReadonly())
	// root credentials for pfs-fuse
	mountInfo.UsernameRoot, mountInfo.PasswordRoot = ns.credentialInfo.usernameRoot, ns.credentialInfo.passwordRoot
	mountInfo.TargetPath = targetPath
	if err := mountVolume(volumeID, mountInfo, req.GetReadonly()); err != nil {
		log.Errorf("mount filesystem[%s] with server[%s] failed: %v", fsID, server, err)
		return &csi.NodePublishVolumeResponse{}, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	targetPath := req.GetTargetPath()
	volumeID := req.VolumeId
	mountInfo := pfs.MountInfo{
		UsernameRoot: ns.credentialInfo.usernameRoot,
		PasswordRoot: ns.credentialInfo.passwordRoot,
	}
	if err := mount.PodUnmount(volumeID, targetPath, mountInfo); err != nil {
		log.Errorf("[UMount]: volumeID[%s] and targetPath[%s] with err: %s", volumeID, targetPath, err.Error())
		return nil, err
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(ctx context.Context,
	req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	log.Debugf("start to stage volume[%s], do nothing...", req.GetVolumeId())
	return &csi.NodeStageVolumeResponse{}, nil

}

func (ns *nodeServer) NodeUnstageVolume(ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	log.Debugf("start to unstage volume[%s], do nothing...", req.GetVolumeId())
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context,
	req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeExpandVolume is not implemented")
}

func mountVolume(volumeID string, mountInfo pfs.MountInfo, readOnly bool) error {
	log.Infof("mountVolume mountInfo:%+v, readOnly:%t", mountInfo, readOnly)
	// business pods use a separate source path
	err := mount.PodMount(volumeID, mountInfo)
	if err != nil {
		log.Errorf("MountThroughPod err: %v", err)
		return err
	}

	bindSource := path.Join(mount.MountDir, mountInfo.FSID, "storage")
	log.Infof("bind info bindSource[%s] targetPath[%s]", bindSource, mountInfo.TargetPath)
	return bindMountVolume(bindSource, mountInfo.TargetPath, readOnly)
}

func bindMountVolume(sourcePath, mountPath string, readOnly bool) error {
	if err := os.MkdirAll(mountPath, 0750); err != nil {
		log.Errorf("mkdir volume bindMountPath[%s] failed: %v", mountPath, err)
		return err
	}
	if ok, _ := mountUtil.IsMountPoint(mountPath); !ok {
		output, err := mountUtil.ExecMountBind(sourcePath, mountPath, readOnly)
		if err != nil {
			log.Errorf("exec mount bind failed: %v, output[%s]", err, string(output))
			return err
		}
	}
	log.Infof("bindMountVolume from [%v] to [%v]", sourcePath, mountPath)
	return nil
}
