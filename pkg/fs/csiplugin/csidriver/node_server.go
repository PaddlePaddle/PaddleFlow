/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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
	"fmt"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/mount"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
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
	if exist, err := utils.Exist(targetPath); err != nil {
		log.Errorf("check path[%s] exist failed: %v", targetPath, err)
		return &csi.NodePublishVolumeResponse{}, status.Error(codes.Internal, err.Error())
	} else if !exist {
		if err := os.MkdirAll(targetPath, 0750); err != nil {
			log.Errorf("create targetPath[%s] failed: %v", targetPath, err)
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	volumeID := req.VolumeId
	volumeContext := req.GetVolumeContext()

	csiconfig.UserNameRoot = ns.credentialInfo.usernameRoot
	csiconfig.PassWordRoot = ns.credentialInfo.passwordRoot
	// assume that the paddleflow server address will not be changed
	csiconfig.PaddleFlowServer = volumeContext[schema.PFSServer]
	csiconfig.ClusterID = volumeContext[schema.PFSClusterID]

	mountInfo, err := mount.ProcessMountInfo(volumeContext[schema.PFSInfo], volumeContext[schema.PFSCache],
		targetPath, req.GetReadonly())
	if err != nil {
		log.Errorf("ProcessMountInfo err: %v", err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	log.Infof("Node publish mountInfo [%+v]", mountInfo)

	k8sClient, err := utils.GetK8sClient()
	if err != nil {
		log.Errorf("get k8s client failed: %v", err)
		return nil, err
	}
	mountInfo.K8sClient = k8sClient

	if err = mountVolume(volumeID, mountInfo, req.GetReadonly()); err != nil {
		log.Errorf("mount filesystem[%s] failed: %v", volumeContext[schema.PFSID], err)
		return &csi.NodePublishVolumeResponse{}, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	mountInfo := mount.Info{
		TargetPath: req.GetTargetPath(),
	}
	if err := mount.PodUnmount(req.VolumeId, mountInfo); err != nil {
		log.Errorf("[UMount]: volumeID[%s] and targetPath[%s] with err: %s", req.VolumeId, mountInfo.TargetPath, err.Error())
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

func mountVolume(volumeID string, mountInfo mount.Info, readOnly bool) error {
	log.Infof("mountVolume: indepedentMp:%t, readOnly:%t", mountInfo.FS.IndependentMountProcess, readOnly)
	if !mountInfo.FS.IndependentMountProcess && mountInfo.FS.Type != common.GlusterFSType {
		// business pods use a separate source path
		if err := mount.PFSMount(volumeID, mountInfo); err != nil {
			log.Errorf("MountThroughPod err: %v", err)
			return err
		}
		if err := bindMountVolume(schema.GetBindSource(mountInfo.FS.ID), mountInfo.TargetPath, readOnly); err != nil {
			log.Errorf("mountVolume[%s] of fs[%s] failed when bindMountVolume, err: %v", volumeID, mountInfo.FS.ID, err)
			return err
		}
	} else {
		cmdName, args := mountInfo.MountCmd()
		log.Debugf("independent mount cmd: %s and args: %v", cmdName, args)
		output, err := utils.ExecCmdWithTimeout(cmdName, args)
		if err != nil {
			log.Errorf("exec mount failed: [%v], output[%v]", err, string(output))
			return err
		}
	}
	return nil
}

func bindMountVolume(sourcePath, mountPath string, readOnly bool) error {
	log.Infof("bindMountVolume source[%s] target[%s]", sourcePath, mountPath)
	if err := os.MkdirAll(mountPath, 0750); err != nil {
		log.Errorf("mkdir volume bindMountPath[%s] failed: %v", mountPath, err)
		return err
	}
	// check bind source
	isMountPoint, err := utils.IsMountPoint(sourcePath)
	if err != nil {
		log.Errorf("bind source %s has err :%v. unmounting ...", sourcePath, err)
		err := utils.ManualUnmount(sourcePath)
		if err != nil {
			log.Errorf("unmount mountPoint[%s] failed: %v", sourcePath, err)
			return err
		}
		log.Infof("bind source %s unmounted", sourcePath)
		// check again
		isMountPoint, err = utils.IsMountPoint(sourcePath)
		if err != nil {
			err := fmt.Errorf("unmount bind source %s failed: %v", sourcePath, err)
			log.Errorf(err.Error())
			return err
		}
	}
	if !isMountPoint {
		err := fmt.Errorf("bindMountVolume failed as sourcePath %s is not a valid mountpoint. Please check fuse pod", sourcePath)
		log.Errorf(err.Error())
		return err
	}
	// check bind target
	isMountPoint, err = utils.IsMountPoint(mountPath)
	if err != nil {
		log.Errorf("bind target %s has err :%v. unmounting ...", mountPath, err)
		err := utils.ManualUnmount(mountPath)
		if err != nil {
			log.Errorf("unmount mountPoint[%s] failed: %v", mountPath, err)
			return err
		}
		// check again
		isMountPoint, err = utils.IsMountPoint(mountPath)
		if err != nil {
			err := fmt.Errorf("unmount bind target %s failed: %v", mountPath, err)
			log.Errorf(err.Error())
			return err
		}
		log.Infof("bind target %s unmounted", mountPath)
	}
	if !isMountPoint {
		output, err := utils.ExecMountBind(sourcePath, mountPath, readOnly)
		if err != nil {
			log.Errorf("exec mount bind failed: %v, output[%s]", err, string(output))
			return err
		}
	}
	log.Infof("bindMountVolume from [%v] to [%v]", sourcePath, mountPath)
	return nil
}
