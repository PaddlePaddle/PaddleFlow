package csi

import (
	"context"
	"errors"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

const (
	ProcessMount = "process"
	PodMount     = "pod"
)

type nodeServer struct {
	nodeId string
	*csicommon.DefaultNodeServer
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

	log.Infof("targetPath: %s", targetPath)
	volumeContext := req.GetVolumeContext()
	mountMethod := volumeContext[schema.PFSMountMethod]
	if mountMethod == ProcessMount {
		err := mountInCsi(volumeContext, req.GetTargetPath())
		if err != nil {
			log.Errorf("mounInCsi err: %v", err)
			return nil, errors.New("mount in csi fail: " + err.Error())
		}
	} else {
		log.Infof("mount in pod volumContext %v", volumeContext)
		err := mountInPod(volumeContext, req.GetTargetPath(), req.VolumeId, req.GetReadonly())
		if err != nil {
			log.Errorf("mountInPod err: %v", err)
			return nil, errors.New("mount in pod fail: " + err.Error())
		}
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	log.Infof("NodeUnpublishVolume with req: %+v", req)
	mountPoint := req.TargetPath
	paths := []string{mountPoint}
	err := utils.CleanUpMountPoints(paths)
	if err != nil {
		log.Errorf("CleanupMountPoints err %v", err)
		return nil, err
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (ns *nodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (ns *nodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (
	*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}
