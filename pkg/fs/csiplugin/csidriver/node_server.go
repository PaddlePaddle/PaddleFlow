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
	"fmt"
	"os"
	"os/exec"
	"paddleflow/pkg/common/http/core"
	"paddleflow/pkg/fs/csiplugin/mount"
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"paddleflow/pkg/client"
	"paddleflow/pkg/common/http/api"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/csiplugin/client/pfs"
	"paddleflow/pkg/fs/utils/common"
	"paddleflow/pkg/fs/utils/io"
	mountUtil "paddleflow/pkg/fs/utils/mount"
)

const (
	pfsFSID   = "pfs.fs.id"
	pfsServer = "pfs.server"

	FsBindPath = mount.MountDir + "/%s/storage" // /home/paddleflow/mnt/{fsID}/storage
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
	fsId := volumeContext[pfsFSID]
	server := volumeContext[pfsServer]

	httpClient := client.NewHttpClient(server, client.DefaultTimeOut)

	if !hasLoggedIn() {
		if err := ns.loginAsRoot(httpClient); err != nil {
			return &csi.NodePublishVolumeResponse{}, err
		}
	}

	_, err := base.NewClient(fsId, httpClient, base.LoginTokenRoot)
	if err != nil {
		log.Errorf("csi addRefOfMount: init client with fs[%s] and server[%s] failed: %v",
			fsId, server, err)
		return &csi.NodePublishVolumeResponse{}, err
	}

	mountInfo := pfs.GetMountInfo(fsId, server, req.GetReadonly())
	// root credentials for pfs-fuse
	mountInfo.UsernameRoot, mountInfo.PasswordRoot = ns.credentialInfo.usernameRoot, ns.credentialInfo.passwordRoot
	pathPrefix := filepath.Dir(targetPath)
	mountInfo.TargetPath = targetPath
	if err := mountVolume(pathPrefix, mountInfo, req.GetReadonly()); err != nil {
		log.Errorf("mount filesystem[%s] with server[%s] failed: %v", fsId, server, err)
		return &csi.NodePublishVolumeResponse{}, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

func hasLoggedIn() bool {
	if base.LoginTokenRoot == "" {
		return false
	} else {
		return true
	}
}

func (ns *nodeServer) loginAsRoot(httpClient *core.PFClient) error {
	loginReq := api.LoginParams{
		UserName: ns.credentialInfo.usernameRoot,
		Password: ns.credentialInfo.passwordRoot,
	}
	loginResponse, err := api.LoginRequest(loginReq, httpClient)
	if err != nil {
		log.Errorf("csi login as root failed: %v", err)
		return err
	}
	base.LoginTokenRoot = loginResponse.Authorization
	return nil
}

func (ns *nodeServer) NodeUnpublishVolume(ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	targetPath := req.GetTargetPath()
	log.Infof("Node Unpublish volume request [%+v], and begin to cleanup mountPoint in targetpath", *req)
	podUID := common.GetPodUIDFromTargetPath(targetPath)

	//if podUID != "" {
	//	// clean up mount points
	//	pathsToCleanup := []string{targetPath}
	//	sourcePath := common.GetVolumeSourceMountPath(filepath.Dir(targetPath))
	//	pathsToCleanup = append(pathsToCleanup, sourcePath)
	//	if err := cleanUpMountPoints(pathsToCleanup); err != nil {
	//		log.Errorf("[UnPublishVolume]: cleanup mount points[%v] err: %s", pathsToCleanup, err.Error())
	//		return nil, err
	//	}
	//}
	//return &csi.NodeUnpublishVolumeResponse{}, nil

	if podUID == "" {
		return &csi.NodeUnpublishVolumeResponse{}, nil
	}
	//// umount mp
	//if err := umountMountPath(targetPath); err != nil {
	//	log.Errorf("UnPublishVolume: umountMountPath[%v] err: %s", targetPath, err.Error())
	//	return nil, err
	//}
	var err error
	hasRef := true
	fsID := common.GetFsIDFromVolumeID(req.GetVolumeId())

	// clean up paths
	if base.Client != nil {
		hasRef, err = mount.HasRef(fsID)
		if err != nil {
			return nil, err
		}
	}

	if podUID != "" {
		// clean up mount points
		pathsToCleanup := []string{targetPath}
		if !hasRef {
			bindPath := fmt.Sprintf(FsBindPath, fsID)
			pathsToCleanup = append(pathsToCleanup, bindPath)
		}
		if err := cleanUpMountPoints(pathsToCleanup); err != nil {
			log.Errorf("UnPublishVolume: cleanup mount points[%v] err: %s", pathsToCleanup, err.Error())
			return nil, err
		}
	}

	// delete po if no refs
	if !hasRef {
		if err := mount.PodUmount(fsID); err != nil {
			log.Errorf("UnPublishVolume: PodUmount[%v] err: %s", fsID, err.Error())
			return nil, err
		}
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func umountMountPath(mountPath string) error {
	// targetPath may be mount bind many times when mount point recovered.
	// umount until it's not mounted.
	for {
		command := exec.Command("umount", mountPath)
		out, err := command.CombinedOutput()
		if err == nil {
			continue
		}
		log.Infoln(string(out))
		if !strings.Contains(string(out), "not mounted") &&
			!strings.Contains(string(out), "mountpoint not found") &&
			!strings.Contains(string(out), "no mount point specified") {
			log.Infof("Unmount %s failed: %q, try to lazy unmount", mountPath, err)
			output, err := exec.Command("umount", "-l", mountPath).CombinedOutput()
			if err != nil {
				log.Infof("Could not lazy unmount %q: %v, output: %s", mountPath, err, string(output))
				return err
			}
		}
		break
	}
	return nil
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

func mountVolume(mountPathPrefix string, mountInfo pfs.MountInfo, readOnly bool) error {
	log.Debugf("mountVolume mountInfo:%+v, readOnly:%t", mountInfo, readOnly)
	// business pods use a separate source path
	volumeSourceMountPath := common.GetVolumeSourceMountPath(mountPathPrefix)
	if err := os.MkdirAll(volumeSourceMountPath, 0750); err != nil {
		log.Errorf("mkdir [%s] failed: %v", volumeSourceMountPath, err)
		return err
	}
	mountInfo.LocalPath = volumeSourceMountPath

	//cmdName, args := mountInfo.GetMountCmd()
	//log.Infof("mountInfo GetMountCmd[%s %v] filesystem ID[%v] in pfs server[%v]", cmdName, args, mountInfo.FSID, mountInfo.Server)
	//output, err := mountUtil.ExecCmdWithTimeout(cmdName, args)
	//if err != nil {
	//	log.Errorf("exec mount failed: [%v], output[%v]", err, string(output))
	//	return err
	//}
	//
	//volumeBindMountPath := common.GetVolumeMountPath(mountPathPrefix)
	//return bindMountVolume(volumeSourceMountPath, volumeBindMountPath, readOnly)

	err := mount.PodMount(mountInfo)
	if err != nil {
		log.Errorf("MountThroughPod err: %v", err)
		return err
	}

	bindSource := fmt.Sprintf(FsBindPath, mountInfo.FSID)
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
	log.Debugf("bindMountVolume from [%v] to [%v]", sourcePath, mountPath)
	return nil
}

func cleanUpMountPoints(paths []string) error {
	var retErr error

	if len(paths) == 0 {
		return nil
	}

	cleanUp := func(path string, cleanAll bool) error {
		isMountPoint, err := mountUtil.IsMountPoint(path)
		if err != nil && !isMountPoint {
			if exist, exErr := io.Exist(path); exErr != nil {
				log.Errorf("check path[%s] exist failed: %v", path, exErr)
				return exErr
			} else if !exist {
				return nil
			}
			log.Errorf("check path[%s] mountpoint failed: %v", path, err)
			return err
		}

		if isMountPoint {
			return mountUtil.CleanUpMountPoint(path)
		}
		log.Infof("path [%s] is not a mountpoint, begin to remove path[%s]", path, path)
		if err := os.Remove(path); err != nil {
			log.Errorf("remove path[%s] failed: %v", path, err)
			return err
		}
		return nil
	}

	for _, path := range paths {
		log.Infof("cleanup mountPoint in path[%s] start", path)
		if err := cleanUp(path, false); err != nil {
			log.Errorf("cleanup path[%s] failed: %v", path, err)
			retErr = err
		}
	}
	return retErr
}
