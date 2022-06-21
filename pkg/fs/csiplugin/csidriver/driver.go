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
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/drivers/pkg/csi-common"
	log "github.com/sirupsen/logrus"
)

const (
	driverName = "paddleflowstorage"
	version    = "1.4.3"
)

type credentials struct {
	usernameRoot string
	passwordRoot string
}

type driver struct {
	csiDriver        *csicommon.CSIDriver
	nodeId, endpoint string
	credentialInfo   credentials
}

func NewDriver(nodeID, endpoint, username, password string) *driver {
	log.Infof("Driver: %v version: %v", driverName, version)
	csiDriver := csicommon.NewCSIDriver(driverName, version, nodeID)
	csiDriver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME})
	csiDriver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER})

	return &driver{
		nodeId:    nodeID,
		endpoint:  endpoint,
		csiDriver: csiDriver,
		credentialInfo: credentials{
			usernameRoot: username,
			passwordRoot: password,
		},
	}
}

func (d *driver) newControllerServer() *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d.csiDriver),
	}
}

func (d *driver) newNodeServer() *nodeServer {
	return &nodeServer{
		nodeId:            d.nodeId,
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d.csiDriver),
		credentialInfo:    d.credentialInfo,
	}
}

func (d *driver) Run() {
	// TODO: add non blocking grpcServer to listen tcp
	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(
		d.endpoint,
		csicommon.NewDefaultIdentityServer(d.csiDriver),
		d.newControllerServer(),
		d.newNodeServer(),
	)
	s.Wait()
}
