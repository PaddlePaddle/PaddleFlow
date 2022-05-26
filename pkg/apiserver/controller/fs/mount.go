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

package fs

import (
	"crypto/md5"
	"encoding/hex"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

type CreateMountRequest struct {
	FsName     string `json:"fsName" validate:"required"`
	Username   string `json:"username"`
	ClusterID  string `json:"clusterID"`
	MountPoint string `json:"mountPoint" validate:"required"`
	NodeName   string `json:"nodename" validate:"required"`
}

type ListMountRequest struct {
	FsName    string `json:"fsName"`
	Username  string `json:"username"`
	ClusterID string `json:"clusterID"`
	NodeName  string `json:"nodename" validate:"required"`
	Marker    string `json:"marker"`
	MaxKeys   int32  `json:"maxKeys"`
}

type DeleteMountRequest struct {
	FsName     string `json:"fsName" validate:"required"`
	Username   string `json:"username"`
	ClusterID  string `json:"clusterID"`
	MountPoint string `json:"mountPoint" validate:"required"`
	NodeName   string `json:"nodename" validate:"required"`
}

type ListMountResponse struct {
	Marker     string           `json:"marker"`
	Truncated  bool             `json:"truncated"`
	NextMarker string           `json:"nextMarker"`
	MountList  []*MountResponse `json:"mountList"`
}

type MountResponse struct {
	MountID    string `json:"mountID"`
	FsID       string `json:"fsID"`
	MountPoint string `json:"mountpoint"`
	NodeName   string `json:"nodename"`
	ClusterID  string `json:"clusterID"`
}

func CreateMount(ctx *logger.RequestContext, req CreateMountRequest) error {
	fsID := common.ID(req.Username, req.FsName)
	mountID := GetMountID(req.ClusterID, req.NodeName, req.MountPoint)

	fsMount := &models.FsMount{
		FsID:       fsID,
		MountPoint: req.MountPoint,
		MountID:    mountID,
		NodeName:   req.NodeName,
		ClusterID:  req.ClusterID,
	}
	err := fsMount.Add(fsMount)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("create mount with req[%v] err:%v", req, err)
		return err
	}
	return err
}

func GetMountID(clusterID, nodeName, mountPoint string) string {
	hash := md5.Sum([]byte(clusterID + nodeName + mountPoint))
	return hex.EncodeToString(hash[:])
}

func ListMount(ctx *logger.RequestContext, req ListMountRequest) ([]models.FsMount, string, error) {
	fsMount := &models.FsMount{
		NodeName: req.NodeName,
	}
	if req.FsName != "" {
		fsID := common.ID(req.Username, req.FsName)
		fsMount.FsID = fsID
	}
	if req.ClusterID != "" {
		fsMount.ClusterID = req.ClusterID
	}
	limit := req.MaxKeys + 1
	marker := req.Marker
	if req.Marker == "" {
		marker = time.Now().Format(TimeFormat)
	}

	items, err := fsMount.ListMount(fsMount, int(limit), marker)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("list mount with req[%v] err:%v", req, err)
		return nil, "", err
	}
	itemsLen := len(items)
	if itemsLen == 0 {
		return items, "", err
	}
	if itemsLen > int(req.MaxKeys) {
		return items[:len(items)-1], items[len(items)-1].UpdatedAt.Format(TimeFormat), err
	}
	return items, "", err
}

func DeleteMount(ctx *logger.RequestContext, req DeleteMountRequest) error {
	fsID := common.ID(req.Username, req.FsName)
	mountID := GetMountID(req.ClusterID, req.NodeName, req.MountPoint)

	fsMount := &models.FsMount{
		FsID:       fsID,
		MountPoint: req.MountPoint,
		MountID:    mountID,
		NodeName:   req.NodeName,
		ClusterID:  req.ClusterID,
	}
	err := fsMount.DeleteMount(mountID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("delete mount with req[%v] err:%v", req, err)
		return err
	}
	return err
}
