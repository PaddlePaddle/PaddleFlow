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

package mount

import (
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/client/pfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
)

func fsCacheConfig(mountInfo pfs.MountInfo, httpClient *core.PaddleFlowClient, token string) (common.FsCacheConfig, error) {
	userName, fsName := common.GetFsNameAndUserNameByFsID(mountInfo.FSID)
	cacheReq := api.FsParams{
		FsName:   fsName,
		UserName: userName,
		Token:    token,
	}
	cacheResp, err := api.FsCacheRequest(cacheReq, httpClient)
	if err != nil {
		log.Errorf("FsCacheRequest failed: %v", err)
		return common.FsCacheConfig{}, err
	}
	log.Infof("the resp is [%+v]", cacheResp)
	cacheConfig := common.FsCacheConfig{
		CacheDir:            cacheResp.CacheDir,
		Quota:               cacheResp.Quota,
		MetaDriver:          cacheResp.MetaDriver,
		BlockSize:           cacheResp.BlockSize,
		NodeAffinity:        cacheResp.NodeAffinity,
		NodeTaintToleration: cacheResp.NodeTaintToleration,
		ExtraConfig:         cacheResp.ExtraConfig,
		FsName:              cacheResp.FsName,
		Username:            cacheResp.Username,
	}
	return cacheConfig, nil
}

func getFs(fsID string, httpClient *core.PaddleFlowClient, token string) (*api.FsResponse, error) {
	userName, fsName := common.GetFsNameAndUserNameByFsID(fsID)
	params := api.FsParams{
		FsName:   fsName,
		UserName: userName,
		Token:    token,
	}
	fsResp, err := api.FsRequest(params, httpClient)
	if err != nil {
		log.Errorf("fs request[%+v] failed: %v", params, err)
		return nil, err
	}
	return fsResp, nil
}

func deleteMount(mountInfo pfs.MountInfo, httpClient *core.PaddleFlowClient, token string) error {
	userName, fsName := common.GetFsNameAndUserNameByFsID(mountInfo.FSID)
	DeleteMountReq := api.DeleteMountRequest{
		FsParams: api.FsParams{
			FsName:   fsName,
			UserName: userName,
			Token:    token,
		},
		ClusterID:  mountInfo.ClusterID,
		NodeName:   csiconfig.NodeName,
		MountPoint: mountInfo.TargetPath,
	}
	err := api.FsMountDelete(DeleteMountReq, httpClient)
	if err != nil {
		log.Errorf("DeleteMount[%s] failed: %v", mountInfo.FSID, err)
		return err
	}
	return nil
}

func listMount(mountInfo pfs.MountInfo, httpClient *core.PaddleFlowClient, token string) (*api.ListMountResponse, error) {
	userName, fsName := common.GetFsNameAndUserNameByFsID(mountInfo.FSID)
	listMountReq := api.ListMountRequest{
		FsParams: api.FsParams{
			FsName:   fsName,
			UserName: userName,
			Token:    token,
		},
		ClusterID: mountInfo.ClusterID,
		NodeName:  csiconfig.NodeName,
	}
	listMountResp, err := api.FsMountList(listMountReq, httpClient)
	if err != nil {
		log.Errorf("FsMountList[%s] failed: %v", mountInfo.FSID, err)
		return nil, err
	}
	return listMountResp, nil
}

func createMount(mountInfo pfs.MountInfo, httpClient *core.PaddleFlowClient, token string) error {
	userName, fsName := common.GetFsNameAndUserNameByFsID(mountInfo.FSID)
	createMountReq := api.CreateMountRequest{
		FsParams: api.FsParams{
			FsName:   fsName,
			UserName: userName,
			Token:    token,
		},
		ClusterID:  mountInfo.ClusterID,
		MountPoint: mountInfo.TargetPath,
		NodeName:   csiconfig.NodeName,
	}
	err := api.FsMountCreate(createMountReq, httpClient)
	if err != nil {
		log.Errorf("CreateFsMount[%s] failed: %v", mountInfo.FSID, err)
		return err
	}
	return nil
}
