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

package base

import (
	"strings"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/common/http/api"
	"paddleflow/pkg/common/http/core"
	"paddleflow/pkg/fs/common"
)

var Client *_Client

type _Client struct {
	Uuid       string
	FsID       string
	FsName     string
	UserName   string
	Token      string
	httpClient *core.PFClient
}

func NewClient(fsID string, c *core.PFClient, token string) (*_Client, error) {
	fsArray := strings.Split(fsID, "-")
	realUsername := strings.Join(fsArray[1:len(fsArray)-1], "")
	_client := _Client{
		Uuid:       uuid.NewString(),
		FsID:       fsID,
		UserName:   realUsername,
		httpClient: c,
		Token:      token,
		FsName:     fsArray[len(fsArray)-1],
	}
	Client = &_client
	return Client, nil
}

func (c *_Client) GetFSMeta() (common.FSMeta, error) {
	log.Debugf("Http CLient is %v", *c)
	params := api.FsParams{
		FsName:   c.FsName,
		UserName: c.UserName,
		Token:    c.Token,
	}
	fsResponseMeta, err := api.FsRequest(params, c.httpClient)
	if err != nil {
		log.Errorf("fs request failed: %v", err)
		return common.FSMeta{}, err
	}
	log.Debugf("the resp is [%+v]", fsResponseMeta)
	fsMeta := common.FSMeta{
		ID:            fsResponseMeta.Id,
		Name:          fsResponseMeta.Name,
		UfsType:       fsResponseMeta.Type,
		ServerAddress: fsResponseMeta.ServerAddress,
		SubPath:       fsResponseMeta.SubPath,
		Properties:    fsResponseMeta.Properties,
	}
	return fsMeta, nil
}

func (c *_Client) GetLinks() (map[string]common.FSMeta, error) {
	log.Debugf("http CLient is %v", *c)
	params := api.LinksParams{
		FsParams: api.FsParams{
			Token:    c.Token,
			FsName:   c.FsName,
			UserName: c.UserName,
		},
	}
	result := make(map[string]common.FSMeta)

	linkResult, err := api.LinksRequest(params, c.httpClient)
	if err != nil {
		log.Errorf("links request failed: %v", err)
		return nil, err
	}
	linkList := linkResult.LinkList

	for _, link := range linkList {
		result[link.FsPath] = common.FSMeta{
			Name:          link.FsName,
			UfsType:       link.Type,
			ServerAddress: link.ServerAddress,
			SubPath:       link.SubPath,
			Properties:    link.Properties,
			// type: fs 表示是默认的后端存储；link 表示是外部存储
			Type: common.LinkType,
		}
	}
	return result, nil
}

func (c *_Client) GetFSCacheConfig() (common.FsCacheConfig, error) {
	log.Debugf("Http CLient is %v", *c)
	params := api.FsParams{
		FsName:   c.FsName,
		UserName: c.UserName,
		Token:    c.Token,
	}
	cacheResp, err := api.FsCacheRequest(params, c.httpClient)
	if err != nil {
		log.Errorf("fs request failed: %v", err)
		return common.FsCacheConfig{}, err
	}
	log.Debugf("the resp is [%+v]", cacheResp)
	cacheConfig := common.FsCacheConfig{
		CacheDir:            cacheResp.CacheDir,
		Quota:               cacheResp.Quota,
		CacheType:           cacheResp.CacheType,
		BlockSize:           cacheResp.BlockSize,
		NodeAffinity:        cacheResp.NodeAffinity,
		NodeTaintToleration: cacheResp.NodeTaintToleration,
		ExtraConfig:         cacheResp.ExtraConfig,
		FsName:              cacheResp.FsName,
		Username:            cacheResp.Username,
	}
	return cacheConfig, nil
}

func (c *_Client) CreateFsMount(req api.CreateMountRequest) error {
	log.Debugf("CreateFsMount client: %+v, req: %+v", *c, req)
	err := api.FsMountCreate(req, c.httpClient)
	if err != nil {
		log.Errorf("fs request failed: %v", err)
	}
	return err
}
