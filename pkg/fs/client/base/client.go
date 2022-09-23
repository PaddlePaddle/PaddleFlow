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
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

var Client *_Client

type _Client struct {
	Uuid       string
	FsID       string
	FsName     string
	UserName   string
	Token      string
	httpClient *core.PaddleFlowClient
}

func NewClient(fsID string, c *core.PaddleFlowClient, token string) (*_Client, error) {
	fsName, userName, err := utils.GetFsNameAndUserNameByFsID(fsID)
	if err != nil {
		return nil, err
	}
	_client := _Client{
		Uuid:       uuid.NewString(),
		FsID:       fsID,
		UserName:   userName,
		httpClient: c,
		Token:      token,
		FsName:     fsName,
	}
	Client = &_client
	return &_client, nil
}

func (c *_Client) GetFSMeta() (common.FSMeta, error) {
	log.Debugf("Http CLient is %v", *c)
	params := api.FsParams{
		FsName:   c.FsName,
		UserName: c.UserName,
		Token:    c.Token,
	}
	log.Infof("Fs MetaRequest Parmas %+v", params)
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
