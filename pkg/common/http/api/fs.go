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

package api

import (
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/http/core"
	"paddleflow/pkg/common/http/util/http"
)

const (
	Prefix       = util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1
	LoginApi     = Prefix + "/login"
	GetFsApi     = Prefix + "/fs"
	GetLinksApis = Prefix + "/link"
)

type LoginParams struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Authorization string `json:"authorization"`
}

type FsParams struct {
	FsID  string
	Token string
}

type LinksParams struct {
	Marker   string `json:"marker"`
	MaxKeys  int32  `json:"maxKeys"`
	Username string `json:"username"`
	FsID     string `json:"fsID"`
	FsPath   string `json:"fsPath"`
	Token    string
}

type FsResponse struct {
	Id            string            `json:"id"`
	Name          string            `json:"name"`
	ServerAddress string            `json:"serverAddress"`
	Type          string            `json:"type"`
	SubPath       string            `json:"subPath"`
	Username      string            `json:"username"`
	Properties    map[string]string `json:"properties"`
}

type LinkResponse struct {
	FsName        string            `json:"fsName"`
	FsPath        string            `json:"fsPath"`
	ServerAddress string            `json:"serverAddress"`
	Type          string            `json:"type"`
	Username      string            `json:"username"`
	SubPath       string            `json:"subPath"`
	Properties    map[string]string `json:"properties"`
}

type LinksResponse struct {
	Marker     string          `json:"marker"`
	Truncated  bool            `json:"truncated"`
	NextMarker string          `json:"nextMarker"`
	LinkList   []*LinkResponse `json:"linkList"`
}

func LoginRequest(params LoginParams, c *core.PFClient) (*LoginResponse, error) {
	var err error
	resp := &LoginResponse{}
	err = core.NewRequestBuilder(c).
		WithURL(LoginApi).
		WithMethod(http.POST).
		WithBody(params).
		WithResult(resp).
		Do()
	if err != nil {
		log.Errorf("login response err: %v", err)
		return nil, err
	}
	return resp, nil
}

func FsRequest(params FsParams, c *core.PFClient) (*FsResponse, error) {
	resp := &FsResponse{}
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, params.Token).
		WithURL(GetFsApi + "/" + params.FsID).
		WithMethod(http.GET).
		WithResult(resp).
		Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func LinksRequest(params LinksParams, c *core.PFClient) (*LinksResponse, error) {
	resp := &LinksResponse{}
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, params.Token).
		WithURL(GetLinksApis + "/" + params.FsID).
		WithMethod(http.GET).
		WithResult(resp).
		Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}
