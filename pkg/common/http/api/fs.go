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

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	Prefix            = util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1
	LoginApi          = Prefix + "/login"
	GetFsApi          = Prefix + "/fs"
	GetLinksApis      = Prefix + "/link"
	FsCacheConfig     = Prefix + "/fsCache"
	FsMount           = Prefix + "/fsMount"
	CacheReportConfig = Prefix + "/fsCache/report"

	KeyUsername   = "username"
	KeyFsName     = "fsName"
	KeyClusterID  = "clusterID"
	KeyNodeName   = "nodename"
	KeyMountPoint = "mountpoint"
)

type LoginParams struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	Authorization string `json:"authorization"`
}

type FsParams struct {
	FsName   string `json:"fsName"`
	UserName string `json:"username"`
	Token    string
}

type CacheReportParams struct {
	FsParams
	ClusterID string `json:"clusterID"`
	CacheDir  string `json:"cacheDir"`
	NodeName  string `json:"nodename"`
	UsedSize  int    `json:"usedsize"`
}

type LinksParams struct {
	Marker  string `json:"marker"`
	MaxKeys int32  `json:"maxKeys"`
	FsPath  string `json:"fsPath"`
	FsParams
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

type FsCacheResponse struct {
	CacheDir            string                 `json:"cacheDir"`
	Quota               int                    `json:"quota"`
	MetaDriver          string                 `json:"metaDriver"`
	BlockSize           int                    `json:"blockSize"`
	Debug               bool                   `json:"debug"`
	NodeAffinity        map[string]interface{} `json:"nodeAffinity"`
	NodeTaintToleration map[string]interface{} `json:"nodeTaintToleration"`
	ExtraConfig         map[string]string      `json:"extraConfig"`
	FsName              string                 `json:"fsName"`
	Username            string                 `json:"username"`
	CreateTime          string                 `json:"createTime"`
	UpdateTime          string                 `json:"updateTime,omitempty"`
}

type CreateMountRequest struct {
	ClusterID  string `json:"clusterID"`
	MountPoint string `json:"mountPoint" validate:"required"`
	NodeName   string `json:"nodename" validate:"required"`
	FsParams
}

type ListMountRequest struct {
	FsParams
	ClusterID string `json:"clusterID"`
	NodeName  string `json:"nodename" validate:"required"`
	Marker    string `json:"marker"`
	MaxKeys   int32  `json:"maxKeys"`
}

type DeleteMountRequest struct {
	FsParams
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

func LoginRequest(params LoginParams, c *core.PaddleFlowClient) (*LoginResponse, error) {
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

func FsRequest(params FsParams, c *core.PaddleFlowClient) (*FsResponse, error) {
	resp := &FsResponse{}
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, params.Token).
		WithURL(GetFsApi+"/"+params.FsName).
		WithQueryParam(KeyUsername, params.UserName).
		WithMethod(http.GET).
		WithResult(resp).
		Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func LinksRequest(params LinksParams, c *core.PaddleFlowClient) (*LinksResponse, error) {
	resp := &LinksResponse{}
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, params.Token).
		WithURL(GetLinksApis+"/"+params.FsName).
		WithQueryParam(KeyUsername, params.UserName).WithMethod(http.GET).
		WithResult(resp).
		Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func FsCacheRequest(params FsParams, c *core.PaddleFlowClient) (*FsCacheResponse, error) {
	resp := &FsCacheResponse{}
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, params.Token).
		WithURL(FsCacheConfig+"/"+params.FsName).
		WithQueryParam(KeyUsername, params.UserName).WithMethod(http.GET).
		WithResult(resp).
		Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func FsMountCreate(req CreateMountRequest, c *core.PaddleFlowClient) error {
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, req.Token).
		WithURL(FsMount).
		WithMethod(http.POST).
		WithBody(req).
		Do()
	return err
}

func FsMountList(req ListMountRequest, c *core.PaddleFlowClient) (*ListMountResponse, error) {
	resp := &ListMountResponse{}
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, req.Token).
		WithURL(FsMount).
		WithQueryParam(KeyClusterID, req.ClusterID).
		WithQueryParam(KeyNodeName, req.NodeName).
		WithQueryParam(KeyFsName, req.FsName).
		WithQueryParam(KeyUsername, req.UserName).
		WithMethod(http.GET).
		WithResult(resp).
		Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func FsMountDelete(req DeleteMountRequest, c *core.PaddleFlowClient) error {
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, req.Token).
		WithURL(FsMount+"/"+req.FsName).
		WithQueryParam(KeyClusterID, req.ClusterID).
		WithQueryParam(KeyNodeName, req.NodeName).
		WithQueryParam(KeyUsername, req.UserName).
		WithQueryParam(KeyMountPoint, req.MountPoint).
		WithMethod(http.DELETE).
		Do()
	return err
}

func CacheReportRequest(req CacheReportParams, c *core.PaddleFlowClient) error {
	err := core.NewRequestBuilder(c).
		WithHeader(common.HeaderKeyAuthorization, req.Token).
		WithURL(CacheReportConfig).
		WithBody(req).WithMethod(http.POST).Do()
	if err != nil {
		return err
	}
	return nil
}
