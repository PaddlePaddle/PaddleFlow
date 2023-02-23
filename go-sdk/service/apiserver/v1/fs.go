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

package v1

import (
	"context"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	FsApi       = Prefix + "/fs"
	FsCacheApi  = Prefix + "/fsCache"
	StsApi      = Prefix + "/fsSts"
	KeyUsername = "username"
	StsDuration = "duration"
)

type fileSystem struct {
	client *core.PaddleFlowClient
}

type CreateFileSystemRequest struct {
	Name       string            `json:"name"`
	Url        string            `json:"url"`
	Properties map[string]string `json:"properties"`
	Username   string            `json:"username"`
}

type CreateFileSystemResponse struct {
	FsName string `json:"fsName"`
	FsID   string `json:"fsID"`
}

type GetFileSystemRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
}

type GetFileSystemResponse struct {
	Id            string            `json:"id"`
	Name          string            `json:"name"`
	ServerAddress string            `json:"serverAddress"`
	Type          string            `json:"type"`
	SubPath       string            `json:"subPath"`
	Username      string            `json:"username"`
	Properties    map[string]string `json:"properties"`
}

type DeleteFileSystemRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
}

type GetStsRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
}

type GetStsResponse struct {
	AccessKeyId     string `json:"accessKey"`
	SecretAccessKey string `json:"secretKey"`
	SessionToken    string `json:"sessionToken"`
	Bucket          string `json:"bucket"`
	SubPath         string `json:"subPath"`
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	CreateTime      string `json:"createTime"`
	Expiration      string `json:"expiration"`
	UserId          string `json:"userId"`
	Duration        int    `json:"duration"`
}

func (f *fileSystem) Create(ctx context.Context, request *CreateFileSystemRequest,
	token string) (result *CreateFileSystemResponse, err error) {
	result = &CreateFileSystemResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsApi).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (f *fileSystem) Get(ctx context.Context, request *GetFileSystemRequest,
	token string) (result *GetFileSystemResponse, err error) {
	result = &GetFileSystemResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsApi+"/"+request.FsName).
		WithQueryParam(KeyUsername, request.Username).
		WithMethod(http.GET).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (f *fileSystem) Delete(ctx context.Context, request *DeleteFileSystemRequest, token string) (err error) {
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsApi+"/"+request.FsName).
		WithQueryParam(KeyUsername, request.Username).
		WithMethod(http.DELETE).
		Do()
	return
}

func (f *fileSystem) Sts(ctx context.Context, request *GetStsRequest,
	token string) (result *GetStsResponse, err error) {
	result = &GetStsResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(StsApi+"/"+request.FsName).
		WithQueryParam(KeyUsername, request.Username).
		WithMethod(http.GET).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

type FileSystemGetter interface {
	FileSystem() FileSystemInterface
}

type FileSystemInterface interface {
	Create(ctx context.Context, request *CreateFileSystemRequest, token string) (*CreateFileSystemResponse, error)
	Get(ctx context.Context, request *GetFileSystemRequest, token string) (*GetFileSystemResponse, error)
	Delete(ctx context.Context, request *DeleteFileSystemRequest, token string) error
	Sts(ctx context.Context, request *GetStsRequest, token string) (*GetStsResponse, error)
}

// newFileSystem returns a fileSystem.
func newFileSystem(c *APIV1Client) *fileSystem {
	return &fileSystem{
		client: c.RESTClient(),
	}
}
