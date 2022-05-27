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
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	FsApi       = Prefix + "/fs"
	KeyUsername = "username"
)

type fileSystem struct {
	client *core.PaddleFlowClient
}

func (f *fileSystem) Create(ctx context.Context, request *fs.CreateFileSystemRequest,
	token string) (result *fs.CreateFileSystemResponse, err error) {
	result = &fs.CreateFileSystemResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsApi).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (f *fileSystem) Get(ctx context.Context, request *fs.GetFileSystemRequest,
	token string) (result *fs.GetFileSystemResponse, err error) {
	result = &fs.GetFileSystemResponse{}
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

func (f *fileSystem) Delete(ctx context.Context, request *fs.DeleteFileSystemRequest, token string) (err error) {
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsApi+"/"+request.FsName).
		WithQueryParam(KeyUsername, request.Username).
		WithMethod(http.DELETE).
		Do()
	return
}

type FileSystemGetter interface {
	FileSystem() FileSystemInterface
}

type FileSystemInterface interface {
	Create(ctx context.Context, request *fs.CreateFileSystemRequest, token string) (*fs.CreateFileSystemResponse, error)
	Get(ctx context.Context, request *fs.GetFileSystemRequest, token string) (*fs.GetFileSystemResponse, error)
	Delete(ctx context.Context, request *fs.DeleteFileSystemRequest, token string) error
}

// newFileSystem returns a fileSystem.
func newFileSystem(c *APIV1Client) *fileSystem {
	return &fileSystem{
		client: c.RESTClient(),
	}
}
