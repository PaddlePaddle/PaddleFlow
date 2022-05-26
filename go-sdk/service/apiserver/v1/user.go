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

	user_ "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/user"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	Prefix   = util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1
	LoginApi = Prefix + "/login"
)

type user struct {
	client *core.PaddleFlowClient
}

func (u *user) Login(ctx context.Context, request *user_.LoginInfo) (result *user_.LoginResponse, err error) {
	result = &user_.LoginResponse{}
	err = core.NewRequestBuilder(u.client).
		WithURL(LoginApi).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

type UserGetter interface {
	User() UserInterface
}

type UserInterface interface {
	Login(ctx context.Context, request *user_.LoginInfo) (*user_.LoginResponse, error)
}

// newUsers returns a Users.
func newUsers(c *APIV1Client) *user {
	return &user{
		client: c.RESTClient(),
	}
}
