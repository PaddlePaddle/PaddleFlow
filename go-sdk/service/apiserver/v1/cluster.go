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
	"strconv"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	cluster_ "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/cluster"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

const (
	ClusterApi         = Prefix + "/cluster"
	KeyMarker          = "marker"
	KeyMaxKeys         = "maxKeys"
	KeyClusterNameList = "clusterNames"
	KeyClusterStatus   = "clusterStatus"
)

type cluster struct {
	client *core.PaddleFlowClient
}

func (c *cluster) Create(ctx context.Context, request *cluster_.CreateClusterRequest,
	token string) (result *cluster_.CreateClusterResponse, err error) {
	result = &cluster_.CreateClusterResponse{}
	err = core.NewRequestBuilder(c.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(ClusterApi).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (c *cluster) Get(ctx context.Context, clusterName,
	token string) (result *cluster_.GetClusterResponse, err error) {
	result = &cluster_.GetClusterResponse{}
	err = core.NewRequestBuilder(c.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(ClusterApi + "/" + clusterName).
		WithMethod(http.GET).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (c *cluster) List(ctx context.Context, request *cluster_.ListClusterRequest,
	token string) (result *cluster_.ListClusterResponse, err error) {
	result = &cluster_.ListClusterResponse{}
	err = core.NewRequestBuilder(c.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(ClusterApi).
		WithMethod(http.GET).
		WithQueryParamFilter(KeyMarker, request.Marker).
		WithQueryParamFilter(KeyMaxKeys, strconv.Itoa(request.MaxKeys)).
		WithQueryParamFilter(KeyClusterNameList, strings.Join(request.ClusterNameList, ",")).
		WithQueryParamFilter(KeyClusterStatus, request.ClusterStatus).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (c *cluster) Update(ctx context.Context, clusterName string, request *cluster_.UpdateClusterRequest,
	token string) (result *cluster_.UpdateClusterReponse, err error) {
	result = &cluster_.UpdateClusterReponse{}
	err = core.NewRequestBuilder(c.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(ClusterApi + "/" + clusterName).
		WithMethod(http.PUT).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (c *cluster) Delete(ctx context.Context, clusterName, token string) (err error) {
	err = core.NewRequestBuilder(c.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(ClusterApi + "/" + clusterName).
		WithMethod(http.DELETE).
		Do()
	return
}

type ClusterGetter interface {
	Cluster() ClusterInterface
}

type ClusterInterface interface {
	Create(ctx context.Context, request *cluster_.CreateClusterRequest, token string) (*cluster_.CreateClusterResponse, error)
	Get(ctx context.Context, clusterName string, token string) (*cluster_.GetClusterResponse, error)
	List(ctx context.Context, request *cluster_.ListClusterRequest, token string) (*cluster_.ListClusterResponse, error)
	Update(ctx context.Context, clusterName string, request *cluster_.UpdateClusterRequest, token string) (*cluster_.UpdateClusterReponse, error)
	Delete(ctx context.Context, clusterName string, token string) error
}

// newCluster returns a cluster.
func newCluster(c *APIV1Client) *cluster {
	return &cluster{
		client: c.RESTClient(),
	}
}
