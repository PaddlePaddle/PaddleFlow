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
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
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

type CreateClusterRequest struct {
	ClusterCommonInfo
	Name string `json:"clusterName"` // 集群名字
}

type ClusterCommonInfo struct {
	ID            string   `json:"clusterId"`     // 集群id
	Description   string   `json:"description"`   // 集群描述
	Endpoint      string   `json:"endpoint"`      // 集群endpoint, 比如 http://10.11.11.47:8080
	Source        string   `json:"source"`        // 来源, 比如 OnPremise （内部部署）、AWS、CCE
	ClusterType   string   `json:"clusterType"`   // 集群类型，比如kubernetes/local/yarn
	Version       string   `json:"version"`       // 集群版本v1.16
	Status        string   `json:"status"`        // 集群状态，可选值为online, offline
	Credential    string   `json:"credential"`    // 用于存储集群的凭证信息，比如k8s的kube_config配置
	Setting       string   `json:"setting"`       // 存储额外配置信息
	NamespaceList []string `json:"namespaceList"` // 命名空间列表，json类型，如["ns1", "ns2"]
}

type CreateClusterResponse struct {
	ID               string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Pk               int64    // 自增主键
	Name             string   // 集群名字
	Description      string   // 集群描述
	Endpoint         string   // 集群endpoint, 比如 http://10.11.11.47:8080
	Source           string   // 来源, 比如 OnPremise （内部部署）、AWS、CCE
	ClusterType      string   // 集群类型，比如Kubernetes/Local
	Version          string   // 集群版本，比如v1.16
	Status           string   // 集群状态，可选值为online, offline
	Credential       string   // 用于存储集群的凭证信息，比如k8s的kube_config配置
	Setting          string   // 存储额外配置信息
	RawNamespaceList string   // 命名空间列表，json类型，如["ns1", "ns2"]
	NamespaceList    []string // 命名空间列表，json类型，如["ns1", "ns2"]
	DeletedAt        string   // 删除标识，非空表示软删除
}

type ClusterInfo struct {
	ID               string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	Pk               int64    `gorm:"primaryKey;autoIncrement" json:"-"`      // 自增主键
	Name             string   `gorm:"column:name" json:"clusterName"`         // 集群名字
	Description      string   `gorm:"column:description" json:"description"`  // 集群描述
	Endpoint         string   `gorm:"column:endpoint" json:"endpoint"`        // 集群endpoint, 比如 http://10.11.11.47:8080
	Source           string   `gorm:"column:source" json:"source"`            // 来源, 比如 OnPremise （内部部署）、AWS、CCE
	ClusterType      string   `gorm:"column:cluster_type" json:"clusterType"` // 集群类型，比如Kubernetes/Local
	Version          string   `gorm:"column:version" json:"version"`          // 集群版本，比如v1.16
	Status           string   `gorm:"column:status" json:"status"`            // 集群状态，可选值为online, offline
	Credential       string   `gorm:"column:credential" json:"credential"`    // 用于存储集群的凭证信息，比如k8s的kube_config配置
	Setting          string   `gorm:"column:setting" json:"setting"`          // 存储额外配置信息
	RawNamespaceList string   `gorm:"column:namespace_list" json:"-"`         // 命名空间列表，json类型，如["ns1", "ns2"]
	NamespaceList    []string `gorm:"-" json:"namespaceList"`                 // 命名空间列表，json类型，如["ns1", "ns2"]
	DeletedAt        string   `gorm:"column:deleted_at" json:"-"`             // 删除标识，非空表示软删除
}

type GetClusterResponse struct {
	ClusterInfo
}

type ListClusterRequest struct {
	Marker          string   `json:"marker"`
	MaxKeys         int      `json:"maxKeys"`
	ClusterNameList []string `json:"clusterNameList"`
	ClusterStatus   string   `json:"clusterStatus"`
}

type ListClusterResponse struct {
	common.MarkerInfo
	ClusterList []ClusterInfo `json:"clusterList"`
}

type UpdateClusterRequest struct {
	ClusterCommonInfo
}

type UpdateClusterResponse struct {
	ClusterInfo
}

func (c *cluster) Create(ctx context.Context, request *CreateClusterRequest,
	token string) (result *CreateClusterResponse, err error) {
	result = &CreateClusterResponse{}
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
	token string) (result *GetClusterResponse, err error) {
	result = &GetClusterResponse{}
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

func (c *cluster) List(ctx context.Context, request *ListClusterRequest,
	token string) (result *ListClusterResponse, err error) {
	result = &ListClusterResponse{}
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

func (c *cluster) Update(ctx context.Context, clusterName string, request *UpdateClusterRequest,
	token string) (result *UpdateClusterResponse, err error) {
	result = &UpdateClusterResponse{}
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
	Create(ctx context.Context, request *CreateClusterRequest, token string) (*CreateClusterResponse, error)
	Get(ctx context.Context, clusterName string, token string) (*GetClusterResponse, error)
	List(ctx context.Context, request *ListClusterRequest, token string) (*ListClusterResponse, error)
	Update(ctx context.Context, clusterName string, request *UpdateClusterRequest, token string) (*UpdateClusterResponse, error)
	Delete(ctx context.Context, clusterName string, token string) error
}

// newCluster returns a cluster.
func newCluster(c *APIV1Client) *cluster {
	return &cluster{
		client: c.RESTClient(),
	}
}
