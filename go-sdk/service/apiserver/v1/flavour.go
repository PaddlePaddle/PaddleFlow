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
	"gorm.io/gorm"
	"strconv"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	FlavourAPI = Prefix + "/flavour"
)

type flavour struct {
	client *core.PaddleFlowClient
}

type Flavour struct {
	ID                 string                     `json:"id"`
	CreatedAt          time.Time                  `json:"-"`
	UpdatedAt          time.Time                  `json:"-"`
	Pk                 int64                      `json:"-"           gorm:"primaryKey;autoIncrement"`
	Name               string                     `json:"name"        gorm:"uniqueIndex"`
	ClusterID          string                     `json:"-" gorm:"column:cluster_id;default:''"`
	ClusterName        string                     `json:"-" gorm:"column:cluster_name;->"`
	CPU                string                     `json:"cpu"         gorm:"column:cpu"`
	Mem                string                     `json:"mem"         gorm:"column:mem"`
	RawScalarResources string                     `json:"-"           gorm:"column:scalar_resources;type:text;default:'{}'"`
	ScalarResources    schema.ScalarResourcesType `json:"scalarResources" gorm:"-"`
	UserName           string                     `json:"-" gorm:"column:user_name"`
	DeletedAt          gorm.DeletedAt             `json:"-" gorm:"index"`
}

// CreateFlavourRequest convey request for create flavour
type CreateFlavourRequest struct {
	Name            string                     `json:"name"`
	CPU             string                     `json:"cpu"`
	Mem             string                     `json:"mem"`
	ScalarResources schema.ScalarResourcesType `json:"scalarResources,omitempty"`
}

// UpdateFlavourRequest convey request for update flavour
type UpdateFlavourRequest struct {
	Name            string                     `json:"-"`
	CPU             string                     `json:"cpu,omitempty"`
	Mem             string                     `json:"mem,omitempty"`
	ScalarResources schema.ScalarResourcesType `json:"scalarResources,omitempty"`
}

type ListFlavourRequest struct {
	ClusterName string `json:"clusterName,omitempty"`
	Name        string `json:"name"`
	Marker      string `json:"marker"`
	MaxKeys     int    `json:"maxKeys"`
}

// CreateFlavourResponse convey response for create flavour
type CreateFlavourResponse struct {
	FlavourName string `json:"name"`
}

// UpdateFlavourResponse convey response for update flavour
type UpdateFlavourResponse struct {
	Flavour
}

// ListFlavourResponse convey response for list flavour
type ListFlavourResponse struct {
	common.MarkerInfo
	FlavourList []Flavour `json:"flavourList"`
}

func (f *flavour) Create(ctx context.Context, request *CreateFlavourRequest,
	token string) (result *CreateFlavourResponse, err error) {
	result = &CreateFlavourResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FlavourAPI).
		WithMethod(http.POST).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (f *flavour) Update(ctx context.Context, request *UpdateFlavourRequest,
	token string) (result *UpdateFlavourResponse, err error) {
	result = &UpdateFlavourResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FlavourAPI + "/" + request.Name).
		WithMethod(http.PUT).
		WithBody(request).
		WithResult(result).
		Do()
	return
}

func (f *flavour) Get(ctx context.Context, name string,
	token string) (result *Flavour, err error) {
	result = &Flavour{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FlavourAPI + "/" + name).
		WithMethod(http.GET).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (f *flavour) List(ctx context.Context, request *ListFlavourRequest,
	token string) (result *ListFlavourResponse, err error) {
	result = &ListFlavourResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FlavourAPI).
		WithMethod(http.GET).
		WithQueryParamFilter(KeyMarker, request.Marker).
		WithQueryParamFilter(KeyMaxKeys, strconv.Itoa(request.MaxKeys)).
		WithQueryParamFilter(KeyName, request.Name).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (f *flavour) Delete(ctx context.Context, name string,
	token string) (err error) {
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FlavourAPI + "/" + name).
		WithMethod(http.DELETE).
		Do()
	return
}

type FlavourGetter interface {
	Flavour() FlavourInterface
}

type FlavourInterface interface {
	Create(ctx context.Context, request *CreateFlavourRequest, token string) (*CreateFlavourResponse, error)
	Update(ctx context.Context, request *UpdateFlavourRequest, token string) (*UpdateFlavourResponse, error)
	Get(ctx context.Context, name string, token string) (*Flavour, error)
	List(ctx context.Context, request *ListFlavourRequest, token string) (*ListFlavourResponse, error)
	Delete(ctx context.Context, name string, token string) error
}

// newFlavour returns a flavour.
func newFlavour(c *APIV1Client) *flavour {
	return &flavour{
		client: c.RESTClient(),
	}
}
