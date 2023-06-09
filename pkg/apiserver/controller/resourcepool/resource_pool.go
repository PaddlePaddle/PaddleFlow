/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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

package resourcepool

import (
	"errors"
	"fmt"

	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type CreateRequest struct {
	Name           string              `json:"name"`
	Namespace      string              `json:"namespace"`
	Type           string              `json:"type"`
	Provider       string              `json:"provider"`
	OrgID          string              `json:"orgID"`
	ClusterName    string              `json:"clusterName"`
	Description    string              `json:"description"`
	QuotaType      string              `json:"quotaType"`
	TotalResources schema.ResourceInfo `json:"totalResources"`
	MinResources   schema.ResourceInfo `json:"minResources"`
	Annotations    map[string]string   `json:"annotations"`
	Labels         map[string]string   `json:"labels"`
	IsHybrid       bool                `json:"isHybrid"`
	// ClusterID internal id of cluster
	ClusterID         string         `json:"-"`
	RawTotalResources model.Resource `json:"-"`
	Status            string         `json:"-"`
}

type CreateResponse struct {
	Name string `json:"name"`
}

type UpdateRequest struct {
	Name           string              `json:"-"`
	Namespace      string              `json:"-"`
	TotalResources schema.ResourceInfo `json:"totalResources,omitempty"`
	Annotations    map[string]string   `json:"annotations,omitempty"`
	Status         string              `json:"status,omitempty"`
}

type RPResponse struct {
	model.ResourcePool
}

type ListRequest struct {
	Name    string            `json:"name,omitempty"`
	Marker  string            `json:"marker"`
	MaxKeys int               `json:"maxKeys"`
	Status  string            `json:"status,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type ListResponse struct {
	common.MarkerInfo
	ResourcePoolList []model.ResourcePool `json:"resourcePoolList"`
}

func (c *CreateRequest) validateString(ctx *logger.RequestContext) error {
	if err := common.CheckName("name", c.Name); err != nil {
		return err
	}
	if err := common.CheckName("namespace", c.Namespace); err != nil {
		return err
	}
	if err := common.CheckLength("type", c.Type, 32); err != nil {
		return err
	}
	if err := common.CheckLength("provider", c.Provider, 32); err != nil {
		return err
	}
	if err := common.CheckLength("description", c.Description, 2048); err != nil {
		return err
	}
	return nil
}

func (c *CreateRequest) validateCluster(ctx *logger.RequestContext) error {
	if c.ClusterName == "" {
		c.ClusterName = config.DefaultClusterName
	}
	clusterInfo, err := storage.Cluster.GetClusterByName(c.ClusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln("create resource pool failed. error: cluster not found by Name.")
		return errors.New("cluster not found by Name")
	}
	if clusterInfo.Status != model.ClusterStatusOnLine {
		ctx.ErrorCode = common.InvalidClusterStatus
		errMsg := fmt.Sprintf("cluster[%s] not in online status, operator not permit", clusterInfo.Name)
		ctx.Logging().Errorln(errMsg)
		return errors.New(errMsg)
	}
	c.ClusterID = clusterInfo.ID
	return nil
}

func (c *CreateRequest) validateResources(ctx *logger.RequestContext) error {
	// check quota type of queue
	if len(c.QuotaType) == 0 {
		// TODO: get quota type from cluster info
		c.QuotaType = schema.TypeElasticQuota
	}
	if c.QuotaType != schema.TypeElasticQuota && c.QuotaType != schema.TypeVolcanoCapabilityQuota {
		err := fmt.Errorf("the type of quota [%s] is not supported", c.QuotaType)
		ctx.Logging().Errorln(err)
		ctx.ErrorCode = common.QueueQuotaTypeIsNotSupported
		return err
	}

	// check request total resources and min resources
	totalResources, err := resources.NewResourceFromMap(c.TotalResources.ToMap())
	if err != nil {
		ctx.Logging().Errorf("create queue failed. error: %s", err.Error())
		ctx.ErrorCode = common.InvalidComputeResource
		return err
	}
	c.RawTotalResources = model.Resource(*totalResources)
	return nil
}

func (c *CreateRequest) Validate(ctx *logger.RequestContext) error {
	err := c.validateString(ctx)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		return err
	}
	err = c.validateCluster(ctx)
	if err != nil {
		return err
	}
	return c.validateResources(ctx)
}

func Create(ctx *logger.RequestContext, request *CreateRequest) (CreateResponse, error) {
	ctx.Logging().Debugf("begin create resource pool. request:%s", config.PrettyFormat(request))
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("create resource pool failed. error: admin is needed.")
		return CreateResponse{}, errors.New("admin is needed")
	}

	if err := request.Validate(ctx); err != nil {
		ctx.Logging().Errorln("create resource pool failed. error:", err.Error())
		return CreateResponse{}, err
	}
	rpInfo := model.ResourcePool{
		Name:           request.Name,
		Namespace:      request.Namespace,
		Type:           request.Type,
		Provider:       request.Provider,
		OrgID:          request.OrgID,
		CreatorID:      ctx.UserID,
		Description:    request.Description,
		ClusterId:      request.ClusterID,
		QuotaType:      request.QuotaType,
		TotalResources: request.RawTotalResources,
		Annotations:    request.Annotations,
		Labels:         request.Labels,
		Status:         schema.StatusQueueCreating,
	}
	if request.IsHybrid {
		rpInfo.IsHybrid = 1
	}
	// TODO: create resource pool on cluster
	err := storage.ResourcePool.Create(&rpInfo)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("create resource pool failed. error:%s", err.Error())
		return CreateResponse{}, err
	}
	ctx.Logging().Debugf("create resource pool successfully. name:%s", request.Name)
	return CreateResponse{Name: request.Name}, nil
}

func updatingResources(ctx *logger.RequestContext, newRes schema.ResourceInfo, oldRes *model.Resource) (bool, error) {
	needUpdate := false
	rpRes := resources.Resource(*oldRes)
	if newRes.CPU != "" {
		needUpdate = true
		cpu, err := resources.ParseMilliQuantity(newRes.CPU)
		if err != nil || cpu < 0 {
			err = fmt.Errorf("parse cpu resource[%s] failed", newRes.CPU)
			return needUpdate, err
		}
		rpRes.SetResources(resources.ResCPU, int64(cpu))
	}
	if newRes.Mem != "" {
		needUpdate = true
		mem, err := resources.ParseQuantity(newRes.Mem)
		if err != nil || mem < 0 {
			err = fmt.Errorf("parse memory resource[%s] failed", newRes.Mem)
			return needUpdate, err
		}
		rpRes.SetResources(resources.ResMemory, int64(mem))
	}
	if len(newRes.ScalarResources) != 0 {
		needUpdate = true
		for rName, rValue := range newRes.ScalarResources {
			if rValue == "" {
				// remove empty resource
				rpRes.DelResources(string(rName))
			} else {
				rQuantity, err := resources.ParseQuantity(rValue)
				if err != nil || rQuantity < 0 {
					err = fmt.Errorf("parse scalarResource %s resource[%s] failed", rName, rValue)
					return needUpdate, err
				}
				rpRes.SetResources(string(rName), int64(rQuantity))

			}
		}
	}
	*oldRes = model.Resource(rpRes)
	return needUpdate, nil
}

func (u *UpdateRequest) validateResources(ctx *logger.RequestContext, rpResource *model.Resource) (bool, error) {
	return updatingResources(ctx, u.TotalResources, rpResource)
}

func (u *UpdateRequest) validateAnnotations(ctx *logger.RequestContext, rpInfo *model.ResourcePool) (bool, error) {
	if len(u.Annotations) == 0 {
		return false, nil
	}
	// check the hierarchy of elastic quota
	if rpInfo.QuotaType == schema.TypeElasticQuota {
		_, exist := u.Annotations[v1beta1.QuotaTypeKey]
		if exist {
			return false, fmt.Errorf("the isolaction type of resource pool cannot be changed")
		}
		// remove parent for physical elastic quota
		if rpInfo.Annotations[v1beta1.QuotaTypeKey] == v1beta1.QuotaTypePhysical {
			delete(u.Annotations, v1beta1.ElasticQuotaParentKey)
			delete(rpInfo.Annotations, v1beta1.ElasticQuotaParentKey)
		}
	}
	for key, value := range u.Annotations {
		if len(value) == 0 {
			// remove annotations when value is empty
			delete(rpInfo.Annotations, key)
		} else {
			rpInfo.Annotations[key] = value
		}
	}
	return true, nil
}

func (u *UpdateRequest) validateStatus(ctx *logger.RequestContext, rpInfo *model.ResourcePool) (bool, error) {
	if u.Status == "" {
		return false, nil
	}
	if u.Status != schema.StatusQueueOpen && u.Status != schema.StatusQueueClosed {
		return false, fmt.Errorf("the status of resource pool[%s] is invalid", u.Status)
	}
	rpInfo.Status = u.Status
	return u.Status != rpInfo.Status, nil
}

func (u *UpdateRequest) Validate(ctx *logger.RequestContext, rpInfo *model.ResourcePool) (bool, error) {
	// update resource pool
	needUpdateStatus, err := u.validateStatus(ctx, rpInfo)
	if err != nil {
		ctx.Logging().Errorf("update resource pool status failed. err: %s", err.Error())
		return false, err
	}
	needUpdate, err := u.validateAnnotations(ctx, rpInfo)
	if err != nil {
		ctx.Logging().Errorf("update resource pool annotations failed. err: %s", err.Error())
		return false, err
	}
	needUpdateRes, err := u.validateResources(ctx, &rpInfo.TotalResources)
	if err != nil {
		ctx.Logging().Errorf("update resource pool resources failed. err: %s", err.Error())
		return false, err
	}
	return needUpdateStatus || needUpdate || needUpdateRes, nil
}

// Update updating totalResources, status for resource pool
func Update(ctx *logger.RequestContext, request *UpdateRequest) (RPResponse, error) {
	ctx.Logging().Debugf("begin update request. request:%s", config.PrettyFormat(request))
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		msg := fmt.Sprintf("update resource pool failed. error: admin is needed")
		ctx.Logging().Errorln(msg)
		return RPResponse{}, errors.New(msg)
	}
	rpInfo, err := storage.ResourcePool.Get(request.Name)
	if err != nil {
		ctx.ErrorCode = common.RecordNotFound
		ctx.Logging().Errorf("get resource pool failed. error:%s", err.Error())
		return RPResponse{}, err
	}
	needUpdate, err := request.Validate(ctx, &rpInfo)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		return RPResponse{}, err
	}
	if !needUpdate {
		return RPResponse{rpInfo}, nil
	}
	// update resource pool in db
	if err = storage.ResourcePool.Update(&rpInfo); err != nil {
		ctx.Logging().Errorf("update resource pool failed. error:%s", err.Error())
		ctx.ErrorCode = common.InternalError
		return RPResponse{}, err
	}
	// TODO: update resource pool on cluster
	ctx.Logging().Debugf("update resource pool success. name:%s", rpInfo.Name)
	return RPResponse{rpInfo}, nil
}

func List(ctx *logger.RequestContext, req ListRequest) (ListResponse, error) {
	ctx.Logging().Debugf("begin list resource pool for user %s.", ctx.UserName)
	listResponse := ListResponse{}
	listResponse.IsTruncated = false
	listResponse.ResourcePoolList = []model.ResourcePool{}

	var pk int64
	var err error
	if req.Marker != "" {
		pk, err = common.DecryptPk(req.Marker)
		if err != nil {
			err = fmt.Errorf("decrypt marker[%s] failed. err:[%s]", req.Marker, err.Error())
			ctx.Logging().Errorln(err)
			ctx.ErrorCode = common.InvalidMarker
			return listResponse, err
		}
	}

	rpList, err := storage.ResourcePool.List(pk, req.MaxKeys+1, req.Labels)
	if err != nil {
		ctx.Logging().Errorf("list resource pool from db failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
		return listResponse, err
	}
	// get next marker
	if len(rpList) == req.MaxKeys+1 {
		rp := rpList[req.MaxKeys]
		nextMarker, err := common.EncryptPk(rp.Pk)
		if err != nil {
			ctx.Logging().Errorf("Encrypt marker failed, pk:[%d] error:[%s]", rp.Pk, err.Error())
			ctx.ErrorCode = common.InternalError
			return listResponse, err
		}
		rpList = rpList[0:req.MaxKeys]
		listResponse.NextMarker = nextMarker
		listResponse.IsTruncated = true
	}

	listResponse.MaxKeys = req.MaxKeys
	listResponse.ResourcePoolList = append(listResponse.ResourcePoolList, rpList...)
	return listResponse, nil
}

func GetByName(ctx *logger.RequestContext, name string) (RPResponse, error) {
	ctx.Logging().Debugf("begin get resource pool by name. name:%s", name)

	rp, err := storage.ResourcePool.Get(name)
	if err != nil {
		ctx.ErrorCode = common.ResourceNotFound
		return RPResponse{}, fmt.Errorf("resource pool[%s] not found", name)
	}
	if err = common.CheckPermission(ctx.UserName, rp.CreatorID, common.ResourceTypeResourcePool, rp.ID); err != nil {
		ctx.ErrorCode = common.ActionNotAllowed
		err = fmt.Errorf("get resource pool[%s] failed. error:  access denied", name)
		ctx.Logging().Errorln(err.Error())
		return RPResponse{}, err
	}
	return RPResponse{rp}, nil
}

func Delete(ctx *logger.RequestContext, rpName string) error {
	ctx.Logging().Debugf("begin delete resource pool. name: %s", rpName)
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		msg := fmt.Sprintf("delete resource pool failed. error: admin is needed")
		ctx.Logging().Errorln(msg)
		return errors.New(msg)
	}

	_, err := storage.ResourcePool.Get(rpName)
	if err != nil {
		ctx.ErrorCode = common.ResourceNotFound
		return fmt.Errorf("resource pool:%s not found", rpName)
	}

	err = storage.ResourcePool.Delete(rpName)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("delete resource pool update db failed. name: %s", rpName)
		return err
	}
	ctx.Logging().Debugf("resource pool is deleted. name:%s", rpName)
	return nil
}
