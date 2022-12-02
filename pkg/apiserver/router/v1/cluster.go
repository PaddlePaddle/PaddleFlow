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

package v1

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/cluster"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

type ClusterRouter struct{}

func (cr *ClusterRouter) Name() string {
	return "ClusterRouter"
}

func (cr *ClusterRouter) AddRouter(r chi.Router) {
	log.Info("add cluster router")

	r.Post("/cluster", cr.createCluster)
	r.Get("/cluster", cr.listCluster)
	r.Get("/cluster/{clusterName}", cr.getClusterDetail)
	r.Delete("/cluster/{clusterName}", cr.deleteCluster)
	r.Put("/cluster/{clusterName}", cr.updateCluster)
	r.Get("/cluster/resource", cr.listClusterQuota)
	r.Post("/cluster/resource", cr.listClusterQuotaV2)

	r.Post("/cluster/{clusterName}/k8s/object", func(w http.ResponseWriter, r *http.Request) {
		ctx := common.GetRequestContext(r)
		action := r.URL.Query().Get(util.QueryKeyAction)
		switch action {
		case "", util.QueryActionCreate:
			cr.createKubernetesObject(w, r)
		case util.QueryActionDelete:
			cr.deleteKubernetesObject(w, r)
		default:
			common.RenderErr(w, ctx.RequestID, common.ActionNotAllowed)
		}
	})
	r.Get("/cluster/{clusterName}/k8s/object", cr.getKubernetesObject)
	r.Put("/cluster/{clusterName}/k8s/object", cr.updateKubernetesObject)
}

// 创建集群
func (cr *ClusterRouter) createCluster(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var request cluster.CreateClusterRequest

	if err := common.BindJSON(r, &request); err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"create cluster failed parsing request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response, err := cluster.CreateCluster(&ctx, &request)
	if err != nil {
		ctx.Logging().Errorf(
			"create cluster failed. clusterRequest:%v error:%s", request, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("CreateCluster cluster:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

// 获取集群列表
func (cr *ClusterRouter) listCluster(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	marker := r.URL.Query().Get(util.QueryKeyMarker)
	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}

	var clusterNameList []string
	clusterNames := strings.TrimSpace(r.URL.Query().Get(util.ParamKeyClusterNames))
	if clusterNames != "" {
		clusterNameList = common.SplitString(clusterNames, ",")
	}

	clusterStatus := strings.TrimSpace(r.URL.Query().Get(util.ParamKeyClusterStatus))
	ctx.Logging().Infof("listCluster marker: %s, maxKeys: %d, clusterNames: %s, clusterStatus: %s",
		marker, maxKeys, clusterNames, clusterStatus)

	response, err := cluster.ListCluster(&ctx, marker, maxKeys, clusterNameList, clusterStatus)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}

	common.Render(w, http.StatusOK, response)
}

// 获取集群详情
func (cr *ClusterRouter) getClusterDetail(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	clusterName := strings.TrimSpace(chi.URLParam(r, util.ParamKeyClusterName))

	response, err := cluster.GetCluster(&ctx, clusterName)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}

	common.Render(w, http.StatusOK, response)
}

// 删除集群
func (cr *ClusterRouter) deleteCluster(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	clusterName := strings.TrimSpace(chi.URLParam(r, util.ParamKeyClusterName))

	if err := cluster.DeleteCluster(&ctx, clusterName); err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}

	common.RenderStatus(w, http.StatusOK)
}

// 修改集群
func (cr *ClusterRouter) updateCluster(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	clusterName := strings.TrimSpace(chi.URLParam(r, util.ParamKeyClusterName))
	var request cluster.UpdateClusterRequest

	if err := common.BindJSON(r, &request); err != nil {
		logger.LoggerForRequest(&ctx).Errorf(
			"update cluster failed parsing request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response, err := cluster.UpdateCluster(&ctx, clusterName, &request)
	if err != nil {
		ctx.Logging().Errorf("update cluster failed. clusterName:%s, error:%s", clusterName, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
		return
	}
	common.Render(w, http.StatusOK, response)
}

/* 返回集群quota信息
返回示例:
{
  "shared-gpu-cluster": {
    "summary": {
      "total": {
        "cpu": 16,
        "memory": "64422.83Mi",
        "storage": "445762.49Mi",
        "scalarResources": {
          "nvidia.com/gpu": "8"
        }
      },
      "idle": {
        "cpu": 11,
        "memory": "53832.83Mi",
        "storage": "445762.49Mi",
        "scalarResources": {
          "nvidia.com/gpu": "8"
        }
      }
    },
    "nodeList": [
      {
        "nodeName": "nodeName",
        "schedulable": true,
        "total": {
          "cpu": 16,
          "memory": "64422.83Mi",
          "storage": "445762.49Mi",
          "scalarResources": {
            "nvidia.com/gpu": "8"
          }
        },
        "idle": {
          "cpu": 11,
          "memory": "53832.83Mi",
          "storage": "445762.49Mi",
          "scalarResources": {
            "nvidia.com/gpu": "8"
          }
        }
      }
    ],
    "errMsg": ""
  },
  "cluster_test": {
    "nodeList": [],
    "errMsg": "clusterName: cluster_test, errorMsg: yaml: invalid trailing UTF-8 octet"
  }
}
*/
func (cr *ClusterRouter) listClusterQuota(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	clusterNames := strings.TrimSpace(r.URL.Query().Get(util.ParamKeyClusterNames))

	var clusterNameList []string
	if clusterNames != "" {
		clusterNameList = common.SplitString(clusterNames, ",")
	}

	quotaList, err := cluster.ListClusterQuota(&ctx, clusterNameList)
	if err != nil {
		ctx.Logging().Errorf("list cluster quota failed, error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, quotaList)
}

// listClusterQuotaV2
// @Summary 获取集群资源列表
// @Description 获取集群资源列表,可按标签进行过滤统计
// @Id listClusterQuotaV2
// @tags Resource
// @Accept  json
// @Produce json
// @Param listClusterResourcesRequest body ListClusterResourcesRequest true  "获取集群资源列表"
// @Request  ListClusterResourcesRequest
// @Success 200 {object} []ClusterQuotaResponse "获取可筛选标签查询的集群资源的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /cluster/resource [POST]
func (cr *ClusterRouter) listClusterQuotaV2(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var listClusterResourcesRequest cluster.ListClusterResourcesRequest
	if err := common.BindJSON(r, &listClusterResourcesRequest); err != nil {
		ctx.ErrorCode = common.MalformedJSON
		ctx.Logging().Errorf("parsing request body failed:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	log.Debugf("list cluster resources request:%+v", listClusterResourcesRequest)

	if err := validateListClusterRequest(&listClusterResourcesRequest); err != nil {
		ctx.ErrorCode = common.InvalidHTTPRequest
		ctx.Logging().Errorf("parsing request body failed:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	quotaList, err := cluster.ListClusterResources(&ctx, listClusterResourcesRequest)
	if err != nil {
		ctx.Logging().Errorf("list cluster quota failed, error:%s", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, quotaList)
}

func validateListClusterRequest(request *cluster.ListClusterResourcesRequest) error {
	if request.PageSize <= 0 {
		log.Warningf("pageSize is %d, set to default value", request.PageSize)
		request.PageSize = util.ListPageMax
	}
	if request.PageNo <= 0 {
		log.Warningf("PageNo is %d, set to default value", request.PageSize)
		request.PageNo = 1
	}
	return nil
}

func (cr *ClusterRouter) createKubernetesObject(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	clusterName := chi.URLParam(r, util.ParamKeyClusterName)
	ctx.Logging().Infof("create object on cluster: %s", clusterName)

	obj := make(map[string]interface{})
	if err := common.BindJSON(r, &obj); err != nil {
		ctx.Logging().Errorf(
			"create cluster object failed parsing request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	request := cluster.ObjectRequest{
		ClusterName: clusterName,
		Object:      obj,
	}
	err := cluster.CreateOrDeleteClusterObject(&ctx, request, util.QueryActionCreate)
	if err != nil {
		ctx.Logging().Errorf("create cluster object failed, err: %v", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("create cluster object: %s", string(config.PrettyFormat(obj)))
	common.Render(w, http.StatusOK, obj)
}

func (cr *ClusterRouter) updateKubernetesObject(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	clusterName := chi.URLParam(r, util.ParamKeyClusterName)
	ctx.Logging().Infof("update object on cluster: %s", clusterName)

	obj := make(map[string]interface{})
	if err := common.BindJSON(r, &obj); err != nil {
		ctx.Logging().Errorf(
			"update cluster object failed parsing request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err := cluster.UpdateClusterObject(&ctx, clusterName, obj)
	if err != nil {
		ctx.Logging().Errorf("update cluster object failed, err: %v", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	ctx.Logging().Debugf("update object on cluster: %s", string(config.PrettyFormat(obj)))
	common.Render(w, http.StatusOK, obj)
}

func (cr *ClusterRouter) getKubernetesObject(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	clusterName := chi.URLParam(r, util.ParamKeyClusterName)
	name := r.URL.Query().Get(util.ParamKeyName)
	namespace := r.URL.Query().Get(util.ParamKeyNamespace)
	kind := r.URL.Query().Get(util.ParamKeyKind)
	apiVersion := r.URL.Query().Get(util.ParamKeyAPIVersion)

	ctx.Logging().Infof("get object from cluster: %s", clusterName)
	request := cluster.ObjectRequest{
		ClusterName: clusterName,
		Name:        name,
		Namespace:   namespace,
		Kind:        kind,
		APIVersion:  apiVersion,
	}
	if err := validateObjectRequest(&request); err != nil {
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorf("get cluster object request validate failed, err: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	ctx.Logging().Debugf("get cluster object %s from cluster %s", string(config.PrettyFormat(request)), clusterName)
	obj, err := cluster.GetClusterObject(&ctx, &request)
	if err != nil {
		ctx.Logging().Errorf("get cluster object failed, err: %v", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, obj)
}

func (cr *ClusterRouter) deleteKubernetesObject(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var request cluster.ObjectRequest

	clusterName := chi.URLParam(r, util.ParamKeyClusterName)
	ctx.Logging().Infof("delete object from cluster: %s", clusterName)

	if err := common.BindJSON(r, &request); err != nil {
		ctx.Logging().Errorf(
			"delete cluster object failed parsing request body:%+v. error:%s", r.Body, err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	request.ClusterName = clusterName

	if err := validateObjectRequest(&request); err != nil {
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorf("validate delete cluster object request failed, err: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	ctx.Logging().Infof("delete cluster object: %s", string(config.PrettyFormat(request)))
	if err := cluster.CreateOrDeleteClusterObject(&ctx, request, util.QueryActionDelete); err != nil {
		ctx.Logging().Errorf("delete cluster object failed, err: %v", err.Error())
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

func validateObjectRequest(req *cluster.ObjectRequest) error {
	if len(req.Name) == 0 {
		return fmt.Errorf("name is required in request")
	}
	if len(req.APIVersion) == 0 {
		return fmt.Errorf("apiVersion is required in request")
	}
	if len(req.Kind) == 0 {
		return fmt.Errorf("kind is required in request")
	}
	gv, err := k8sschema.ParseGroupVersion(req.APIVersion)
	if err != nil {
		return fmt.Errorf("apiVersion is invalid, err: %v", err)
	}
	req.GroupVersionKind = gv.WithKind(req.Kind)
	return nil
}
