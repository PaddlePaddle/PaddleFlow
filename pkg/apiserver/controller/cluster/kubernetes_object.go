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

package cluster

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	commomschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type ObjectRequest struct {
	ClusterName      string                  `json:"-"`
	GroupVersionKind schema.GroupVersionKind `json:"-"`
	Object           map[string]interface{}  `json:"-"`
	Name             string                  `json:"name"`
	Namespace        string                  `json:"namespace"`
	Kind             string                  `json:"kind"`
	APIVersion       string                  `json:"apiVersion"`
}

func CreateOrDeleteClusterObject(ctx *logger.RequestContext, request ObjectRequest, action string) error {
	//  TODO: add permission check
	clusterInfo, err := storage.Cluster.GetClusterByName(request.ClusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNameNotFound
		ctx.Logging().Errorf("get cluster failed. clusterName:[%s]", request.ClusterName)
		return err
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		return err
	}

	if clusterInfo.ClusterType == commomschema.KubernetesType {
		k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
		switch action {
		case "", util.QueryActionCreate:
			obj := &unstructured.Unstructured{
				Object: request.Object,
			}
			if err = k8sRuntime.CreateObject(obj); err != nil {
				ctx.ErrorCode = common.InternalError
				ctx.Logging().Errorf("creatae resource on cluster %s failed, err: %v", request.ClusterName, err)
				return err
			}
		case util.QueryActionDelete:
			err = k8sRuntime.DeleteObject(request.Namespace, request.Name, request.GroupVersionKind)
			if err != nil {
				ctx.ErrorCode = common.InternalError
				ctx.Logging().Errorf("delete resource from cluster %s failed, err: %v", request.ClusterName, err)
				return err
			}
		default:
			ctx.ErrorCode = common.ActionNotAllowed
			return fmt.Errorf("action %s is not allowed", action)
		}
	} else {
		ctx.ErrorCode = common.ActionNotAllowed
		return fmt.Errorf("action is not allowed in clusterType %s", clusterInfo.ClusterType)
	}
	return nil
}

func UpdateClusterObject(ctx *logger.RequestContext, clusterName string, clusterObject map[string]interface{}) error {
	//  TODO: add permission check
	clusterInfo, err := storage.Cluster.GetClusterByName(clusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNameNotFound
		ctx.Logging().Errorf("get cluster failed. clusterName:[%s]", clusterName)
		return err
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		return err
	}

	switch clusterInfo.ClusterType {
	case commomschema.KubernetesType:
		k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
		obj := &unstructured.Unstructured{
			Object: clusterObject,
		}
		if err = k8sRuntime.UpdateObject(obj); err != nil {
			ctx.ErrorCode = common.InternalError
			ctx.Logging().Errorf("creatae resource on cluster %s failed, err: %v", clusterName, err)
			return err
		}
		return nil
	default:
		ctx.ErrorCode = common.ActionNotAllowed
		return fmt.Errorf("action is not allowed in clusterType %s", clusterInfo.ClusterType)
	}
}

func GetClusterObject(ctx *logger.RequestContext, request *ObjectRequest) (interface{}, error) {
	//  TODO: add permission check
	clusterInfo, err := storage.Cluster.GetClusterByName(request.ClusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNameNotFound
		ctx.Logging().Errorf("get cluster %s failed. err: %v", request.ClusterName, err)
		return nil, err
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("get cluster %s runtime failed. err: %v", request.ClusterName, err)
		return nil, err
	}

	switch clusterInfo.ClusterType {
	case commomschema.KubernetesType:
		k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
		obj, err := k8sRuntime.GetObject(request.Namespace, request.Name, request.GroupVersionKind)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			ctx.Logging().Errorf("get resource from cluster %s failed, err: %v", request.ClusterName, err)
			return nil, err
		}
		return obj, nil
	default:
		ctx.ErrorCode = common.ActionNotAllowed
		return nil, fmt.Errorf("action is not allowed in clusterType %s", clusterInfo.ClusterType)
	}
}
