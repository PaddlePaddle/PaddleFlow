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

package executor

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
)

func Get(namespace string, name string, gvk schema.GroupVersionKind, clientOpt *k8s.DynamicClientOption) (*unstructured.Unstructured, error) {
	log.Debugf("executor begin to get kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if clientOpt == nil {
		return nil, fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := clientOpt.GetGVR(gvk)
	if err != nil {
		return nil, err
	}
	var obj *unstructured.Unstructured
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		obj, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Get(context.TODO(), name, v1.GetOptions{})
	} else {
		obj, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Get(context.TODO(), name, v1.GetOptions{})
	}
	if err != nil {
		log.Errorf("get kubernetes %s resource[%s/%s] failed. error:[%s]", gvk.String(), namespace, name, err.Error())
	}
	return obj, err
}

func Create(resource interface{}, gvk schema.GroupVersionKind, clientOpt *k8s.DynamicClientOption) error {
	log.Debugf("executor begin to create kuberentes resource[%s]", gvk.String())
	if clientOpt == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := clientOpt.GetGVR(gvk)
	if err != nil {
		return err
	}

	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	obj.SetKind(gvk.Kind)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	// Create the object with dynamic client
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Namespace(obj.GetNamespace()).Create(context.TODO(), obj, v1.CreateOptions{})
	} else {
		_, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Create(context.TODO(), obj, v1.CreateOptions{})
	}
	if err != nil {
		log.Errorf("create kuberentes resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}

func Delete(namespace string, name string, gvk schema.GroupVersionKind, clientOpt *k8s.DynamicClientOption) error {
	log.Debugf("executor begin to delete kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if clientOpt == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	propagationPolicy := v1.DeletePropagationBackground
	deleteOptions := v1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}
	gvrMap, err := clientOpt.GetGVR(gvk)
	if err != nil {
		return err
	}
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Delete(context.TODO(), name, deleteOptions)
	} else {
		err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Delete(context.TODO(), name, deleteOptions)
	}
	if err != nil {
		log.Errorf("delete kubernetes  resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}

func Patch(namespace, name string, gvk schema.GroupVersionKind, data []byte, clientOpt *k8s.DynamicClientOption) error {
	log.Debugf("executor begin to patch kubernetes resource[%s]. ns:[%s] name:[%s]", gvk.String(), namespace, name)
	if clientOpt == nil {
		return fmt.Errorf("dynamic client is nil")
	}

	patchType := types.StrategicMergePatchType
	patchOptions := v1.PatchOptions{}
	gvrMap, err := clientOpt.GetGVR(gvk)
	if err != nil {
		return err
	}
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Namespace(namespace).Patch(context.TODO(), name, patchType, data, patchOptions)
	} else {
		_, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Patch(context.TODO(), name, patchType, data, patchOptions)
	}
	if err != nil {
		log.Errorf("patch kubernetes resource: %s failed. error: %s", gvk.String(), err.Error())
	}
	return err
}

func Update(resource interface{}, gvk schema.GroupVersionKind, clientOpt *k8s.DynamicClientOption) error {
	log.Debugf("executor begin to update kubernetes resource[%s]", gvk.String())
	if clientOpt == nil {
		return fmt.Errorf("dynamic client is nil")
	}
	gvrMap, err := clientOpt.GetGVR(gvk)
	if err != nil {
		return err
	}

	newResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
	if err != nil {
		log.Errorf("convert to unstructured failed, err: %v", err)
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newResource,
	}
	obj.SetKind(gvk.Kind)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	// Create the object with dynamic client
	if gvrMap.Scope.Name() == meta.RESTScopeNameNamespace {
		_, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Namespace(obj.GetNamespace()).Update(context.TODO(), obj, v1.UpdateOptions{})
	} else {
		_, err = clientOpt.DynamicClient.Resource(gvrMap.Resource).Update(context.TODO(), obj, v1.UpdateOptions{})
	}
	if err != nil {
		log.Errorf("update kuberentes resource[%s] failed. error:[%s]", gvk.String(), err.Error())
	}
	return err
}
