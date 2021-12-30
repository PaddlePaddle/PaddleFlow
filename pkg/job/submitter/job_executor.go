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

package submitter

import (
	"context"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"paddleflow/pkg/common/k8s"
)

type JobExecutorInterface interface {
	StartJob(job interface{}, gvk schema.GroupVersionKind) error
	StopJob(namespace string, name string, gvk schema.GroupVersionKind) error
}

var JobExecutor JobExecutorInterface

func Init(config *rest.Config) error {
	executor, err := NewSingleClusterJobExecutor(config)
	if err != nil {
		log.Errorf("new single cluster job executor failed, err %v", err)
		return err
	}
	JobExecutor = executor
	return nil
}

type SingleClusterJobExecutor struct {
	dynamicClient dynamic.Interface
}

func NewSingleClusterJobExecutor(config *rest.Config) (JobExecutorInterface, error) {
	// Prepare the dynamic client
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Errorf("Init dynamic client failed: [%v]", err)
		return nil, err
	}

	executor := SingleClusterJobExecutor{
		dynamicClient: dynamicClient,
	}
	return &executor, nil
}

func (executor *SingleClusterJobExecutor) StartJob(job interface{}, gvk schema.GroupVersionKind) error {
	log.Debugf("job executor begin start job")
	gvr, err := k8s.GetGVRByGVK(gvk)
	if err != nil {
		return err
	}

	newJob, err := runtime.DefaultUnstructuredConverter.ToUnstructured(job)
	if err != nil {
		return err
	}

	obj := &unstructured.Unstructured{
		Object: newJob,
	}
	obj.SetKind(gvk.Kind)
	obj.SetAPIVersion(gvk.GroupVersion().String())
	// Create the object with dynamic client
	_, err = executor.dynamicClient.Resource(gvr).Namespace(obj.GetNamespace()).Create(context.TODO(), obj, v1.CreateOptions{})
	if err != nil {
		log.Errorf("start job failed. error:[%s]", err.Error())
	}
	return err
}

func (executor *SingleClusterJobExecutor) StopJob(namespace string, name string, gvk schema.GroupVersionKind) error {
	log.Debugf("job executor begin stop job. ns:[%s] name:[%s]", namespace, name)
	propagationPolicy := v1.DeletePropagationBackground
	deleteOptions := v1.DeleteOptions{
		PropagationPolicy: &propagationPolicy,
	}
	gvr, err := k8s.GetGVRByGVK(gvk)
	if err != nil {
		return err
	}
	err = executor.dynamicClient.Resource(gvr).Namespace(namespace).Delete(context.TODO(), name, deleteOptions)
	if err != nil {
		log.Errorf("stop job failed. error:[%s]", err.Error())
	}
	return err
}
