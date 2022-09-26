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

package framework

import (
	"fmt"
	"sync"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// JobPlugin Register

// JobPlugin defines job interface
type JobPlugin = func(RuntimeClientInterface) JobInterface

var kubeJobMutex sync.RWMutex

// kubeJobMaps store JobPlugin
var kubeJobMaps = map[schema.FrameworkVersion]JobPlugin{}

func RegisterJobPlugin(runtimeType string, frameworkVersion schema.FrameworkVersion, job JobPlugin) {
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.Lock()
		defer kubeJobMutex.Unlock()
		kubeJobMaps[frameworkVersion] = job
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
}

func CleanupJobPlugins(runtimeType string) {
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.Lock()
		defer kubeJobMutex.Unlock()
		kubeJobMaps = map[schema.FrameworkVersion]JobPlugin{}
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
}

func GetJobPlugin(runtimeType string, frameworkVersion schema.FrameworkVersion) (JobPlugin, bool) {
	var jobPlugin JobPlugin
	var found bool
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.RLock()
		defer kubeJobMutex.RUnlock()
		jobPlugin, found = kubeJobMaps[frameworkVersion]
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
	return jobPlugin, found
}

func ListJobPlugins(runtimeType string) map[schema.FrameworkVersion]JobPlugin {
	jobPlugin := make(map[schema.FrameworkVersion]JobPlugin)
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.RLock()
		defer kubeJobMutex.RUnlock()
		for fv, jp := range kubeJobMaps {
			jobPlugin[fv] = jp
		}
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
	return jobPlugin
}

// QueuePlugin register

// QueuePlugin defines queue interface
type QueuePlugin = func(RuntimeClientInterface) QueueInterface

var queueMutex sync.RWMutex
var queueMaps = map[schema.FrameworkVersion]QueuePlugin{}

func RegisterQueuePlugin(runtimeType string, quotaType schema.FrameworkVersion, queue QueuePlugin) {
	switch runtimeType {
	case schema.KubernetesType:
		queueMutex.Lock()
		defer queueMutex.Unlock()
		queueMaps[quotaType] = queue
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
}

func CleanupQueuePlugin(runtimeType string) {
	switch runtimeType {
	case schema.KubernetesType:
		queueMutex.Lock()
		defer queueMutex.Unlock()
		queueMaps = map[schema.FrameworkVersion]QueuePlugin{}
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
}

func GetQueuePlugin(runtimeType string, quotaType schema.FrameworkVersion) (QueuePlugin, bool) {
	var queuePlugin QueuePlugin
	var found bool
	switch runtimeType {
	case schema.KubernetesType:
		queueMutex.RLock()
		defer queueMutex.RUnlock()
		queuePlugin, found = queueMaps[quotaType]
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
	return queuePlugin, found
}

func ListQueuePlugins(runtimeType string) map[schema.FrameworkVersion]QueuePlugin {
	pluginMaps := make(map[schema.FrameworkVersion]QueuePlugin)
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.RLock()
		defer kubeJobMutex.RUnlock()
		for fv, plugin := range queueMaps {
			pluginMaps[fv] = plugin
		}
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
	return pluginMaps
}
