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

// JobBuilder defines job interface
// kubeJobBuilders store JobBuilder
type JobBuilder = func(RuntimeClientInterface) JobInterface

var kubeJobMutex sync.RWMutex
var kubeJobBuilders = map[string]JobBuilder{}

func RegisterJobBuilder(runtimeType string, frameworkVersion schema.FrameworkVersion, job JobBuilder) {
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.Lock()
		defer kubeJobMutex.Unlock()
		kubeJobBuilders[frameworkVersion.String()] = job
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
}

func CleanupJobBuilders(runtimeType string) {
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.Lock()
		defer kubeJobMutex.Unlock()
		kubeJobBuilders = map[string]JobBuilder{}
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
}

func GetJobBuilder(runtimeType string, frameworkVersion schema.FrameworkVersion) (JobBuilder, bool) {
	var jobBuilder JobBuilder
	var found bool
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.RLock()
		defer kubeJobMutex.RUnlock()
		jobBuilder, found = kubeJobBuilders[frameworkVersion.String()]
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
	return jobBuilder, found
}

// Queue register

// QueuePlugin defines queue interface
type QueuePlugin = func(RuntimeClientInterface) QueueInterface

var queueMutex sync.RWMutex
var queueMaps = map[string]QueuePlugin{}

func RegisterQueuePlugin(runtimeType string, quotaType schema.FrameworkVersion, queue QueuePlugin) {
	switch runtimeType {
	case schema.KubernetesType:
		queueMutex.Lock()
		defer queueMutex.Unlock()
		queueMaps[quotaType.String()] = queue
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
}

func CleanupQueuePlugin(runtimeType string) {
	switch runtimeType {
	case schema.KubernetesType:
		queueMutex.Lock()
		defer queueMutex.Unlock()
		queueMaps = map[string]QueuePlugin{}
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
		queuePlugin, found = queueMaps[quotaType.String()]
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
	return queuePlugin, found
}
