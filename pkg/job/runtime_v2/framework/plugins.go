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

func RegisterJobBuilder(runtimeType, frameworkVersion string, job JobBuilder) {
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.Lock()
		defer kubeJobMutex.Unlock()
		kubeJobBuilders[frameworkVersion] = job
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

func GetJobBuilder(runtimeType, frameworkVersion string) (JobBuilder, bool) {
	var jobBuilder JobBuilder
	var found bool
	switch runtimeType {
	case schema.KubernetesType:
		kubeJobMutex.RLock()
		defer kubeJobMutex.RUnlock()
		jobBuilder, found = kubeJobBuilders[frameworkVersion]
	default:
		fmt.Printf("runtime type %s is not supported\n", runtimeType)
	}
	return jobBuilder, found
}
