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

package queues

import (
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queues/elasticquota"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/queues/queue"
)

func init() {
	framework.RegisterQueue(pfschema.KubernetesType, elasticquota.KubeElasticQuotaType, elasticquota.New)
	framework.RegisterQueue(pfschema.KubernetesType, queue.KubeVCQueueQuotaType, queue.New)
	// TODO: add more plugin
}
