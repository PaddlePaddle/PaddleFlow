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

package job

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/argoworkflow"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/mpi"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/paddle"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/pytorch"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/ray"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/single"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/spark"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/tensorflow"
)

func init() {
	// Plugins for Jobs
	framework.RegisterJobPlugin(schema.KubernetesType, schema.StandaloneKindGroupVersion, single.New)
	framework.RegisterJobPlugin(schema.KubernetesType, schema.PaddleKindGroupVersion, paddle.New)
	framework.RegisterJobPlugin(schema.KubernetesType, schema.MPIKindGroupVersion, mpi.New)
	framework.RegisterJobPlugin(schema.KubernetesType, schema.PyTorchKindGroupVersion, pytorch.New)
	framework.RegisterJobPlugin(schema.KubernetesType, schema.TFKindGroupVersion, tensorflow.New)
	framework.RegisterJobPlugin(schema.KubernetesType, schema.SparkKindGroupVersion, spark.New)
	framework.RegisterJobPlugin(schema.KubernetesType, schema.RayKindGroupVersion, ray.New)
	framework.RegisterJobPlugin(schema.KubernetesType, schema.WorkflowKindGroupVersion, argoworkflow.New)
	// TODO: add more plugins
}
