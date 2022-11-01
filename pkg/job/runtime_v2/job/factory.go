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
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/argoworkflow"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/paddle"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/pytorch"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/ray"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/single"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/spark"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/job/tensorflow"
)

func init() {
	// Plugins for Jobs
	framework.RegisterJobPlugin(pfschema.KubernetesType, single.KubeSingleFwVersion, single.New)
	framework.RegisterJobPlugin(pfschema.KubernetesType, paddle.KubePaddleFwVersion, paddle.New)
	framework.RegisterJobPlugin(pfschema.KubernetesType, pytorch.KubePyTorchFwVersion, pytorch.New)
	framework.RegisterJobPlugin(pfschema.KubernetesType, tensorflow.KubeTFFwVersion, tensorflow.New)
	framework.RegisterJobPlugin(pfschema.KubernetesType, spark.KubeSparkFwVersion, spark.New)
	framework.RegisterJobPlugin(pfschema.KubernetesType, ray.KubeRayFwVersion, ray.New)
	framework.RegisterJobPlugin(pfschema.KubernetesType, argoworkflow.KubeArgoWorkflowFwVersion, argoworkflow.New)
	// TODO: add more plugins
}
