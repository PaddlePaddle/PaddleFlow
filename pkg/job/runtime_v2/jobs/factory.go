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

package jobs

import (
	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/framework"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/jobs/paddle"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/jobs/pytorch"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2/jobs/single"
)

func init() {
	// Plugins for Jobs
	framework.RegisterJobBuilder(pfschema.KubernetesType, single.KubeSingleFwVersion, single.New)
	framework.RegisterJobBuilder(pfschema.KubernetesType, paddle.KubePaddleFwVersion, paddle.New)
	framework.RegisterJobBuilder(pfschema.KubernetesType, pytorch.KubePyTorchFwVersion, pytorch.New)
	// TODO: add more plugins
}