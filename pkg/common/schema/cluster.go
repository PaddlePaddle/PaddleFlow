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

package schema

const (
	LocalType      = "Local"
	KubernetesType = "Kubernetes"
)

// ClientOptions used to build rest config.
type ClientOptions struct {
	Master string
	Config string
	QPS    float32
	Burst  int
}

type Cluster struct {
	ID   string
	Name string
	Type string

	// ClientOpt defines client config for cluster
	ClientOpt ClientOptions
}
