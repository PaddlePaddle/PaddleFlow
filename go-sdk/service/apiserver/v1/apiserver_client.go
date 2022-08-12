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

package v1

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
)

type APIV1Interface interface {
	UserGetter
	FileSystemGetter
	ClusterGetter
	QueueGetter
	FlavourGetter
	JobGetter
	RunGetter
	PipelineGetter
	ScheduleGetter
}

// APIV1Client is used to interact with features provided by the group.
type APIV1Client struct {
	restClient *core.PaddleFlowClient
}

func (c *APIV1Client) User() UserInterface {
	return newUsers(c)
}

func (c *APIV1Client) FileSystem() FileSystemInterface {
	return newFileSystem(c)
}

func (c *APIV1Client) Cluster() ClusterInterface {
	return newCluster(c)
}

func (c *APIV1Client) Queue() QueueInterface {
	return newQueue(c)
}

func (c *APIV1Client) Flavour() FlavourInterface {
	return newFlavour(c)
}

func (c *APIV1Client) Job() JobInterface {
	return newJob(c)
}

func (c *APIV1Client) Run() RunInterface {
	return newRun(c)
}

func (c *APIV1Client) Pipeline() PipelineInterface {
	return newPipeline(c)
}

func (c *APIV1Client) Schedule() ScheduleInterface {
	return newSchedule(c)
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *APIV1Client) RESTClient() *core.PaddleFlowClient {
	if c == nil {
		return nil
	}

	return c.restClient
}

func NewForConfig(config *core.PaddleFlowClientConfiguration) (*APIV1Client, error) {
	httpClient := core.NewPaddleFlowClient(config)
	return &APIV1Client{restClient: httpClient}, nil
}
