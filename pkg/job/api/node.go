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

package api

import (
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type NodeSyncInfo struct {
	Name       string
	Status     string
	Capacity   map[string]string
	Labels     map[string]string
	Action     schema.ActionType
	RetryTimes int
}

type NodeTaskSyncInfo struct {
	ID         string
	Name       string
	NodeName   string
	Status     model.TaskAllocateStatus
	Resources  map[string]int64
	Labels     map[string]string
	Action     schema.ActionType
	RetryTimes int
}
