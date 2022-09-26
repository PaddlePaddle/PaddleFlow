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
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

// QueueID is UID type, serves as unique ID for each queue
type QueueID string

type ClusterID string

// QueueInfo will have all details about queue
type QueueInfo struct {
	UID       QueueID
	Name      string
	Namespace string
	// ClusterID for queue
	ClusterID ClusterID
	Type      string
	Status    string

	// Priority for queue
	Priority int32
	Weight   int32

	// SortPolicy for queue job
	SortPolicyNames []string
	SortPolicies    []SortPolicy
	// Location for queue affinity
	Location map[string]string

	// SchedulerName for queue job
	SchedulerName string

	//
	Permissions []string

	// Resource range of queue
	MaxResources  *resources.Resource
	MinResources  *resources.Resource
	UsedResources *resources.Resource
}

func NewQueueInfo(q model.Queue) *QueueInfo {
	return &QueueInfo{
		UID:             QueueID(q.ID),
		Name:            q.Name,
		ClusterID:       ClusterID(q.ClusterId),
		Status:          q.Status,
		SortPolicyNames: q.SchedulingPolicy,
		SortPolicies:    NewRegistry(q.SchedulingPolicy),
		MaxResources:    q.MaxResources,
		MinResources:    q.MinResources,
		Location:        q.Location,
	}
}

func (q *QueueInfo) JobOrderFn(l, r interface{}) bool {
	for _, policy := range q.SortPolicies {
		if res := policy.OrderFn(l, r); res != 0 {
			return res < 0
		}
	}

	// If no job order funcs, order job by CreateTime first, then by UID.
	lv := l.(*PFJob)
	rv := r.(*PFJob)
	if lv.CreateTime.Equal(rv.CreateTime) {
		return lv.ID < rv.ID
	}
	return lv.CreateTime.Before(rv.CreateTime)
}

// QueueSyncInfo contains queue sync info
type QueueSyncInfo struct {
	Name        string
	Namespace   string
	Labels      map[string]string
	Status      string
	QuotaType   string
	MaxResource *resources.Resource
	MinResource *resources.Resource
	Action      schema.ActionType
	Message     string
	RetryTimes  int
}

type SortPolicy interface {
	Name() string
	OrderFn(interface{}, interface{}) int
}

// Arguments map
type Arguments map[string]string

// PolicyFactory is a function that builds a sort policy.
type PolicyFactory = func(configuration Arguments) (SortPolicy, error)

// Registry is a collection of all available sort polices.
type Registry map[string]PolicyFactory

// Register adds a new sort policy to the registry. If a sort policy with the same name
// exists, it returns an error.
func (r Registry) Register(name string, factory PolicyFactory) error {
	if _, ok := r[name]; ok {
		return fmt.Errorf("a sort policy named %v already exists", name)
	}
	r[name] = factory
	return nil
}

// Unregister removes an existing sort policy from the registry. If no sort policy with
// the provided name exists, it returns an error.
func (r Registry) Unregister(name string) error {
	if _, ok := r[name]; !ok {
		return fmt.Errorf("no sort policy named %v exists", name)
	}
	delete(r, name)
	return nil
}

// QueueSortPolicies global queue sort policies
var QueueSortPolicies = make(Registry)

// NewRegistry registry sort policy for queue
func NewRegistry(policyNames []string) []SortPolicy {
	var policies []SortPolicy

	for _, name := range policyNames {
		policyNew, find := QueueSortPolicies[name]
		if !find {
			logrus.Warningf("queue sort policy[%s] is not found.", name)
			continue
		}
		policy, err := policyNew(Arguments{})
		if err != nil {
			logrus.Warningf("new sort policy[%s] failed, err: %v", name, err)
			continue
		}
		policies = append(policies, policy)
	}
	return policies
}
