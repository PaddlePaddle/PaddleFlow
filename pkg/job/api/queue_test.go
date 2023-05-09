/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueInfo_JobOrderFn(t *testing.T) {
	testCases := []struct {
		name    string
		left    *PFJob
		right   *PFJob
		wantAns bool
	}{
		{
			name: "left < right",
			left: &PFJob{
				CreateTime: time.Now(),
			},
			right: &PFJob{
				CreateTime: time.Now(),
			},
			wantAns: true,
		},
	}

	queueInfo := &QueueInfo{
		SortPolicies: []SortPolicy{},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ans := queueInfo.JobOrderFn(tc.left, tc.right)
			assert.Equal(t, tc.wantAns, ans)
		})
	}
}

func TestNewRegistry(t *testing.T) {
	r := make(Registry)

	pc := func(configuration Arguments) (SortPolicy, error) { return nil, nil }
	t.Run("test registry", func(t *testing.T) {
		err := r.Register("p1", pc)
		assert.Equal(t, nil, err)

		err = r.Register("p1", pc)
		assert.Equal(t, fmt.Errorf("a sort policy named p1 already exists"), err)

		err = r.Unregister("p1")
		assert.Equal(t, nil, err)

		err = r.Unregister("p2")
		assert.Equal(t, fmt.Errorf("no sort policy named p2 exists"), err)
	})

	t.Run("test new registry", func(t *testing.T) {
		QueueSortPolicies["priority"] = pc

		ans := NewRegistry([]string{"priority"})
		assert.Equal(t, 1, len(ans))
	})
}
