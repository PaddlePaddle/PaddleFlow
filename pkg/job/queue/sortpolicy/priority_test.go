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

package sortpolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
)

func TestQueuePriority(t *testing.T) {

	testCases := []struct {
		name string
		arg  api.Arguments
		l    *api.PFJob
		r    *api.PFJob
		ans  int
	}{
		{
			name: "left is less than right",
			arg: api.Arguments{
				"a": "b",
			},
			l:   &api.PFJob{Priority: 10},
			r:   &api.PFJob{Priority: 20},
			ans: 1,
		},
		{
			name: "left is larger than right",
			arg: api.Arguments{
				"a": "b",
			},
			l:   &api.PFJob{Priority: 200},
			r:   &api.PFJob{Priority: 20},
			ans: -1,
		},
		{
			name: "left is equal to right",
			arg: api.Arguments{
				"a": "b",
			},
			l:   &api.PFJob{Priority: 20},
			r:   &api.PFJob{Priority: 20},
			ans: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sp, err := PriorityPolicyNew(tc.arg)
			t.Logf("run %s", sp.Name())
			assert.Equal(t, nil, err)
			ans := sp.OrderFn(tc.l, tc.r)
			assert.Equal(t, tc.ans, ans)
		})
	}
}
