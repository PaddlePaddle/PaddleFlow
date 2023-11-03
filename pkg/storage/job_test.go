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

package storage

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestListJob(t *testing.T) {
	initMockDB()
	time1 := time.Now()
	testParentID := "test-parent-id"
	testUser1 := "user1"
	job1 := model.Job{
		ID:        "job-00001",
		Name:      "test-job-1",
		UserName:  "user1",
		QueueID:   "test-queue-1",
		Status:    schema.StatusJobRunning,
		ParentJob: testParentID,
		CreatedAt: time1,
		UpdatedAt: time1.Add(2 * time.Second),
		ActivatedAt: sql.NullTime{
			Time:  time1.Add(2 * time.Second),
			Valid: true,
		},
	}
	err := Job.CreateJob(&job1)
	if err != nil {
		t.Error(err)
	}
	assert.NotEmpty(t, job1.ID)

	testCases := []struct {
		name   string
		filter JobFilter
		jobLen int
		err    error
	}{
		{
			name: "list job with labels",
			filter: JobFilter{
				Labels: map[string]string{
					"a": "b",
				},
			},
			jobLen: 0,
			err:    nil,
		},
		{
			name: "list job with UpdateTime",
			filter: JobFilter{
				UpdateTime: time1.Format(model.TimeFormat),
				MaxKeys:    10,
			},
			jobLen: 1,
			err:    nil,
		},
		{
			name: "list job with StartTime",
			filter: JobFilter{
				StartTime: time1.Add(-time.Second).Format(model.TimeFormat),
				Order:     "DESC",
			},
			jobLen: 1,
			err:    nil,
		},
		{
			name: "list job with parent job",
			filter: JobFilter{
				ParentID: testParentID,
			},
			jobLen: 1,
			err:    nil,
		},
		{
			name: "list job with user",
			filter: JobFilter{
				User: testUser1,
			},
			jobLen: 1,
			err:    nil,
		},
		{
			name: "list job with pk",
			filter: JobFilter{
				PK: 1,
			},
			jobLen: 0,
			err:    nil,
		},
		{
			name: "list job with pk",
			filter: JobFilter{
				PK: 1,
			},
			jobLen: 0,
			err:    nil,
		},
		{
			name: "list job with pk 0",
			filter: JobFilter{
				PK:      0,
				MaxKeys: 50,
				User:    "root",
				Order:   "desc",
			},
			jobLen: 1,
			err:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jobs, err := Job.ListJob(tc.filter)
			assert.Equal(t, tc.err, err)
			assert.Equal(t, tc.jobLen, len(jobs))
			t.Logf("case %s, jobs: %v", tc.name, jobs)
		})
	}

}
