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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func TestCacheLabel(t *testing.T) {
	initMockCache()

	mockObjectID := "test-node-id"
	err := LabelCache.AddLabel(&model.LabelInfo{
		ID:         "test-label-id",
		Name:       "xxx/queue-name",
		Value:      "default-queue",
		ObjectID:   mockObjectID,
		ObjectType: model.ObjectTypeNode,
	})
	assert.Equal(t, nil, err)

	LabelCache.DeleteLabel(mockObjectID, model.ObjectTypeNode)
	assert.Equal(t, nil, err)
}
