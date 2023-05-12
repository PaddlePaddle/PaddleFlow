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

	"github.com/stretchr/testify/assert"
)

func TestPFJob(t *testing.T) {
	tfjob := PFJob{
		ID:        "test-id",
		Name:      "test-name",
		Namespace: "default",
	}
	t.Run("test GetID func", func(t *testing.T) {
		assert.Equal(t, tfjob.ID, tfjob.GetID())
	})

	t.Run("test NamespacedName func", func(t *testing.T) {
		namespacedName := fmt.Sprintf("%s/%s", tfjob.Namespace, tfjob.ID)
		assert.Equal(t, namespacedName, tfjob.NamespacedName())
	})

	t.Run("test UpdateLabels", func(t *testing.T) {
		tfjob.UpdateLabels(map[string]string{
			"a": "b",
		})
		assert.Equal(t, "b", tfjob.Labels["a"])
	})

	t.Run("test UpdateAnnotations", func(t *testing.T) {
		tfjob.UpdateAnnotations(map[string]string{
			"paddleflow/xx": "b",
		})
		assert.Equal(t, "b", tfjob.Annotations["paddleflow/xx"])
	})

	t.Run("test UpdateJobPriority", func(t *testing.T) {
		newPC := "high"
		tfjob.UpdateJobPriority(newPC)
		assert.Equal(t, newPC, tfjob.PriorityClassName)
	})
}
