/*
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

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

package vfs

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"paddleflow/pkg/fs/client/base"
)

func TestInitVFS(t *testing.T) {
	fsMeta := base.FSMeta{
		UfsType: base.LocalType,
		Properties: map[string]string{
			base.RootKey: "./test/",
		},
		SubPath: "./test/",
	}
	InitOldVFS(fsMeta, nil, true)
	assert.NotNil(t, GetOldVFS())
}
