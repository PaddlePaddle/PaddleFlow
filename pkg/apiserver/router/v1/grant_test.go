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

package v1

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
)

func TestGrantRouter(t *testing.T) {
	r := DebugChiRouter()
	// test create grant
	testServer := httptest.NewServer(r)
	grant := models.Grant{UserName: mockUserName, ResourceType: "mockRT"}
	req := NewHttpRecorder(testServer.URL, "/grant", http.MethodPost, grant)
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, res.StatusCode, 403)
	// bind json error
	req = NewHttpRecorder(testServer.URL, "/grant", http.MethodPost, "bindJsonTest")
	res, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, res.StatusCode, 400)
}
