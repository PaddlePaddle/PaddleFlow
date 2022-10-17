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
	"net/http"

	"github.com/go-chi/chi"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/version"
)

type VersionRouter struct{}

func (ur *VersionRouter) Name() string {
	return "Version"
}

func (ur *VersionRouter) AddRouter(r chi.Router) {
	r.Get("/version", ur.getVersion)
}

func (ur *VersionRouter) getVersion(w http.ResponseWriter, r *http.Request) {
	common.Render(w, http.StatusOK, version.Info)
}
