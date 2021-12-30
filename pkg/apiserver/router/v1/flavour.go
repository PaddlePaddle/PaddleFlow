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

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/common/config"
)

type FlavourRouter struct{}

func (fr *FlavourRouter) Name() string {
	return "FlavourRouter"
}

func (fr *FlavourRouter) AddRouter(r chi.Router) {
	log.Info("add flavour router")
	r.Get("/flavour", fr.listFlavour)
}

// listFlavour
// @Summary 获取套餐列表
// @Description 获取套餐列表
// @Id listFlavour
// @tags User
// @Accept  json
// @Produce json
// @Success 200 {object} []schema.Flavour "获取套餐列表的响应"
// @Failure 400 {object} common.ErrorResponse "400"
// @Router /flavour [GET]
func (fr *FlavourRouter) listFlavour(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	if config.GlobalServerConfig == nil || len(config.GlobalServerConfig.Flavour) == 0 {
		common.RenderErr(w, ctx.RequestID, common.FlavourNotFound)
		return
	}
	common.Render(w, http.StatusOK, config.GlobalServerConfig.Flavour)
}
