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
	"github.com/go-chi/chi"
	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/middleware"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
)

type IRouter interface {
	Name() string
	AddRouter(r chi.Router)
}

// @title PaddleFlow API
// @version 1.0
// @description This is PaddleFLow server.

// @contact.name paddleflow
// @contact.url http://www.paddlepaddle.org.cn
// @contact.email paddleflow@baidu.com

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host paddlepaddle.org.cn
// @BasePath /api/paddleflow/v1

func RegisterRouters(r *chi.Mux, debugMode bool) {
	r.Use(middleware.CheckRequestID)
	r.NotFound(middleware.NotFound)
	r.MethodNotAllowed(middleware.MethodNotAllowed)
	r.Use(middleware.Recoverer)
	// route group
	pathPrefix := util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1
	r.Route(pathPrefix, func(apiV1Router chi.Router) {
		if !debugMode {
			apiV1Router.Use(middleware.BaseAuth)
		}
		AddRouter(apiV1Router, &GrantRouter{})
		AddRouter(apiV1Router, &QueueRouter{})
		AddRouter(apiV1Router, &FlavourRouter{})
		AddRouter(apiV1Router, &RunRouter{})
		AddRouter(apiV1Router, &PipelineRouter{})
		AddRouter(apiV1Router, &ScheduleRouter{})
		AddRouter(apiV1Router, &UserRouter{})
		AddRouter(apiV1Router, &LinkRouter{})
		AddRouter(apiV1Router, &PFSRouter{})
		AddRouter(apiV1Router, &ClusterRouter{})
		AddRouter(apiV1Router, &TrackRouter{})
		AddRouter(apiV1Router, &LogRouter{})
		AddRouter(apiV1Router, &JobRouter{})
		AddRouter(apiV1Router, &StatisticsRouter{})
	})
}

func AddRouter(r chi.Router, router IRouter) {
	logrus.Infof("Add router[%s]", router.Name())
	router.AddRouter(r)
}
