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

package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
)

func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.Request.Header.Get(common.HeaderKeyRequestID)
		if requestID == "" {
			requestID = uuid.NewString()
			log.Debugf("request is null, generate request-id:%s", requestID)
		}
		c.Request.Header.Set(common.HeaderKeyRequestID, requestID)
		c.Writer.Header().Set(common.HeaderKeyRequestID, requestID)
		c.Next()
	}
}

func CheckRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requestID := req.Header.Get(common.HeaderKeyRequestID)
		if requestID == "" {
			requestID = uuid.NewString()
			log.Debugf("request is null, generate request-id:%s", requestID)
		}
		req.Header.Set(common.HeaderKeyRequestID, requestID)
		w.Header().Add(common.HeaderKeyRequestID, requestID)
		w.Header().Add("Content-Type", "application/json")
		next.ServeHTTP(w, req)
	})
}

func NotFound(w http.ResponseWriter, req *http.Request) {
	common.RenderErr(w, req.Header.Get(common.HeaderKeyRequestID), common.PathNotFound)
}
func MethodNotAllowed(w http.ResponseWriter, req *http.Request) {
	common.RenderErr(w, req.Header.Get(common.HeaderKeyRequestID), common.MethodNotAllowed)
}
