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

package common

import (
	"encoding/json"
	"net/http"

	log "github.com/sirupsen/logrus"
)

func RenderErr(w http.ResponseWriter, requestID string, code string) {
	httpCode := GetHttpStatusByCode(code)
	message := GetMessageByCode(code)
	errorResponse := ErrorResponse{
		RequestID:    requestID,
		ErrorCode:    code,
		ErrorMessage: message,
	}
	Render(w, httpCode, errorResponse)
}

func RenderErrWithMessage(w http.ResponseWriter, requestID string, code string, message string) {
	httpCode := GetHttpStatusByCode(code)
	if message == "" {
		message = GetMessageByCode(code)
	}
	errorResponse := ErrorResponse{
		RequestID:    requestID,
		ErrorCode:    code,
		ErrorMessage: message,
	}
	// code没有设置对应的http状态码
	if httpCode == 0 {
		httpCode = http.StatusInternalServerError
	}
	Render(w, httpCode, errorResponse)
}

func RenderStatus(w http.ResponseWriter, httpCode int) {
	Render(w, httpCode, nil)
}

func Render(w http.ResponseWriter, httpCode int, data interface{}) {
	w.WriteHeader(httpCode)
	if data != nil {
		jsonBytes, err := json.Marshal(data)
		if err != nil {
			log.Errorf("Render response requestID[%s],err[%s]", w.Header().Get(HeaderKeyRequestID), err.Error())
		}
		w.Write(jsonBytes)
	}
}
