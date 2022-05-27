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

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
)

type PFRequest struct {
	http.Request
	requestId string
}

func (b *PFRequest) RequestId() string {
	return b.requestId
}

func (b *PFRequest) SetRequestId(val string) {
	b.requestId = val
}

func (b *PFRequest) BuildHttpRequest() {
	if len(b.requestId) == 0 {
		// Construct the request ID with UUID
		b.requestId = util.NewRequestId()
	}
	b.SetHeader(BCE_REQUEST_ID, b.requestId)
	b.SetHeader(BCE_DATE, util.FormatISO8601Date(util.NowUTCSeconds()))
}

func (b *PFRequest) String() string {
	requestIdStr := "requestId=" + b.requestId
	return requestIdStr + "\n" + b.Request.String()
}

func NewRequestBodyWithStruct(body interface{}) (io.ReadCloser, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request body failed, error: %v, body: %v", err, body)
	}
	buf := bytes.NewBuffer(jsonBody)
	return ioutil.NopCloser(buf), nil
}
