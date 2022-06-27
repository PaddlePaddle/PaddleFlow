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

const (
	EACCESS_DENIED        = "AccessDenied"
	EINTERNAL_ERROR       = "InternalError"
	EINVALID_HTTP_REQUEST = "InvalidHTTPRequest"
	EMALFORMED_JSON       = "MalformedJSON"
	EPRECONDITION_FAILED  = "PreconditionFailed"
)

type PFServiceError struct {
	Code       string
	Message    string
	RequestId  string
	StatusCode int
}

func (b *PFServiceError) Error() string {
	ret := "[Code: " + b.Code
	ret += "; Message: " + b.Message
	ret += "; RequestID: " + b.RequestId + "]"
	return ret
}

func NewPFServiceError(code, msg, reqId string, status int) *PFServiceError {
	return &PFServiceError{code, msg, reqId, status}
}
