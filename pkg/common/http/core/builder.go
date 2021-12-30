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
	"fmt"
)

// RequestBuilder holds config data for bce request.
// Some of fields are required and the others are optional.
// The builder pattern can simplify the execution of requests.
type RequestBuilder struct {
	client Client

	url         string              // required
	method      string              // required
	queryParams map[string][]string // optional
	headers     map[string]string   // optional
	body        interface{}         // optional
	result      interface{}         // optional
}

// create RequestBuilder with the client.
func NewRequestBuilder(client Client) *RequestBuilder {
	return &RequestBuilder{
		client: client,
	}
}

func (b *RequestBuilder) WithURL(url string) *RequestBuilder {
	b.url = url
	return b
}

func (b *RequestBuilder) WithMethod(method string) *RequestBuilder {
	b.method = method
	return b
}

// set query param with the key/value directly.
func (b *RequestBuilder) WithQueryParam(key, value string) *RequestBuilder {
	if b.queryParams == nil {
		b.queryParams = make(map[string][]string)
	}
	if values, ok := b.queryParams[key]; ok {
		b.queryParams[key] = append(values, value)
	} else {
		b.queryParams[key] = []string{value}
	}
	return b
}

// set query param with the key/value only when the value is not blank.
func (b *RequestBuilder) WithQueryParamFilter(key, value string) *RequestBuilder {
	if len(value) == 0 {
		return b
	}
	return b.WithQueryParam(key, value)
}

func (b *RequestBuilder) WithQueryParams(params map[string][]string) *RequestBuilder {
	if b.queryParams == nil {
		b.queryParams = params
	} else {
		for key, value := range params {
			b.queryParams[key] = value
		}
	}
	return b
}

func (b *RequestBuilder) WithHeader(key, value string) *RequestBuilder {
	if b.headers == nil {
		b.headers = make(map[string]string)
	}
	b.headers[key] = value
	return b
}

func (b *RequestBuilder) WithHeaders(headers map[string]string) *RequestBuilder {
	if b.headers == nil {
		b.headers = headers
	} else {
		for key, value := range headers {
			b.headers[key] = value
		}
	}
	return b
}

func (b *RequestBuilder) WithBody(body interface{}) *RequestBuilder {
	b.body = body
	return b
}

func (b *RequestBuilder) WithResult(result interface{}) *RequestBuilder {
	b.result = result
	return b
}

// Do will send request to core and get result with the builder's parameters.
func (b *RequestBuilder) Do() error {
	if err := b.validate(); err != nil {
		return err
	}

	// build PFRequest
	req, err := b.buildPFRequest()
	if err != nil {
		return err
	}

	// get result from PFResponse
	if err := b.buildPFResponse(req); err != nil {
		return err
	}

	return nil
}

// Validate if the required fields are providered.
func (b *RequestBuilder) validate() error {
	if len(b.url) == 0 {
		return fmt.Errorf("The url can't be null.")
	}
	if len(b.method) == 0 {
		return fmt.Errorf("The method can't be null.")
	}
	if b.client == nil {
		return fmt.Errorf("The client can't be null.")
	}
	return nil
}

func (b *RequestBuilder) buildPFRequest() (*PFRequest, error) {
	// Build the request
	req := &PFRequest{}
	req.SetUri(b.url)
	req.SetMethod(b.method)

	if b.headers != nil {
		req.SetHeaders(b.headers)
	}
	if b.queryParams != nil {
		req.SetParams(b.queryParams)
	}
	if b.body != nil {
		body, err := NewRequestBodyWithStruct(b.body)
		if err != nil {
			return nil, err
		}
		req.SetBody(body)
	}

	return req, nil
}

func (b *RequestBuilder) buildPFResponse(req *PFRequest) error {
	// Send request and get response
	resp, err := b.client.SendRequest(req)
	if err != nil {
		return err
	}
	if resp.IsFail() {
		return resp.ServiceError()
	}
	defer resp.Body().Close()

	if b.result == nil {
		return nil
	}

	return resp.ParseJsonBody(b.result)
}
