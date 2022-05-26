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

package http

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util"
)

type Request struct {
	host    string
	port    int
	method  string
	uri     string
	timeout int
	headers map[string]string
	params  map[string][]string
	body    io.ReadCloser
}

func (r *Request) Host() string {
	return r.host
}

func (r *Request) SetHost(host string) {
	r.host = host
	pos := strings.Index(host, ":")
	if pos != -1 {
		p, e := strconv.Atoi(host[pos+1:])
		if e == nil {
			r.port = p
		}
	}
}

func (r *Request) Port() int {
	return r.port
}

func (r *Request) SetPort(port int) {
	r.port = port
}

func (r *Request) Method() string {
	return r.method
}

func (r *Request) SetMethod(method string) {
	r.method = method
}

func (r *Request) Uri() string {
	return r.uri
}

func (r *Request) SetUri(uri string) {
	r.uri = uri
}

func (r *Request) Timeout() int {
	return r.timeout
}

func (r *Request) SetTimeout(timeout int) {
	r.timeout = timeout
}

func (r *Request) Header(key string) string {
	if v, ok := r.headers[key]; ok {
		return v
	}
	return ""
}

func (r *Request) SetHeader(key, value string) {
	if r.headers == nil {
		r.headers = make(map[string]string)
	}
	r.headers[key] = value
}

func (r *Request) Headers() map[string]string {
	return r.headers
}

func (r *Request) SetHeaders(headers map[string]string) {
	r.headers = headers
}

func (r *Request) Params() map[string][]string {
	return r.params
}

func (r *Request) SetParams(params map[string][]string) {
	r.params = params
}

func (r *Request) Param(key string) []string {
	if v, ok := r.params[key]; ok {
		return v
	}
	return []string{}
}

func (r *Request) SetParam(key, value string) {
	if r.params == nil {
		r.params = make(map[string][]string)
	}
	if values, ok := r.params[key]; ok {
		r.params[key] = append(values, value)
	} else {
		r.params[key] = []string{value}
	}
}

func (r *Request) QueryString() string {
	buf := make([]string, 0, len(r.params))
	for key, values := range r.params {
		for _, value := range values {
			buf = append(buf, util.UriEncode(key, true)+"="+util.UriEncode(value, true))
		}
	}
	return strings.Join(buf, "&")
}

func (r *Request) Body() io.ReadCloser {
	return r.body
}

func (r *Request) SetBody(stream io.ReadCloser) {
	r.body = stream
}

func (r *Request) GenerateUrl() string {
	url := fmt.Sprintf("http://%s:%d%s", r.host, r.port, r.uri)
	if len(r.params) > 0 {
		url = fmt.Sprintf("%s?%s", url, r.QueryString())
	}
	return url
}

func (r *Request) String() string {
	header := make([]string, 0, len(r.headers))
	for k, v := range r.headers {
		header = append(header, "\t"+k+"="+v)
	}
	return fmt.Sprintf("\t%s %s\n%v",
		r.method, r.GenerateUrl(), strings.Join(header, "\n"))
}
