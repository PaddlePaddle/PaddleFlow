package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"paddleflow-go-sdk/util"
)

type Request struct {
	// internal request values
	host      string
	port      int
	method    string
	uri       string
	requestID string
	timeout   int
	headers   map[string]string
	params    map[string][]string
	body      io.Reader
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

func (r *Request) SetMethod(method string) {
	r.method = method
}

func (r *Request) SetURI(uri string) {
	r.uri = uri
}

func (r *Request) RequestID() string {
	return r.requestID
}

func (r *Request) SetRequestID(id string) {
	r.requestID = id
}

func (r *Request) SetHeader(key, value string) {
	if r.headers == nil {
		r.headers = make(map[string]string)
	}
	r.headers[key] = value
}

func (r *Request) SetParam(key, value string) {
	if r.params == nil {
		r.params = make(map[string][]string)
	}
	r.params[key] = []string{value}
}

func (r *Request) SetParams(key string, value []string) {
	if r.params == nil {
		r.params = make(map[string][]string)
	}
	r.params[key] = append(r.params[key], value...)
}

func (r *Request) QueryString() string {
	buf := make([]string, 0, len(r.params))
	for k, values := range r.params {
		for _, v := range values {
			buf = append(buf, util.UriEncode(k, true)+"="+util.UriEncode(v, true))
		}
	}
	return strings.Join(buf, "&")
}

func (r *Request) Timeout() int {
	return r.timeout
}

func (r *Request) SetTimeout(timeout int) {
	r.timeout = timeout
}

func NewRequestBodyWithStruct(requestBody interface{}) ([]byte, error) {
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request body failed, error: %v, body: %v", err, requestBody)
	}
	return jsonBody, nil
}

func (r *Request) SetBody(body []byte) {
	r.body = bytes.NewBuffer(body)
}

func (r *Request) BuildHttpRequest() (*http.Request, error) {
	url := fmt.Sprintf("http://%s:%d%s", r.host, r.port, r.uri)
	if len(r.params) > 0 {
		url = fmt.Sprintf("%s?%s", url, r.QueryString())
	}

	// GET类请求不会设置body，因此在这儿做下兼容处理
	if nil == r.body {
		var body []byte
		r.SetBody(body)
	}

	httpReq, err := http.NewRequest(r.method, url, r.body)
	if err != nil {
		return nil, fmt.Errorf("new request with method[%s] and url[%s] failed, %v", r.method, url, err)
	}

	// set headers of request
	for k, v := range r.headers {
		httpReq.Header.Set(k, v)
	}

	return httpReq, nil
}
