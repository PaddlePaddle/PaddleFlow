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
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	defaultMaxIdleConnsPerHost   = 500
	defaultResponseHeaderTimeout = 60 * time.Second
	defaultDialTimeout           = 30 * time.Second
	defaultSmallInterval         = 600 * time.Second
	defaultLargeInterval         = 1200 * time.Second
)

// The httpClient is the global variable to send the request and get response
// for reuse and the Client provided by the Go standard library is thread safe.
var (
	httpClient *http.Client
	transport  *http.Transport
)

type timeoutConn struct {
	conn          net.Conn
	smallInterval time.Duration
	largeInterval time.Duration
}

func (c *timeoutConn) Read(b []byte) (n int, err error) {
	c.SetReadDeadline(time.Now().Add(c.smallInterval))
	n, err = c.conn.Read(b)
	c.SetReadDeadline(time.Now().Add(c.largeInterval))
	return n, err
}
func (c *timeoutConn) Write(b []byte) (n int, err error) {
	c.SetWriteDeadline(time.Now().Add(c.smallInterval))
	n, err = c.conn.Write(b)
	c.SetWriteDeadline(time.Now().Add(c.largeInterval))
	return n, err
}
func (c *timeoutConn) Close() error                       { return c.conn.Close() }
func (c *timeoutConn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *timeoutConn) RemoteAddr() net.Addr               { return c.conn.RemoteAddr() }
func (c *timeoutConn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *timeoutConn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *timeoutConn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

var customizeInit sync.Once

func InitClient() {
	customizeInit.Do(func() {
		httpClient = &http.Client{}
		transport = &http.Transport{
			MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
			ResponseHeaderTimeout: defaultResponseHeaderTimeout,
			Dial: func(network, address string) (net.Conn, error) {
				conn, err := net.DialTimeout(network, address, defaultDialTimeout)
				if err != nil {
					return nil, err
				}
				tc := &timeoutConn{conn, defaultSmallInterval, defaultLargeInterval}
				tc.SetReadDeadline(time.Now().Add(defaultLargeInterval))
				return tc, nil
			},
		}
		httpClient.Transport = transport
	})
}

// Execute - do the http requset and get the response
//
// PARAMS:
//     - request: the http request instance to be sent
// RETURNS:
//     - response: the http response returned from the server
//     - error: nil if ok otherwise the specific error
func Execute(request *Request) (*Response, error) {
	url := fmt.Sprintf("http://%s:%d%s", request.host, request.port, request.uri)
	if len(request.params) > 0 {
		url = fmt.Sprintf("%s?%s", url, request.QueryString())
	}

	// Set the request headers
	header := make(http.Header)
	for k, v := range request.Headers() {
		val := make([]string, 0, 1)
		val = append(val, v)
		header[k] = val
	}

	httpRequest, err := http.NewRequest(request.method, url, request.body)
	if err != nil {
		return nil, fmt.Errorf("new request with method[%s] and url[%s] failed, %v", request.method, url, err)
	}
	httpRequest.Header = header

	// Perform the http request and get response
	// It needs to explicitly close the keep-alive connections when error occurs for the request
	// that may continue sending request's data subsequently.
	start := time.Now()

	httpResponse, err := httpClient.Do(httpRequest)

	end := time.Now()
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	if httpResponse.StatusCode >= 400 &&
		(httpRequest.Method == PUT || httpRequest.Method == POST) {
		transport.CloseIdleConnections()
	}
	response := &Response{httpResponse, end.Sub(start)}
	return response, nil
}
