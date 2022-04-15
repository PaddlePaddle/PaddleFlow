package client

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"paddleflow-go-sdk/util"
)

const (
	WsJobUri       = "/api/paddleflow/v1/wsjob"
	JobUriPrefix   = "/api/paddleflow/v1/job/"
	LoginUri       = "/api/paddleflow/v1/login"
	MaxReceiveTime = 10
)

type Client interface {
	SendRequest(*Request) (*Response, error)
	SendPostRequest(structBody interface{}, url string) (*Response, error)
	SendGetRequest(url string) (*Response, error)
	SendPutRequest(structBody interface{}, url string) (*Response, error)
	SendDeleteRequest(url string) (*Response, error)
}

type PFClient struct {
	HttpClient    *http.Client
	Connection    *websocket.Conn
	HealthCheck   bool
	HeartbeatChan chan int
	ReceiveChan   chan []byte
	CloseChan     chan byte
	MuxClose      sync.Mutex
	IsClosed      bool
	Config        *ClientConfig
	JobCache      util.CacheProcess
	AuthHeader    http.Header
}

type ClientCache struct {
	Cache util.CacheProcess
}

func NewPFClient(config *ClientConfig) *PFClient {
	if config.ConnectionTimeoutInSeconds == 0 {
		config.ConnectionTimeoutInSeconds = DefaultConnectionTimeout
	}
	client := &PFClient{
		HttpClient: &http.Client{
			Timeout: time.Duration(config.ConnectionTimeoutInSeconds) * time.Second,
		},
		HeartbeatChan: make(chan int, 100),
		ReceiveChan:   make(chan []byte, 1000),
		CloseChan:     make(chan byte, 1),
		Config:        config,
	}
	return client
}

func (c *PFClient) SendRequest(req *Request) (*Response, error) {
	// Build the http request and prepare to send
	httpReq, err := c.buildHttpRequest(req)
	if err != nil {
		return nil, err
	}

	httpResp, err := c.HttpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}

	// construct response
	resp := &Response{}
	resp.SetHttpResponse(httpResp)
	resp.ParseResponse()
	return resp, nil
}

func (c *PFClient) SendPostRequest(structBody interface{}, url string) (*Response, error) {
	req := &Request{}
	req.SetMethod(POST)
	req.SetURI(url)
	requestBody, err := NewRequestBodyWithStruct(structBody)
	if err != nil {
		return nil, err
	}
	req.SetBody(requestBody)
	resp, err := c.SendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.IsFail() {
		return nil, resp.ServiceError()
	}
	return resp, nil
}

func (c *PFClient) SendGetRequest(url string, params *map[string]string) (*Response, error) {
	req := &Request{}
	req.SetMethod(GET)
	req.SetURI(url)
	if params != nil {
		for k, v := range *params {
			req.SetParam(k, v)
		}
	}

	resp, err := c.SendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.IsFail() {
		return nil, resp.ServiceError()
	}
	return resp, nil
}

func (c *PFClient) SendPutRequest(structBody interface{}, url string) (*Response, error) {
	req := &Request{}
	req.SetMethod(PUT)
	req.SetURI(url)
	if structBody != nil {
		requestBody, err := NewRequestBodyWithStruct(structBody)
		if err != nil {
			return nil, err
		}
		req.SetBody(requestBody)
	}
	resp, err := c.SendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.IsFail() {
		return nil, resp.ServiceError()
	}
	return resp, nil
}

func (c *PFClient) SendDeleteRequest(url string) (*Response, error) {
	req := &Request{}
	req.SetMethod(DELETE)
	req.SetURI(url)
	resp, err := c.SendRequest(req)
	if err != nil {
		return nil, err
	}
	if resp.IsFail() {
		return nil, resp.ServiceError()
	}
	return resp, nil
}

func (c *PFClient) buildHttpRequest(request *Request) (*http.Request, error) {
	// Set the client specific configurations
	request.SetHost(c.Config.Host)
	request.SetPort(c.Config.Port)
	request.SetTimeout(c.Config.ConnectionTimeoutInSeconds)

	// Construct the http request instance
	httpRequest, err := request.BuildHttpRequest()
	if err != nil {
		return nil, err
	}

	// add auth header
	if c.AuthHeader != nil && len(c.AuthHeader) != 0 {
		for k, v := range c.AuthHeader {
			httpRequest.Header[k] = v
		}
	}

	return httpRequest, nil
}

func BuildWSConnection(c *PFClient) error {
	url := fmt.Sprintf("ws://%s:%d%s", c.Config.Host, c.Config.Port, WsJobUri)
	ws, _, err := websocket.DefaultDialer.Dial(url, c.AuthHeader)
	if err != nil {
		return err
	}
	c.Connection = ws
	c.HealthCheck = true

	// read
	go c.readLoop()

	go c.writeInCacheLoop()

	// heartbeat
	go func() {
		defer c.Close()
		for {
			err := ws.WriteMessage(websocket.TextMessage, []byte(HeartBeatSingal))
			if err != nil {
				c.HealthCheck = false
				return
			}
			select {
			case <-c.HeartbeatChan:
				c.HealthCheck = true
			case <-c.CloseChan:
				return
			case <-time.After(MaxReceiveTime * time.Second):
				return
			}
			time.Sleep(time.Second * 2)
		}
	}()
	return nil
}

func (c *PFClient) ReadMessage() (int, []byte, error) {
	select {
	case <-c.CloseChan:
		return 0, []byte(""), CloseConnectionError()
	default:
		return c.Connection.ReadMessage()
	}
}

func (c *PFClient) readLoop() {
	defer c.Close()
	for {
		dataType, data, err := c.ReadMessage()
		// TODO for debug
		fmt.Println(dataType, string(data))
		if err != nil {
			return
		}
		switch string(data) {
		case HeartBeatSingal:
			select {
			case c.HeartbeatChan <- dataType:
				continue
			case <-c.CloseChan:
				return
			}
		default:
			select {
			case c.ReceiveChan <- data:
				continue
			case <-c.CloseChan:
				return
			}
		}

	}
}

func (c *PFClient) writeInCacheLoop() {
	for {
		select {
		case data := <-c.ReceiveChan:
			c.JobCache.WriteCache(data)
		case <-c.CloseChan:
			return

		}
	}
}

func (c *PFClient) Close() {
	c.Connection.Close()
	c.HealthCheck = false
	c.MuxClose.Lock()
	if !c.IsClosed {
		close(c.CloseChan)
		c.IsClosed = true
		// 清理cache
		c.JobCache.ClearCache()
	}
	c.MuxClose.Unlock()
}
