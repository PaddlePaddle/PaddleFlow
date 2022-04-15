package client

import (
	"encoding/json"
	"io"
	"net/http"
)

// Response defines the response structure for receiving APE services response.
type Response struct {
	statusCode   int
	statusText   string
	requestID    string
	response     *http.Response
	serviceError *ServiceError
}

func (r *Response) IsFail() bool {
	return r.response.StatusCode != 200
}

func (r *Response) StatusCode() int {
	return r.statusCode
}

func (r *Response) StatusText() string {
	return r.statusText
}

func (r *Response) RequestID() string {
	return r.requestID
}

func (r *Response) Body() io.ReadCloser {
	return r.response.Body
}

func (r *Response) SetHttpResponse(response *http.Response) {
	r.response = response
}

func (r *Response) ServiceError() *ServiceError {
	return r.serviceError
}

func (r *Response) ParseResponse() {
	r.statusCode = r.response.StatusCode
	r.statusText = r.response.Status
	r.requestID = r.response.Header.Get(HeaderKeyRequestID)
	if r.IsFail() {
		r.serviceError = NewServiceError("", r.statusText, r.requestID, r.statusCode)
	}
}

func (r *Response) ParseAndCloseJsonBody(result interface{}) error {
	defer r.Body().Close()
	jsonDecoder := json.NewDecoder(r.Body())
	return jsonDecoder.Decode(result)
}
