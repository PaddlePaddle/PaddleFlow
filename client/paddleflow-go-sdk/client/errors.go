package client

import (
	"fmt"
)

type ServiceError struct {
	Code       string
	Message    string
	RequestID  string
	StatusCode int
}

func (s *ServiceError) Error() string {
	ret := "[Code: " + s.Code
	ret += "; Message: " + s.Message
	ret += "; RequestId: " + s.RequestID + "]"
	return ret
}

func NewServiceError(code, msg, reqID string, status int) *ServiceError {
	return &ServiceError{
		Code:       code,
		Message:    msg,
		RequestID:  reqID,
		StatusCode: status,
	}
}

func CloseConnectionError() error {
	return fmt.Errorf("web connection closed")
}
