// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/pipeline/job.go

// Package pipeline is a generated GoMock package.
package pipeline

import (
	schema "paddleflow/pkg/common/schema"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockJob is a mock of Job interface.
type MockJob struct {
	ctrl     *gomock.Controller
	recorder *MockJobMockRecorder
}

// MockJobMockRecorder is the mock recorder for MockJob.
type MockJobMockRecorder struct {
	mock *MockJob
}

// NewMockJob creates a new mock instance.
func NewMockJob(ctrl *gomock.Controller) *MockJob {
	mock := &MockJob{ctrl: ctrl}
	mock.recorder = &MockJobMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockJob) EXPECT() *MockJobMockRecorder {
	return m.recorder
}

// Check mocks base method.
func (m *MockJob) Check() (schema.JobStatus, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Check")
	ret0, _ := ret[0].(schema.JobStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Check indicates an expected call of Check.
func (mr *MockJobMockRecorder) Check() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Check", reflect.TypeOf((*MockJob)(nil).Check))
}

// Failed mocks base method.
func (m *MockJob) Failed() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Failed")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Failed indicates an expected call of Failed.
func (mr *MockJobMockRecorder) Failed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Failed", reflect.TypeOf((*MockJob)(nil).Failed))
}

// Job mocks base method.
func (m *MockJob) Job() BaseJob {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Job")
	ret0, _ := ret[0].(BaseJob)
	return ret0
}

// Job indicates an expected call of Job.
func (mr *MockJobMockRecorder) Job() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Job", reflect.TypeOf((*MockJob)(nil).Job))
}

// NotEnded mocks base method.
func (m *MockJob) NotEnded() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotEnded")
	ret0, _ := ret[0].(bool)
	return ret0
}

// NotEnded indicates an expected call of NotEnded.
func (mr *MockJobMockRecorder) NotEnded() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotEnded", reflect.TypeOf((*MockJob)(nil).NotEnded))
}

// Skipped mocks base method.
func (m *MockJob) Skipped() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Skipped")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Skipped indicates an expected call of Skipped.
func (mr *MockJobMockRecorder) Skipped() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Skipped", reflect.TypeOf((*MockJob)(nil).Skipped))
}

// Start mocks base method.
func (m *MockJob) Start() (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start")
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Start indicates an expected call of Start.
func (mr *MockJobMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockJob)(nil).Start))
}

// Started mocks base method.
func (m *MockJob) Started() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Started")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Started indicates an expected call of Started.
func (mr *MockJobMockRecorder) Started() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Started", reflect.TypeOf((*MockJob)(nil).Started))
}

// Stop mocks base method.
func (m *MockJob) Stop() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stop")
	ret0, _ := ret[0].(error)
	return ret0
}

// Stop indicates an expected call of Stop.
func (mr *MockJobMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockJob)(nil).Stop))
}

// Succeeded mocks base method.
func (m *MockJob) Succeeded() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Succeeded")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Succeeded indicates an expected call of Succeeded.
func (mr *MockJobMockRecorder) Succeeded() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Succeeded", reflect.TypeOf((*MockJob)(nil).Succeeded))
}

// Terminated mocks base method.
func (m *MockJob) Terminated() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Terminated")
	ret0, _ := ret[0].(bool)
	return ret0
}

// Terminated indicates an expected call of Terminated.
func (mr *MockJobMockRecorder) Terminated() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Terminated", reflect.TypeOf((*MockJob)(nil).Terminated))
}

// Update mocks base method.
func (m *MockJob) Update(cmd string, params, envs map[string]string, artifacts *schema.Artifacts) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Update", cmd, params, envs, artifacts)
	ret0, _ := ret[0].(error)
	return ret0
}

// Update indicates an expected call of Update.
func (mr *MockJobMockRecorder) Update(cmd, params, envs, artifacts interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*MockJob)(nil).Update), cmd, params, envs, artifacts)
}

// Validate mocks base method.
func (m *MockJob) Validate() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Validate")
	ret0, _ := ret[0].(error)
	return ret0
}

// Validate indicates an expected call of Validate.
func (mr *MockJobMockRecorder) Validate() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Validate", reflect.TypeOf((*MockJob)(nil).Validate))
}

// Watch mocks base method.
func (m *MockJob) Watch(arg0 chan WorkflowEvent) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Watch", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Watch indicates an expected call of Watch.
func (mr *MockJobMockRecorder) Watch(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Watch", reflect.TypeOf((*MockJob)(nil).Watch), arg0)
}
