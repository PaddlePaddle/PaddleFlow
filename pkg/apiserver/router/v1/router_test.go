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

package v1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/user"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/middleware"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

var mockUserName = "mockUser"

const (
	MockRunID1     = "run-id-000001"
	MockRunID2     = "run-id-000002"
	MockRunID3     = "run-id-000003"
	MockRunName1   = "run-name1"
	MockRunName2   = "run-name2"
	MockFsName1    = "fs-name_1"
	MockFsName2    = "fs-name_2"
	MockRootUser   = "root"
	MockPassword   = "mockPassword123456"
	MockNormalUser = "user-normal"
	mockFsName     = "mockfs"
	mockFsID       = "fs-root-mockfs"
)

var auth string

func mockAuth1(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(common.HeaderKeyUserName, mockUserName)
		next.ServeHTTP(w, r)
	})
}
func mockAuth2(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(common.HeaderKeyUserName, MockRootUser)
		r.Header.Set(common.HeaderKeyAuthorization, auth)
		next.ServeHTTP(w, r)
	})
}

// mockAuth3 for non-root operate
func mockAuth3(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(common.HeaderKeyUserName, mockUserName)
		r.Header.Set(common.HeaderKeyAuthorization, auth)
		next.ServeHTTP(w, r)
	})
}

func prepareDBAndAPI(t *testing.T) (*chi.Mux, string) {
	chiRouter := NewApiTest()
	baseUrl := util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1

	config.GlobalServerConfig = &config.ServerConfig{
		ApiServer: config.ApiServerConfig{
			TokenExpirationHour: -1,
		},
	}

	driver.InitMockDB()
	rootCtx := &logger.RequestContext{UserName: MockRootUser}

	token, err := CreateTestUser(rootCtx, MockRootUser, MockPassword)
	assert.Nil(t, err)
	setToken(token)

	return chiRouter, baseUrl
}

// NewApiTest func create router of chi for test
func NewApiTest() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.CheckRequestID, mockAuth2)
	RegisterRouters(r, false)
	return r
}

// NewApiTestNonRoot func create router of chi for non-root test
func NewApiTestNonRoot() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.CheckRequestID, mockAuth3)
	RegisterRouters(r, true)
	return r
}

func CreateTestUser(ctx *logger.RequestContext, username, password string) (string, error) {
	fmt.Printf("CreateTestUser begins. username[%s] password[%s]\n", username, password)
	encrypted, err := user.EncodePassWord(password)
	if err != nil {
		fmt.Printf("CreateTestUser failed EncodePassWord. err:%v\n", err)
		return "", err
	}
	root := model.User{
		UserInfo: model.UserInfo{
			Name:     username,
			Password: encrypted,
		},
	}
	err = storage.Auth.CreateUser(ctx, &root)
	if err != nil {
		fmt.Printf("CreateTestUser failed creating user. err:%v\n", err)
		return "", err
	}
	token, err := middleware.GenerateToken(username, encrypted)
	if err != nil {
		fmt.Printf("CreateTestUser failed generating token. err:%v\n", err)
	}
	return token, err
}

func setToken(token string) {
	auth = token
}

// PerformPostRequest func perform post request for test
func PerformPostRequest(handler http.Handler, path string, v interface{}) (*httptest.ResponseRecorder, error) {
	body, _ := json.Marshal(v)
	buf := bytes.NewBuffer(body)
	req, _ := http.NewRequest("POST", path, buf)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder, nil
}

// PerformFileUploadRequest func perform post file upload request for test
func PerformFileUploadRequest(handler http.Handler, path, fileName string,
	fileBytes []byte, v map[string][]string) (*httptest.ResponseRecorder, error) {
	// create form file
	buf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(buf)
	fileWriter, _ := bodyWriter.CreateFormFile("file", fileName)
	// read the file to the fileWriter
	length, _ := fileWriter.Write(fileBytes)
	bodyWriter.Close()
	req, _ := http.NewRequest("POST", path, buf)
	req.Header.Set("Content-Type", bodyWriter.FormDataContentType())
	req.ParseMultipartForm(int64(length))
	req.MultipartForm.Value = v
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder, nil
}

// PerformPutRequest func perform put request for test
func PerformPutRequest(handler http.Handler, path string, v interface{}) (*httptest.ResponseRecorder, error) {
	body, _ := json.Marshal(v)
	buf := bytes.NewBuffer(body)
	req, _ := http.NewRequest("PUT", path, buf)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder, nil
}

// PerformGetRequest function performs get request for test
func PerformGetRequest(handler http.Handler, path string) (*httptest.ResponseRecorder, error) {
	req, _ := http.NewRequest("GET", path, nil)
	req.Header.Set(common.HeaderKeyAuthorization, auth)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder, nil
}

// PerformGetRequest function performs get request for test
func PerformDeleteRequest(handler http.Handler, path string) (*httptest.ResponseRecorder, error) {
	req, _ := http.NewRequest("DELETE", path, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder, nil
}

// ParseBody function parse the resp from body of resp
func ParseBody(body *bytes.Buffer, v interface{}) error {
	content, _ := ioutil.ReadAll(body)
	return json.Unmarshal(content, v)
}

func DebugChiRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(mockAuth1)
	RegisterRouters(r, true)
	return r
}

func NewHttpRecorder(serverPath, url string, method string, body interface{}) *http.Request {
	pathPrefix := util.PaddleflowRouterPrefix + util.PaddleflowRouterVersionV1
	if body != nil {
		byBody, _ := json.Marshal(&body)
		buf := bytes.NewBuffer(byBody)
		req, _ := http.NewRequest(method, serverPath+pathPrefix+url, buf)
		return req
	}
	req, _ := http.NewRequest(method, serverPath+pathPrefix+url, nil)
	return req
}

func TestRegisterRouters(t *testing.T) {
	r := chi.NewRouter()
	RegisterRouters(r, false)
	testServer := httptest.NewServer(r)
	// NotFoundHandler
	req, err := http.NewRequest(http.MethodGet, testServer.URL+"/testNotFound", nil)
	assert.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, res.StatusCode, 404)

	// MethodNotAllowed
	req, err = http.NewRequest(http.MethodPut, testServer.URL+"/api/paddleflow/v1/login", nil)
	assert.NoError(t, err)
	res, err = http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, res.StatusCode, 405)
}
