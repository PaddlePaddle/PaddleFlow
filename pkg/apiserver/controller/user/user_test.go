/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package user

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser = "root"
	MockUser1    = "u123"
	MockWrongPW  = "u1"
	MockPW       = "mock709394"
)

func TestCreateUser(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	// bad case
	resp, err := CreateUser(ctx, MockUser1, MockWrongPW)
	assert.NotNil(t, err)

	resp, err = CreateUser(ctx, MockUser1, MockPW)
	assert.Nil(t, err)
	t.Logf("response=%+v", resp)
}

func TestUpdateUser(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockRootUser}

	resp, err := CreateUser(ctx, MockUser1, MockPW)
	assert.Nil(t, err)
	t.Logf("response=%+v", resp)
}

func TestListUser(t *testing.T) {
	TestCreateUser(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}

	users, err := ListUser(ctx, "", 0)
	assert.Nil(t, err)
	assert.NotZero(t, len(users.Users))
	t.Logf("response=%+v", users)
}

func TestGetUserByName(t *testing.T) {
	TestCreateUser(t)
	ctx := &logger.RequestContext{UserName: MockRootUser}

	user, err := GetUserByName(ctx, MockUser1)
	assert.Nil(t, err)
	assert.Equal(t, MockUser1, user.Name)
	t.Logf("response=%+v", user)
}

func TestLogin(t *testing.T) {
	TestCreateUser(t)
	ctx := &logger.RequestContext{}
	user, err := Login(ctx, MockUser1, MockPW, false)
	assert.Nil(t, err)
	assert.Equal(t, MockUser1, user.Name)
}
