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

package driver

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
)

func TestInitCache(t *testing.T) {
	err := InitCache("DEBUG")
	assert.Equal(t, nil, err)
}

func TestInitStorage(t *testing.T) {
	testCases := []struct {
		name    string
		conf    *config.StorageConfig
		wantErr error
	}{
		{
			name:    "init sqlite",
			conf:    &config.StorageConfig{},
			wantErr: nil,
		},
		{
			name: "init mysql",
			conf: &config.StorageConfig{
				Driver:                               Mysql,
				Host:                                 "127.0.0.1",
				Port:                                 "3306",
				ConnectTimeoutInSeconds:              30,
				LockTimeoutInMilliseconds:            60000,
				IdleTransactionTimeoutInMilliseconds: 60000,
			},
			wantErr: fmt.Errorf("init %s database DB failed", Mysql),
		},
		{
			name: "init postgresql",
			conf: &config.StorageConfig{
				Driver:                               PostgreSQL,
				Host:                                 "127.0.0.1",
				Port:                                 "5432",
				ConnectTimeoutInSeconds:              30,
				LockTimeoutInMilliseconds:            60000,
				IdleTransactionTimeoutInMilliseconds: 60000,
			},
			wantErr: fmt.Errorf("init %s database DB failed", PostgreSQL),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := InitStorage(tc.conf, "DEBUG")
			t.Logf("init storage %v", err)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
