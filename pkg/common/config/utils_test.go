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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitConfigFromYaml(t *testing.T) {
	tests := []struct {
		confPath string
		msg      string // whether getOrCreateJob will return job for corresponding task
	}{
		{
			confPath: "./config/server/default/paddleserver.yaml",
			msg:      "open ./config/server/default/paddleserver.yaml: no such file or directory",
		},
		{
			confPath: "../../../config/server/default/paddleserver.yaml",
			msg:      "",
		},
	}
	for i, test := range tests {
		ServerConf := &ServerConfig{}
		serverDefaultConfPath = test.confPath
		err := InitConfigFromYaml(ServerConf, "")
		if (err != nil && err.Error() != test.msg) || (err == nil && ServerConf.Log.Level == "") {
			t.Errorf("testcase[%d] New ServerConfig[%+v] init failed, err: %v", i, ServerConf, err)
		}
	}
}

func TestInitJobTemplate(t *testing.T) {
	tests := []struct {
		confPath string
		count    int
	}{
		{
			confPath: "../../../config/server/default/job/job_template.yaml",
			count:    3,
		},
	}
	for _, test := range tests {
		err := InitJobTemplate(test.confPath)
		assert.Equal(t, nil, err)
		assert.Equal(t, test.count, len(DefaultJobTemplate))
	}
}
