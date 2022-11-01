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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const rspPath = "../../../../pkg/apiserver/controller/pipeline/testcase/get_run_rsp.json"

func loadCase(casePath string) []byte {
	data, err := ioutil.ReadFile(casePath)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	return data
}

func TestGetRunRspUnmarshal(t *testing.T) {
	rsp := GetRunResponse{}
	rspByte := loadCase(rspPath)
	err := json.Unmarshal(rspByte, &rsp)
	assert.Nil(t, err)

	startTime := rsp.GetStartTime()
	assert.Equal(t, startTime, "2022-10-18 15:19:50")
	endTime := rsp.GetEndTime()
	assert.Equal(t, endTime, "2022-10-18 15:21:46")

	compStartTime := rsp.Runtime["disDag"][0].GetStartTime()
	assert.Equal(t, compStartTime, "2022-10-18 15:19:50")
	compEndTime := rsp.Runtime["disDag"][0].GetEndTime()
	assert.Equal(t, compEndTime, "2022-10-18 15:20:08")
}
