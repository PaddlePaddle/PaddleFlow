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

package schema

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

const runYamlPath = "../../apiserver/controller/pipeline/testcase/run_dag.yaml"

func loadCase(casePath string) []byte {
	data, err := ioutil.ReadFile(casePath)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	return data
}

func TestGetWorkflowSource(t *testing.T) {
	wfs, err := GetWorkflowSource(loadCase(runYamlPath))
	assert.Nil(t, err)
	assert.Equal(t, "{{num}} > -10", wfs.Components["condition2"].GetCondition())
	// TODO: 增加测试用例

	// 将wfs输出成json整体查看
	wfsJson, _ := json.Marshal(wfs)
	fmt.Println(string(wfsJson))

	// 测试UnmarshalJSON
	newWfs := WorkflowSource{}
	err = newWfs.UnmarshalJSON(wfsJson)
	assert.Nil(t, err)

	err = newWfs.UnmarshalJSON([]byte("wrongJson"))
	assert.NotNil(t, err)

	err = newWfs.UnmarshalJSON([]byte(`{"wrongkey": "wrongValue"}`))
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "json of workflow illegal")
}

func TestGetComponentByFullName(t *testing.T) {
	wfs, err := GetWorkflowSource(loadCase(runYamlPath))
	assert.Nil(t, err)

	_, err = wfs.GetComponentByFullName("square-loop.square")
	assert.Nil(t, err)
}

func TestDagDeepCopy(t *testing.T) {
	wfs, err := GetWorkflowSource(loadCase(runYamlPath))
	assert.Nil(t, err)

	loop := wfs.EntryPoints.EntryPoints["square-loop"]
	loop2 := loop.DeepCopy()

	assert.Equal(t, loop, loop2)

	isSame := loop == loop2
	assert.False(t, isSame)

	loop.UpdateLoopArguemt([]int{1, 3, 4})
	assert.NotEqual(t, loop.GetLoopArgument(), loop2.GetLoopArgument())

	fmt.Println("in loop:", loop.GetLoopArgument())
	fmt.Println("in loop2:", loop2.GetLoopArgument())
}

func TestTransToRunYamlRaw(t *testing.T) {
	wfs, err := GetWorkflowSource(loadCase(runYamlPath))
	assert.Nil(t, err)

	wfs.PostProcess = map[string]*WorkflowSourceStep{
		"post": &WorkflowSourceStep{
			Command: "echo post"},
	}

	runYamlRaw, err := wfs.TransToRunYamlRaw()
	assert.Nil(t, err)

	runYamlBytes, err := base64.StdEncoding.DecodeString(runYamlRaw)

	ioutil.WriteFile("trans.yaml", runYamlBytes, 0777)
	newWfs, err := GetWorkflowSource(loadCase("trans.yaml"))
	assert.Nil(t, err)

	assert.Contains(t, newWfs.PostProcess, "post")
	assert.Equal(t, len(wfs.EntryPoints.EntryPoints), len(newWfs.EntryPoints.EntryPoints))
}
