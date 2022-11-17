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

package models

import (
	"encoding/json"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

const (
	MockRunID1   = "run-id_1"
	MockRunName1 = "run-name_1"
	MockFsID1    = "fs-mockUser-mockFs"
	MockRunID3   = "run-id_3"

	MockRunID2   = "run-id_2"
	MockRunName2 = "run-name_2"
	MockUserID2  = "user-id_2"
	MockFsID2    = "fs-mockUser-mockFs"
)

func getMockRun3() Run {
	failureOptions := schema.FailureOptions{Strategy: schema.FailureStrategyContinue}
	failureOptionsRaw, err := json.Marshal(failureOptions)
	if err != nil {
		panic(err)
	}

	run2 := Run{
		ID:                 MockRunID2,
		Name:               MockRunName2,
		UserName:           MockUserID2,
		FsID:               MockFsID2,
		Status:             common.StatusRunPending,
		FailureOptionsJson: string(failureOptionsRaw),
		RunYaml:            string(loadCase(runYamlPath)),
	}
	run2.Encode()
	return run2
}

func insertRun(run *Run) (string, error) {
	initMockDB()
	logEntry := log.WithFields(log.Fields{})
	id, err := CreateRun(logEntry, run)
	return id, err
}

func TestEncode(t *testing.T) {
	var err error
	run3 := getMockRun3()
	assert.Nil(t, run3.FailureOptions)

	run3.FailureOptionsJson = ""
	run3.FailureOptions = &schema.FailureOptions{Strategy: schema.FailureStrategyContinue}
	run3.Encode()

	failureOptions := &schema.FailureOptions{}
	err = json.Unmarshal([]byte(run3.FailureOptionsJson), failureOptions)
	assert.Nil(t, err)
	assert.Equal(t, failureOptions.Strategy, schema.FailureStrategyContinue)
}

func TestGetRunByID(t *testing.T) {
	run := getMockRun3()
	id, err := insertRun(&run)
	assert.Nil(t, err)

	logEntry := log.WithFields(log.Fields{})
	run, err = GetRunByID(logEntry, id)
	assert.Nil(t, err)
	assert.Equal(t, run.FailureOptions.Strategy, schema.FailureStrategyContinue)

	run.FailureOptionsJson = "abde"
	id, err = insertRun(&run)
	assert.Nil(t, err)

	run, err = GetRunByID(logEntry, id)
	assert.Contains(t, err.Error(), "decodeFailureOptions in run decode")
	assert.Contains(t, err.Error(), "json unmarshal failureOptions failed")
}
