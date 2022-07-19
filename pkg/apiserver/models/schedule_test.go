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

package models

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	MockRootUser   = "root"
	MockNormalUser = "user1"
	MockFsName     = "mockFs"
	MockFsID       = "root-mockFs"

	runDagYamlPath = "../controller/pipeline/testcase/run_dag.yaml"
	runYamlPath    = "../controller/pipeline/testcase/run.yaml"
)

func loadCase(casePath string) []byte {
	data, err := ioutil.ReadFile(casePath)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	return data
}

func insertPipeline(t *testing.T, logEntry *log.Entry) (pplID1, pplID2, pplVersionID1, pplVersionID2 string) {
	ppl1 := Pipeline{
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	dagYamlStr := string(loadCase(runDagYamlPath))
	pplVersion1 := PipelineVersion{
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: dagYamlStr,
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	yamlStr := string(loadCase(runYamlPath))
	ppl2 := Pipeline{
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "root",
	}
	pplVersion2 := PipelineVersion{
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: yamlStr,
		PipelineMd5:  "md5_2",
		UserName:     "root",
	}

	var err error
	pplID1, pplVersionID1, err = CreatePipeline(logEntry, &ppl1, &pplVersion1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplVersion1.Pk, int64(1))
	assert.Equal(t, pplVersionID1, pplVersion1.ID)
	assert.Equal(t, pplVersionID1, "1")
	assert.Equal(t, pplVersion1.PipelineID, ppl1.ID)

	pplID2, pplVersionID2, err = CreatePipeline(logEntry, &ppl2, &pplVersion2)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.Pk, int64(2))
	assert.Equal(t, pplID2, ppl2.ID)
	assert.Equal(t, pplID2, "ppl-000002")

	assert.Equal(t, pplVersion2.Pk, int64(2))
	assert.Equal(t, pplVersionID2, pplVersion2.ID)
	assert.Equal(t, pplVersionID2, "1")
	assert.Equal(t, pplVersion2.PipelineID, ppl2.ID)

	return pplID1, pplID2, pplVersionID1, pplVersionID2
}

// ------ job / fs模块需要的函数 ------
func TestGetUsedFsIDs(t *testing.T) {
	initMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, pplID2, pplVersionID1, pplVersionID2 := insertPipeline(t, logEntry)

	schedule := Schedule{
		ID:                "", // to be back filled according to db pk
		Name:              "schedule1",
		Desc:              "schedule1",
		PipelineID:        pplID1,
		PipelineVersionID: pplVersionID1,
		UserName:          MockRootUser,
		Crontab:           "*/5 * * * *",
		Options:           "{}",
		Status:            ScheduleStatusRunning,
		StartAt:           sql.NullTime{},
		EndAt:             sql.NullTime{},
		NextRunAt:         time.Now(),
	}

	// 创建schedule前，查询返回为空
	fsIDMap, err := ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 0, len(fsIDMap))

	// 创建 running 状态的schedule
	schedID, err := CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, "schedule-000001", schedID)

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(fsIDMap))
	print(fsIDMap)

	schedule.PipelineVersionID = pplVersionID2
	schedule.PipelineID = pplID2

	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000002")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(fsIDMap))
	print(fsIDMap)

	// 创建 success 状态的schedule
	schedule.Status = ScheduleStatusSuccess
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000003")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(fsIDMap))

	// 创建 failed 状态的schedule
	schedule.Status = ScheduleStatusFailed
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000004")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(fsIDMap))

	// 创建 terminated 状态的schedule
	schedule.Status = ScheduleStatusTerminated
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000005")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(fsIDMap))
}
