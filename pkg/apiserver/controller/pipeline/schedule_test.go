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

package pipeline

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	cron "github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database/dbinit"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	pkgPplCommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

const (
	MockRootUser   = "root"
	MockNormalUser = "user1"
	MockFsName     = "mockFs"
	MockFsID       = "root-mockFs"
)

// 测试创建schedule
func TestCreateSchedule(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}
	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        "ppl-000001",
		PipelineDetailPk:  1,
		Crontab:           "* */3 * * *",
		StartTime:         "",
		EndTime:           "",
		Concurrency:       10,
		ConcurrencyPolicy: "suspend",
		ExpireInterval:    100,
		Catchup:           true,
		FsName:            MockFsName,
		UserName:          "",
	}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	patch1 := gomonkey.ApplyFunc(SendSingnal, func(string, string) error {
		return nil
	})

	defer patch.Reset()
	defer patch1.Reset()

	// 创建 pipeline & pipelineDetail
	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: MockNormalUser,
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "root-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     MockNormalUser,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &ppl1, &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	// 失败: schedule名称格式不支持
	createScheduleReq.Name = "-asdf"
	resp, err := CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("name[-asdf] for [schedule] does not compile with regex rule[^[A-Za-z0-9_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$]"), err)

	// 失败: concurrency < 0
	createScheduleReq.Name = "schedule_1"
	createScheduleReq.Concurrency = -1
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create schedule failed, err:[concurrency should not be negative]"), err)

	// 失败，corrency policy 不支持
	createScheduleReq.Concurrency = 10
	createScheduleReq.ConcurrencyPolicy = "wrongPolicy"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create schedule failed, err:[concurrency policy[wrongPolicy] not supported]"), err)

	// create 成功, 并检查默认policy是suspend
	createScheduleReq.ConcurrencyPolicy = ""
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)

	getScheduleResp, err := GetSchedule(ctx, resp.ScheduleID, "", 0, nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, getScheduleResp.Options.ConcurrencyPolicy, "suspend")

	// 失败，corrency expire interval < 0,
	createScheduleReq.ExpireInterval = -1
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create schedule failed, err:[expire interval should not be negative]"), err)

	// 失败，start time 格式不对
	createScheduleReq.ExpireInterval = 100
	startTime := time.Now()
	createScheduleReq.StartTime = startTime.Add(48 * time.Hour).Format("20060102 15:04:05")
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create schedule failed, starttime[%s] format not correct, should be YYYY-MM-DD hh-mm-ss", createScheduleReq.StartTime), err)

	// 失败，start time 时间早于当前时间
	createScheduleReq.ExpireInterval = 100
	startTime = time.Now()
	createScheduleReq.StartTime = startTime.Format("2006-01-02 15:04:05")
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf("create schedule failed, starttime[%s] not after currentTime", createScheduleReq.StartTime))

	// 失败，end time 格式不对
	createScheduleReq.StartTime = ""
	createScheduleReq.EndTime = time.Now().Add(48 * time.Hour).Format("20060102 15:04:05")
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create schedule failed, endtime[%s] format not correct, should be YYYY-MM-DD hh-mm-ss", createScheduleReq.EndTime), err)

	// 失败，end time 时间早于当前时间
	endTime := time.Now()
	createScheduleReq.EndTime = endTime.Format("2006-01-02 15:04:05")
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf("create schedule failed, endtime[%s] not after currentTime", createScheduleReq.EndTime))

	// 失败，end time 时间早于当前时间
	nowTime := time.Now()
	createScheduleReq.StartTime = nowTime.Add(48 * time.Hour).Format("2006-01-02 15:04:05")
	createScheduleReq.EndTime = nowTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05")
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf("create schedule failed, endtime[%s] not after startTime[%s]", createScheduleReq.EndTime, createScheduleReq.StartTime))

	// 失败，crontab 表达式有误
	nowTime = time.Now()
	createScheduleReq.StartTime = nowTime.Add(24 * time.Hour).Format("2006-01-02 15:04:05")
	createScheduleReq.EndTime = nowTime.Add(48 * time.Hour).Format("2006-01-02 15:04:05")
	createScheduleReq.Crontab = "* * * sdf asd"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("check crontab failed in creating schedule. error:failed to parse int from sdf: strconv.Atoi: parsing \"sdf\": invalid syntax"), err)

	// 成功，start time 为空
	// 并校验next run at
	createScheduleReq.StartTime = ""
	createScheduleReq.EndTime = startTime.Add(48 * time.Hour).Format("2006-01-02 15:04:05")
	createScheduleReq.Crontab = "* */3 * * *"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)

	cronSchedule, err := cron.ParseStandard(createScheduleReq.Crontab)
	assert.Nil(t, err)

	getScheduleResp, err = GetSchedule(ctx, resp.ScheduleID, "", 0, nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, cronSchedule.Next(time.Now()).Format("2006-01-02 15:04:05"), getScheduleResp.NextRunTime)

	// 成功, starttime不为空
	// 并校验nextrunat
	startTime = time.Now().Add(24 * time.Hour)
	endTime = time.Now().Add(48 * time.Hour)
	createScheduleReq.StartTime = startTime.Format("2006-01-02 15:04:05")
	createScheduleReq.EndTime = endTime.Format("2006-01-02 15:04:05")
	createScheduleReq.Crontab = "* */3 * * *"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)

	cronSchedule, err = cron.ParseStandard(createScheduleReq.Crontab)
	assert.Nil(t, err)

	getScheduleResp, err = GetSchedule(ctx, resp.ScheduleID, "", 0, nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, cronSchedule.Next(startTime).Format("2006-01-02 15:04:05"), getScheduleResp.NextRunTime)

	// 其他普通用户没有权限访问pipeline
	ctx = &logger.RequestContext{UserName: "another_user"}
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("user[another_user] has no access to resource[pipeline] with Name[ppl-000001]"), err)

	// 成功: root用户
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
}

func TestListSchedule(t *testing.T) {
	// todo
}

func TestGetSchedule(t *testing.T) {
	// todo
}

func TestStopSchedule(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	patch1 := gomonkey.ApplyFunc(SendSingnal, func(string, string) error {
		return nil
	})

	defer patch.Reset()
	defer patch1.Reset()

	// 创建 pipeline & pipelineDetail
	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: MockNormalUser,
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     MockNormalUser,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &ppl1, &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	// 连续创建2次schedule 成功
	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        "ppl-000001",
		PipelineDetailPk:  1,
		Crontab:           "* * * * */1",
		StartTime:         "",
		EndTime:           "",
		Concurrency:       10,
		ConcurrencyPolicy: "suspend",
		ExpireInterval:    100,
		Catchup:           true,
		FsName:            MockFsName,
		UserName:          "",
	}
	createResp1, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp1.ScheduleID, "schedule-000001")

	createResp2, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp2.ScheduleID, "schedule-000002")

	// 失败: schedule不存在
	err = StopSchedule(ctx, "schedule-000003")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000003] failed. schedule[schedule-000003] not found!", err.Error())

	// 失败: 用户没有权限
	ctx = &logger.RequestContext{UserName: "wrongUser"}
	err = StopSchedule(ctx, "schedule-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000001] failed. user[wrongUser] has no access to resource[schedule] with Name[schedule-000001]", err.Error())

	// 成功: 普通用户
	ctx = &logger.RequestContext{UserName: MockNormalUser}
	err = StopSchedule(ctx, "schedule-000001")
	assert.Nil(t, err)

	// 失败: 已经是终态
	ctx = &logger.RequestContext{UserName: MockNormalUser}
	err = StopSchedule(ctx, "schedule-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000001] failed, already in status[terminated]", err.Error())

	// 成功: root用户
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = StopSchedule(ctx, "schedule-000002")
	assert.Nil(t, err)

	// 失败: 已经是终态
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = StopSchedule(ctx, "schedule-000002")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000002] failed, already in status[terminated]", err.Error())
}

func TestDeleteSchedule(t *testing.T) {
	dbinit.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	patch1 := gomonkey.ApplyFunc(SendSingnal, func(string, string) error {
		return nil
	})

	defer patch.Reset()
	defer patch1.Reset()

	// 创建 pipeline & pipelineDetail
	ppl1 := models.Pipeline{
		Pk:       1,
		ID:       "ppl-000001",
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: MockNormalUser,
	}
	pplDetail1 := models.PipelineDetail{
		Pk:           1,
		DetailType:   pkgPplCommon.PplDetailTypeNormal,
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     MockNormalUser,
	}

	pplID1, pplDetailPk1, err := models.CreatePipeline(ctx.Logging(), &ppl1, &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.ID, pplID1)
	assert.Equal(t, pplDetail1.Pk, pplDetailPk1)

	// 连续创建2次schedule 成功
	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        "ppl-000001",
		PipelineDetailPk:  1,
		Crontab:           "* * * * */1",
		StartTime:         "",
		EndTime:           "",
		Concurrency:       10,
		ConcurrencyPolicy: "suspend",
		ExpireInterval:    100,
		Catchup:           true,
		FsName:            MockFsName,
		UserName:          "",
	}
	createResp1, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp1.ScheduleID, "schedule-000001")

	createResp2, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp2.ScheduleID, "schedule-000002")

	createResp3, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp3.ScheduleID, "schedule-000003")

	err = StopSchedule(ctx, createResp2.ScheduleID)
	assert.Nil(t, err)

	err = StopSchedule(ctx, createResp3.ScheduleID)
	assert.Nil(t, err)

	// 失败: schedule不存在
	err = DeleteSchedule(ctx, "schedule-000004")
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000004] failed. schedule[schedule-000004] not found!", err.Error())

	// 失败: 用户没有权限
	ctx = &logger.RequestContext{UserName: "wrongUser"}
	err = DeleteSchedule(ctx, "schedule-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000001] failed. user[wrongUser] has no access to resource[schedule] with Name[schedule-000001]", err.Error())

	// 失败: 不是终态
	ctx = &logger.RequestContext{UserName: MockNormalUser}
	err = DeleteSchedule(ctx, "schedule-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000001] in status[running] failed. only schedules in final status: [success failed terminated] can be deleted", err.Error())

	// 成功: 普通用户
	err = DeleteSchedule(ctx, createResp2.ScheduleID)
	assert.Nil(t, err)

	// 失败: 删除后已经不存在
	err = DeleteSchedule(ctx, createResp2.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000002] failed. schedule[schedule-000002] not found!", err.Error())

	// 成功: root用户
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = DeleteSchedule(ctx, createResp3.ScheduleID)
	assert.Nil(t, err)

	// 失败: 已经是终态
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = DeleteSchedule(ctx, createResp3.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000003] failed. schedule[schedule-000003] not found!", err.Error())
}
