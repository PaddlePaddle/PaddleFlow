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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	cron "github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser   = "root"
	MockNormalUser = "user1"
	MockFsName     = "mockFs"
	MockFsID       = "root-mockFs"
)

func insertPipeline(t *testing.T, logEntry *log.Entry) (pplID1, pplID2, pplDetailID1, pplDetailID2 string) {
	ppl1 := models.Pipeline{
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplDetail1 := models.PipelineDetail{
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	ppl2 := models.Pipeline{
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "root",
	}
	pplDetail2 := models.PipelineDetail{
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "root",
	}

	var err error
	pplID1, pplDetailID1, err = models.CreatePipeline(logEntry, &ppl1, &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplDetail1.Pk, int64(1))
	assert.Equal(t, pplDetailID1, pplDetail1.ID)
	assert.Equal(t, pplDetailID1, "1")
	assert.Equal(t, pplDetail1.PipelineID, ppl1.ID)

	pplID2, pplDetailID2, err = models.CreatePipeline(logEntry, &ppl2, &pplDetail2)
	assert.Nil(t, err)
	assert.Equal(t, ppl2.Pk, int64(2))
	assert.Equal(t, pplID2, ppl2.ID)
	assert.Equal(t, pplID2, "ppl-000002")

	assert.Equal(t, pplDetail2.Pk, int64(2))
	assert.Equal(t, pplDetailID2, pplDetail2.ID)
	assert.Equal(t, pplDetailID2, "1")
	assert.Equal(t, pplDetail2.PipelineID, ppl2.ID)

	return pplID1, pplID2, pplDetailID1, pplDetailID2
}

// 测试创建schedule
func TestCreateSchedule(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}
	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        "ppl-000001",
		PipelineDetailID:  "1",
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
	patch2 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})

	defer patch.Reset()
	defer patch1.Reset()
	defer patch2.Reset()

	// 创建 pipeline & pipelineDetail
	_, _, _, _ = insertPipeline(t, ctx.Logging())

	// 失败: schedule名称格式不支持
	createScheduleReq.Name = "-asdf"
	resp, err := CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("name[-asdf] for [schedule] does not compile with regex rule[^[A-Za-z_][A-Za-z0-9_]{1,49}$]"), err)

	// 失败: schedule名称格式不支持(长度超过50)
	createScheduleReq.Name = strings.Repeat("a", 51)
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("name[%s] for [schedule] does not compile with regex rule[^[A-Za-z_][A-Za-z0-9_]{1,49}$]", createScheduleReq.Name), err)

	// 失败: desc长度超过1024
	createScheduleReq.Name = "schedule_1"
	createScheduleReq.Desc = strings.Repeat("a", util.MaxDescLength+1)
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("desc too long, should be less than %d", util.MaxDescLength), err)

	// 失败: concurrency < 0
	createScheduleReq.Desc = "schedule test"
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
	assert.Equal(t, resp.ScheduleID, "schedule-000001")

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
	assert.Equal(t, fmt.Errorf("create schedule failed, starttime[%s] format not correct, should be YYYY-MM-DD hh:mm:ss", createScheduleReq.StartTime), err)

	// 失败，start time 时间早于当前时间
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
	assert.Equal(t, fmt.Errorf("create schedule failed, endtime[%s] format not correct, should be YYYY-MM-DD hh:mm:ss", createScheduleReq.EndTime), err)

	// 失败，end time 时间早于当前时间
	endTime := time.Now()
	createScheduleReq.EndTime = endTime.Format("2006-01-02 15:04:05")
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf("create schedule failed, endtime[%s] not after currentTime", createScheduleReq.EndTime))

	// 失败，endTime 时间早于 startTime
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
	assert.Equal(t, resp.ScheduleID, "schedule-000002")

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
	assert.Equal(t, resp.ScheduleID, "schedule-000003")

	cronSchedule, err = cron.ParseStandard(createScheduleReq.Crontab)
	assert.Nil(t, err)

	getScheduleResp, err = GetSchedule(ctx, resp.ScheduleID, "", 0, nil, nil)
	assert.Nil(t, err)
	assert.Equal(t, cronSchedule.Next(startTime).Format("2006-01-02 15:04:05"), getScheduleResp.NextRunTime)

	// 创建schedule失败，其他普通用户没有权限访问pipeline
	ctx = &logger.RequestContext{UserName: "another_user"}
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("user[another_user] has no access to resource[pipeline] with Name[ppl-000001]"), err)

	// 失败: pipelineID不存在
	ctx = &logger.RequestContext{UserName: MockRootUser}
	createScheduleReq.PipelineID = "notExistID"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create schedule failed, pipeline[notExistID] not exist"), err)

	// 失败: pipelineDetailID不存在
	createScheduleReq.PipelineID = "ppl-000001"
	createScheduleReq.PipelineDetailID = "2"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.NotNil(t, err)
	assert.Equal(t, fmt.Errorf("create schedule failed, pipeline[ppl-000001] detail[2] not exist"), err)

	// 成功: root用户
	createScheduleReq.PipelineDetailID = "1"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000004")
}

// todo: 测试marker不为空
func TestListSchedule(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}

	pplID1, _, pplDetailID1, _ := insertPipeline(t, ctx.Logging())

	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        pplID1,
		PipelineDetailID:  pplDetailID1,
		Crontab:           "* */3 * * *",
		StartTime:         "",
		EndTime:           "",
		Concurrency:       10,
		ConcurrencyPolicy: "suspend",
		ExpireInterval:    100,
		Catchup:           true,
		FsName:            MockFsName,
		UserName:          MockNormalUser,
	}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	patch1 := gomonkey.ApplyFunc(SendSingnal, func(string, string) error {
		return nil
	})
	patch2 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})

	defer patch.Reset()
	defer patch1.Reset()
	defer patch2.Reset()

	// 普通用户创建两个周期调度
	resp, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000001")

	createScheduleReq.Name = "schedule_2"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000002")

	// root用户创建三个周期调度
	ctx = &logger.RequestContext{UserName: MockRootUser}
	createScheduleReq.UserName = MockRootUser
	createScheduleReq.Name = "schedule_3"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000003")

	createScheduleReq.Name = "schedule_4"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000004")

	createScheduleReq.Name = "schedule_5"
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000005")

	// test list
	marker := ""
	maxKeys := 0
	pplFilter := []string{}
	pplDetailFilter := []string{}
	userFilter := []string{}
	scheduleFilter := []string{}
	nameFilter := []string{}
	statusFilter := []string{}
	ListScheduleResp, err := ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.ScheduleList[2].ID, "schedule-000003")
	assert.Equal(t, ListScheduleResp.ScheduleList[3].ID, "schedule-000004")
	assert.Equal(t, ListScheduleResp.ScheduleList[4].ID, "schedule-000005")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ := json.Marshal(ListScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	pplFilter = []string{"notExistPplID"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	pplFilter = []string{pplID1}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.ScheduleList[2].ID, "schedule-000003")
	assert.Equal(t, ListScheduleResp.ScheduleList[3].ID, "schedule-000004")
	assert.Equal(t, ListScheduleResp.ScheduleList[4].ID, "schedule-000005")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定maxkeys
	maxKeys = 2
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.IsTruncated, true)
	assert.NotEqual(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定userfilter
	maxKeys = 0
	userFilter = []string{MockNormalUser, "another_user"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, 指定userfilter
	userFilter = []string{MockRootUser}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000003")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000004")
	assert.Equal(t, ListScheduleResp.ScheduleList[2].ID, "schedule-000005")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// test list，user非root时，指定userfilter时会报错
	ctx = &logger.RequestContext{UserName: MockNormalUser}
	userFilter = []string{MockRootUser}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.NotNil(t, err)
	assert.Equal(t, "only root user can set userFilter!", err.Error())
	println("")
	fmt.Printf("%s\n", b)

	// test list, 普通用户不指定userfilter，只返回自己有权限的schedule
	userFilter = []string{}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// test list, ppldetail filter
	// 传入不存在的 ppldetail 不会报错
	// 注意不存在匹配记录时，istruncated = false
	ctx = &logger.RequestContext{UserName: MockRootUser}
	pplDetailFilter = []string{"2", "notExistPplDetailID"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)

	// 传入存在的ppldetail, 正确过滤
	pplDetailFilter = []string{"1"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.ScheduleList[2].ID, "schedule-000003")
	assert.Equal(t, ListScheduleResp.ScheduleList[3].ID, "schedule-000004")
	assert.Equal(t, ListScheduleResp.ScheduleList[4].ID, "schedule-000005")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)

	// test list, schedule filter
	// 先测试不能匹配前缀，传入不存在的scheduleID不会报错
	// 注意不存在匹配记录时，istruncated = false
	scheduleFilter = []string{"schedule-000", "schedule-hahah"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)

	// 传入存在的scheduleID, 正确过滤
	scheduleFilter = []string{"schedule-000001", "schedule-000002"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)

	// test list, name filter
	// 先测试不能匹配前缀，传入不存在的 name 不会报错
	// 注意不存在匹配记录时，istruncated = false
	scheduleFilter = []string{}
	nameFilter = []string{"schedule_", "schedule_asdd"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)

	// 传入存在的schedule name, 正确过滤
	nameFilter = []string{"schedule_2", "schedule_4"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000004")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)

	// test list, status filter
	// 先测试不能匹配前缀，传入不存在的 status 不会报错
	// 注意不存在匹配记录时，istruncated = false
	nameFilter = []string{}
	statusFilter = []string{models.ScheduleStatusSuccess, "notExistStatus"}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)

	// 传入存在的schedule status, 正确过滤
	statusFilter = []string{models.ScheduleStatusRunning}
	ListScheduleResp, err = ListSchedule(ctx, marker, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(ListScheduleResp.ScheduleList))
	assert.Equal(t, ListScheduleResp.ScheduleList[0].ID, "schedule-000001")
	assert.Equal(t, ListScheduleResp.ScheduleList[1].ID, "schedule-000002")
	assert.Equal(t, ListScheduleResp.ScheduleList[2].ID, "schedule-000003")
	assert.Equal(t, ListScheduleResp.ScheduleList[3].ID, "schedule-000004")
	assert.Equal(t, ListScheduleResp.ScheduleList[4].ID, "schedule-000005")
	assert.Equal(t, ListScheduleResp.IsTruncated, false)
	assert.Equal(t, ListScheduleResp.NextMarker, "")
	b, _ = json.Marshal(ListScheduleResp)
}

// todo: 测试使用marker，以及maxKeys，runFilter, statusFilter 等参数过滤 run列表
func TestGetSchedule(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}
	pplID1, _, pplDetailID1, _ := insertPipeline(t, ctx.Logging())

	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        pplID1,
		PipelineDetailID:  pplDetailID1,
		Crontab:           "* */3 * * *",
		StartTime:         "",
		EndTime:           "",
		Concurrency:       10,
		ConcurrencyPolicy: "suspend",
		ExpireInterval:    100,
		Catchup:           true,
		FsName:            MockFsName,
		UserName:          MockNormalUser,
	}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	patch1 := gomonkey.ApplyFunc(SendSingnal, func(string, string) error {
		return nil
	})
	patch2 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})

	defer patch.Reset()
	defer patch1.Reset()
	defer patch2.Reset()

	// 普通用户创建一个周期调度
	resp, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000001")

	// root用户创建一个周期调度
	ctx = &logger.RequestContext{UserName: MockRootUser}
	resp, err = CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, resp.ScheduleID, "schedule-000002")

	// root 用户查询普通用户创建的schedule，成功
	scheduleID := "schedule-000001"
	marker := ""
	maxKeys := 0
	runFilter := []string{}
	statusFilter := []string{}
	getScheduleResp, err := GetSchedule(ctx, scheduleID, marker, maxKeys, runFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(getScheduleResp.ListRunResponse.RunList))
	assert.Equal(t, getScheduleResp.ListRunResponse.IsTruncated, false)
	assert.Equal(t, getScheduleResp.ListRunResponse.NextMarker, "")
	b, _ := json.Marshal(getScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// root 用户查询 root 用户创建的schedule，成功
	scheduleID = "schedule-000002"
	getScheduleResp, err = GetSchedule(ctx, scheduleID, marker, maxKeys, runFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(getScheduleResp.ListRunResponse.RunList))
	assert.Equal(t, getScheduleResp.ListRunResponse.IsTruncated, false)
	assert.Equal(t, getScheduleResp.ListRunResponse.NextMarker, "")
	b, _ = json.Marshal(getScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// 普通 用户查询 自己 创建的schedule，成功
	ctx = &logger.RequestContext{UserName: MockNormalUser}
	scheduleID = "schedule-000001"
	getScheduleResp, err = GetSchedule(ctx, scheduleID, marker, maxKeys, runFilter, statusFilter)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(getScheduleResp.ListRunResponse.RunList))
	assert.Equal(t, getScheduleResp.ListRunResponse.IsTruncated, false)
	assert.Equal(t, getScheduleResp.ListRunResponse.NextMarker, "")
	b, _ = json.Marshal(getScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// 普通 用户查询 root用户 创建的schedule，失败
	scheduleID = "schedule-000002"
	getScheduleResp, err = GetSchedule(ctx, scheduleID, marker, maxKeys, runFilter, statusFilter)
	assert.NotNil(t, err)
	assert.Equal(t, "get schedule[schedule-000002] failed. err:user[user1] has no access to resource[schedule] with Name[schedule-000002]", err.Error())
	b, _ = json.Marshal(getScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// 普通 用户查询 其他普通用户 创建的schedule，失败
	ctx = &logger.RequestContext{UserName: "another_user"}
	scheduleID = "schedule-000001"
	getScheduleResp, err = GetSchedule(ctx, scheduleID, marker, maxKeys, runFilter, statusFilter)
	assert.NotNil(t, err)
	assert.Equal(t, "get schedule[schedule-000001] failed. err:user[another_user] has no access to resource[schedule] with Name[schedule-000001]", err.Error())
	b, _ = json.Marshal(getScheduleResp)
	println("")
	fmt.Printf("%s\n", b)

	// 查询一个不存在的schedule，失败
	ctx = &logger.RequestContext{UserName: MockRootUser}
	scheduleID = "schedule-noExist"
	getScheduleResp, err = GetSchedule(ctx, scheduleID, marker, maxKeys, runFilter, statusFilter)
	assert.NotNil(t, err)
	assert.Equal(t, "get schedule[schedule-noExist] failed. err:schedule[schedule-noExist] not found!", err.Error())
	b, _ = json.Marshal(getScheduleResp)
	println("")
	fmt.Printf("%s\n", b)
}

func TestStopSchedule(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	patch1 := gomonkey.ApplyFunc(SendSingnal, func(string, string) error {
		return nil
	})
	patch2 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})

	defer patch.Reset()
	defer patch1.Reset()
	defer patch2.Reset()

	// 创建 pipeline & pipelineDetail
	pplID1, _, pplDetailID1, _ := insertPipeline(t, ctx.Logging())

	// 连续创建2次schedule 成功
	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        pplID1,
		PipelineDetailID:  pplDetailID1,
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

	// 普通用户创建两个schedule
	createResp1, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp1.ScheduleID, "schedule-000001")

	createResp2, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp2.ScheduleID, "schedule-000002")

	// root 用户创建一个schedule
	ctx = &logger.RequestContext{UserName: MockRootUser}
	createResp3, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp3.ScheduleID, "schedule-000003")

	// 失败: schedule不存在
	err = StopSchedule(ctx, "schedule-000004")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000004] failed. schedule[schedule-000004] not found!", err.Error())

	// root用户 stop 普通用户创建的schedule 成功
	err = StopSchedule(ctx, "schedule-000002")
	assert.Nil(t, err)

	// 失败: 已经是终态
	err = StopSchedule(ctx, "schedule-000002")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000002] failed, already in status[terminated]", err.Error())

	// root用户 stop 自己创建的schedule 成功
	err = StopSchedule(ctx, "schedule-000003")
	assert.Nil(t, err)

	// 失败: 普通用户没有权限stop其他普通用户的schedule
	ctx = &logger.RequestContext{UserName: "wrongUser"}
	err = StopSchedule(ctx, "schedule-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000001] failed. user[wrongUser] has no access to resource[schedule] with Name[schedule-000001]", err.Error())

	// 成功: 普通用户stop自己创建的schedule
	ctx = &logger.RequestContext{UserName: MockNormalUser}
	err = StopSchedule(ctx, "schedule-000001")
	assert.Nil(t, err)

	// 失败: schedule已经是终态
	err = StopSchedule(ctx, "schedule-000001")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000001] failed, already in status[terminated]", err.Error())

	// 失败: 普通用户没有root用户创建的schedule权限
	err = StopSchedule(ctx, "schedule-000003")
	assert.NotNil(t, err)
	assert.Equal(t, "stop schedule[schedule-000003] failed. user[user1] has no access to resource[schedule] with Name[schedule-000003]", err.Error())
}

func TestDeleteSchedule(t *testing.T) {
	driver.InitMockDB()
	ctx := &logger.RequestContext{UserName: MockNormalUser}

	patch := gomonkey.ApplyFunc(handler.ReadFileFromFs, func(fsID, runYamlPath string, logEntry *log.Entry) ([]byte, error) {
		return os.ReadFile(runYamlPath)
	})
	patch1 := gomonkey.ApplyFunc(SendSingnal, func(string, string) error {
		return nil
	})
	patch2 := gomonkey.ApplyFunc(CheckFsAndGetID, func(string, string, string) (string, error) {
		return "", nil
	})

	defer patch.Reset()
	defer patch1.Reset()
	defer patch2.Reset()

	// 创建 pipeline & pipelineDetail
	pplID1, _, pplDetailID1, _ := insertPipeline(t, ctx.Logging())

	// 连续创建2次schedule 成功
	createScheduleReq := CreateScheduleRequest{
		Name:              "schedule_1",
		Desc:              "schedule test",
		PipelineID:        pplID1,
		PipelineDetailID:  pplDetailID1,
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

	// 普通用户创建三个schedule
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

	// root用户创建一个schedule
	ctx = &logger.RequestContext{UserName: MockRootUser}
	createResp4, err := CreateSchedule(ctx, &createScheduleReq)
	assert.Nil(t, err)
	assert.Equal(t, createResp4.ScheduleID, "schedule-000004")

	err = StopSchedule(ctx, createResp4.ScheduleID)
	assert.Nil(t, err)

	// 失败: schedule不存在
	err = DeleteSchedule(ctx, "schedule-000005")
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000005] failed. schedule[schedule-000005] not found!", err.Error())

	// 失败: 不是终态
	err = DeleteSchedule(ctx, createResp1.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000001] in status[running] failed. only schedules in final status: [success failed terminated] can be deleted", err.Error())

	// 失败: 普通用户没有其他普通用户创建的schedule权限
	ctx = &logger.RequestContext{UserName: "wrongUser"}
	err = DeleteSchedule(ctx, createResp1.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000001] failed. user[wrongUser] has no access to resource[schedule] with Name[schedule-000001]", err.Error())

	// 失败: 普通用户没有root用户创建的schedule权限
	ctx = &logger.RequestContext{UserName: MockNormalUser}
	err = DeleteSchedule(ctx, createResp4.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000004] failed. user[user1] has no access to resource[schedule] with Name[schedule-000004]", err.Error())

	// 成功: 普通用户删除自己创建的schedule
	err = DeleteSchedule(ctx, createResp2.ScheduleID)
	assert.Nil(t, err)

	// 失败: 删除后已经不存在
	err = DeleteSchedule(ctx, createResp2.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000002] failed. schedule[schedule-000002] not found!", err.Error())

	// 成功: root用户删除普通用户创建的schedule
	ctx = &logger.RequestContext{UserName: MockRootUser}
	err = DeleteSchedule(ctx, createResp3.ScheduleID)
	assert.Nil(t, err)

	// 失败: 已经是终态
	err = DeleteSchedule(ctx, createResp3.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000003] failed. schedule[schedule-000003] not found!", err.Error())

	// 成功: root用户删除自己创建的schedule
	err = DeleteSchedule(ctx, createResp4.ScheduleID)
	assert.Nil(t, err)

	// 失败: 已经是终态
	err = DeleteSchedule(ctx, createResp4.ScheduleID)
	assert.NotNil(t, err)
	assert.Equal(t, "delete schedule[schedule-000004] failed. schedule[schedule-000004] not found!", err.Error())
}
