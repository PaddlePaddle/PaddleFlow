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
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const (
	MockRootUser   = "root"
	MockNormalUser = "user1"
	MockFsName     = "mockFs"
	MockFsID       = "root-mockFs"
)

func insertPipeline(t *testing.T, logEntry *log.Entry) (pplID1, pplID2, pplDetailID1, pplDetailID2 string) {
	ppl1 := Pipeline{
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplDetail1 := PipelineDetail{
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	ppl2 := Pipeline{
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "root",
	}
	pplDetail2 := PipelineDetail{
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: "ddddd",
		PipelineMd5:  "md5_2",
		UserName:     "root",
	}

	var err error
	pplID1, pplDetailID1, err = CreatePipeline(logEntry, &ppl1, &pplDetail1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplDetail1.Pk, int64(1))
	assert.Equal(t, pplDetailID1, pplDetail1.ID)
	assert.Equal(t, pplDetailID1, "1")
	assert.Equal(t, pplDetail1.PipelineID, ppl1.ID)

	pplID2, pplDetailID2, err = CreatePipeline(logEntry, &ppl2, &pplDetail2)
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

// ------ job / fs模块需要的函数 ------
func TestGetUsedFsIDs(t *testing.T) {
	initMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, _, pplDetailID1, _ := insertPipeline(t, logEntry)

	fsConfig := FsConfig{GlobalFsName: "fsname", UserName: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := Schedule{
		ID:               "", // to be back filled according to db pk
		Name:             "schedule1",
		Desc:             "schedule1",
		PipelineID:       pplID1,
		PipelineDetailID: pplDetailID1,
		UserName:         MockRootUser,
		FsConfig:         StrFsConfig,
		Crontab:          "*/5 * * * *",
		Options:          "{}",
		Status:           ScheduleStatusRunning,
		StartAt:          sql.NullTime{},
		EndAt:            sql.NullTime{},
		NextRunAt:        time.Now(),
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
	assert.Equal(t, 1, len(fsIDMap))
	print(fsIDMap)

	// fsconfig中不包含user，使用默认的“”
	fsConfig = FsConfig{GlobalFsName: "fsname"}
	StrFsConfig, err = fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule.FsConfig = StrFsConfig
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000002")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(fsIDMap))
	print(fsIDMap)

	// 创建 success 状态的schedule
	fsConfig = FsConfig{GlobalFsName: "fsname", UserName: "another_user"}
	StrFsConfig, err = fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule.FsConfig = StrFsConfig
	schedule.Status = ScheduleStatusSuccess
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000003")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(fsIDMap))

	// 创建 failed 状态的schedule
	schedule.Status = ScheduleStatusFailed
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000004")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(fsIDMap))

	// 创建 terminated 状态的schedule
	schedule.Status = ScheduleStatusTerminated
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000005")

	fsIDMap, err = ScheduleUsedFsIDs()
	assert.Nil(t, err)
	assert.Equal(t, 2, len(fsIDMap))
}

// ------ 周期调度逻辑需要的函数 ------

// 测试创建schedule catchup 参数
func TestCatchup(t *testing.T) {
	initMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, _, pplDetailID1, _ := insertPipeline(t, logEntry)

	fsConfig := FsConfig{GlobalFsName: "fsname", UserName: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := Schedule{
		ID:               "", // to be back filled according to db pk
		Name:             "schedule1",
		Desc:             "schedule1",
		PipelineID:       pplID1,
		PipelineDetailID: pplDetailID1,
		UserName:         "user1",
		FsConfig:         StrFsConfig,
		Crontab:          "*/5 * * * *",
		Options:          "{}",
		Status:           ScheduleStatusRunning,
		StartAt:          sql.NullTime{},
		EndAt:            sql.NullTime{},
		NextRunAt:        time.Now(),
	}
	schedID, err := CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000001")

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	assert.Nil(t, err)

	// test list
	checkCatchup := false
	killMap, execMap, nextWakeupTime, err := GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, nextWakeupTime.Equal(cronSchedule.Next(schedule.NextRunAt)), true)
	println("\n")

	// checkCatchup设置为true，同时catchup设置为true，最终结果跟checkCatchup设置为false一致
	err = DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	catchup := true
	expire_interval := 0
	concurrency := 0
	concurrencyPolicy := ConcurrencyPolicySuspend
	scheduleOptions, err := NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now()
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000002")

	checkCatchup = true
	killMap, execMap, nextWakeupTime, err = GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, nextWakeupTime.Equal(cronSchedule.Next(schedule.NextRunAt)), true)
	println("\n")

	// checkCatchup设置为true，同时catchup设置为false
	err = DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	catchup = false
	expire_interval = 0
	concurrency = 0
	concurrencyPolicy = ConcurrencyPolicySuspend
	scheduleOptions, err = NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now()
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000003")

	checkCatchup = true
	killMap, execMap, nextWakeupTime, err = GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 0)
	assert.Equal(t, nextWakeupTime.Equal(cronSchedule.Next(schedule.NextRunAt)), true)
	println("\n")
}

// 测试创建schedule expire interval 参数
func TestExpireInterval(t *testing.T) {
	initMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, _, pplDetailID1, _ := insertPipeline(t, logEntry)

	// 开启catchup，设置expireinterval = 60s（1min）, 同时设置开始的 NextRunAt 为 2min前，周期频率为1/min
	// 有两次nextRunAt会被校验，最终第一次会被过滤掉，只有第二次才会被加进execMap
	duration, err := time.ParseDuration("-2m")
	assert.Nil(t, err)

	catchup := true
	expire_interval := 60
	concurrency := 0
	concurrencyPolicy := ConcurrencyPolicySuspend
	scheduleOptions, err := NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	strOptions, err := scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	fsConfig := FsConfig{GlobalFsName: "fsname", UserName: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := Schedule{
		ID:               "", // to be backfilled according to db pk
		Name:             "schedule1",
		Desc:             "schedule1",
		PipelineID:       pplID1,
		PipelineDetailID: pplDetailID1,
		UserName:         "user1",
		FsConfig:         StrFsConfig,
		Crontab:          "*/1 * * * *",
		Options:          strOptions,
		Status:           ScheduleStatusRunning,
		StartAt:          sql.NullTime{},
		EndAt:            sql.NullTime{},
		NextRunAt:        time.Now().Add(duration),
	}
	log.Infof("start nextRunAt: %s", schedule.NextRunAt.Format("2006-01-02 15:04:05"))
	schedID, err := CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000001")

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	assert.Nil(t, err)

	// test list
	checkCatchup := false
	killMap, execMap, nextWakeupTime, err := GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, len(execMap["schedule-000001"]), 1)
	nextRunAt := cronSchedule.Next(cronSchedule.Next(schedule.NextRunAt))
	assert.Equal(t, execMap["schedule-000001"][0].Equal(nextRunAt), true)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, nextWakeupTime.Equal(nextRunAt), true)

	log.Infof("execMap of schedule1 %v", execMap["schedule-000001"])
	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*&nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")
}

// 测试创建schedule concurrency, 以及concurrencyPolicy 参数
func TestConcurrency(t *testing.T) {
	initMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, _, pplDetailID1, _ := insertPipeline(t, logEntry)

	// 开启catchup，设置expireinterval = 0（没有expire限制）, 同时设置开始的 NextRunAt 为 2min前，周期频率为1/min
	// concurrency = 1，且policy = suspend，所以只校验一次nextRunAt，并被加入execMap
	duration, err := time.ParseDuration("-2m")
	assert.Nil(t, err)

	catchup := true
	expire_interval := 0
	concurrency := 1
	concurrencyPolicy := ConcurrencyPolicySuspend
	scheduleOptions, err := NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	strOptions, err := scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	fsConfig := FsConfig{GlobalFsName: "fsname", UserName: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := Schedule{
		ID:               "", // to be back filled according to db pk
		Name:             "schedule1",
		Desc:             "schedule1",
		PipelineID:       pplID1,
		PipelineDetailID: pplDetailID1,
		UserName:         "user1",
		FsConfig:         StrFsConfig,
		Crontab:          "*/1 * * * *",
		Options:          strOptions,
		Status:           ScheduleStatusRunning,
		StartAt:          sql.NullTime{},
		EndAt:            sql.NullTime{},
		NextRunAt:        time.Now().Add(duration),
	}
	log.Infof("start nextRunAt: %s", schedule.NextRunAt.Format("2006-01-02 15:04:05"))
	schedID, err := CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000001")

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	assert.Nil(t, err)

	checkCatchup := false
	killMap, execMap, nextWakeupTime, err := GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, len(execMap[schedID]), 1)
	nextRunAt := cronSchedule.Next(schedule.NextRunAt)
	assert.Nil(t, nextWakeupTime)

	log.Infof("execMap of schedule1 %v", execMap[schedID])
	log.Infof("nextRunAt: %s", nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// 将concurrency设置为10，足够大，concurrencyPolicy设置为suspend，所有到时间的都会被执行
	err = DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	concurrency = 10
	scheduleOptions, err = NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now().Add(duration)
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000002")

	checkCatchup = false
	killMap, execMap, nextWakeupTime, err = GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, len(execMap[schedID]), 3)
	nextRunAt = cronSchedule.Next(cronSchedule.Next(cronSchedule.Next(schedule.NextRunAt)))
	assert.NotNil(t, nextWakeupTime)
	assert.Equal(t, nextWakeupTime.Equal(nextRunAt), true)

	log.Infof("execMap of schedule1 %v", execMap[schedID])
	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*&nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// 将concurrency设置为1，concurrencyPolicy设置为skip
	// 只有第一个到期任务会被执行，后面的都会被skip掉，而且nextRunAt会更新到第一个以后的时间
	err = DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	concurrency = 1
	concurrencyPolicy = ConcurrencyPolicySkip
	scheduleOptions, err = NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now().Add(duration)
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000003")

	checkCatchup = false
	killMap, execMap, nextWakeupTime, err = GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, len(execMap[schedID]), 1)
	nextRunAt = cronSchedule.Next(cronSchedule.Next(cronSchedule.Next(schedule.NextRunAt)))
	assert.NotNil(t, nextWakeupTime)
	assert.Equal(t, nextWakeupTime.Equal(nextRunAt), true)

	log.Infof("execMap of schedule1 %v", execMap[schedID])
	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*&nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// 将concurrency设置为1，concurrencyPolicy设置为replace
	// 只有最后一个到期任务会被执行，前面的都会被skip掉，而且nextRunAt会更新到第一个以后的时间
	err = DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	concurrency = 1
	concurrencyPolicy = ConcurrencyPolicyReplace
	scheduleOptions, err = NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now().Add(duration)
	schedID, err = CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000004")

	checkCatchup = false
	killMap, execMap, nextWakeupTime, err = GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, len(execMap[schedID]), 1)
	nextRunAt = cronSchedule.Next(cronSchedule.Next(cronSchedule.Next(schedule.NextRunAt)))
	assert.NotNil(t, nextWakeupTime)
	assert.Equal(t, nextWakeupTime.Equal(nextRunAt), true)

	log.Infof("execMap of schedule1 %v", execMap[schedID])
	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*&nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// 带测试：concurrencyPolicy是replace，而且有运行中的任务
}

// 测试创建schedule endtime 参数
// 此处不测试starttime参数，因为start time只用于create schedule时，确定nextRunAt的值，该功能在createSchedule会测试
func TestScheduleTime(t *testing.T) {
	initMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, _, pplDetailID1, _ := insertPipeline(t, logEntry)

	// 开启catchup，设置expireinterval = 0（没有expire限制）,
	// concurrency = 0，且policy = suspend，即没有concurrency限制
	// 同时设置开始的 NextRunAt 为 2min前，周期频率为1/min
	NextRunAtDuration, err := time.ParseDuration("-2m")
	assert.Nil(t, err)

	endTimeDuration, err := time.ParseDuration("-1m")
	assert.Nil(t, err)

	catchup := true
	expire_interval := 0
	concurrency := 0
	concurrencyPolicy := ConcurrencyPolicySuspend
	scheduleOptions, err := NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	strOptions, err := scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	fsConfig := FsConfig{GlobalFsName: "fsname", UserName: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := Schedule{
		ID:               "", // to be back filled according to db pk
		Name:             "schedule1",
		Desc:             "schedule1",
		PipelineID:       pplID1,
		PipelineDetailID: pplDetailID1,
		UserName:         "user1",
		FsConfig:         StrFsConfig,
		Crontab:          "*/1 * * * *",
		Options:          strOptions,
		Status:           ScheduleStatusRunning,
		StartAt:          sql.NullTime{},
		EndAt:            sql.NullTime{Time: time.Now().Add(endTimeDuration), Valid: true},
		NextRunAt:        time.Now().Add(NextRunAtDuration),
	}
	log.Infof("start nextRunAt: %s", schedule.NextRunAt.Format("2006-01-02 15:04:05"))
	log.Infof("start endTime: %s", schedule.EndAt.Time.Format("2006-01-02 15:04:05"))
	schedID, err := CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000001")

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	assert.Nil(t, err)

	checkCatchup := false
	killMap, execMap, nextWakeupTime, err := GetAvailableSchedule(logEntry, checkCatchup)
	assert.Nil(t, err)
	assert.Equal(t, len(killMap), 0)
	assert.Equal(t, len(execMap), 1)
	assert.Equal(t, len(execMap[schedID]), 2)
	nextRunAt := cronSchedule.Next(cronSchedule.Next(schedule.NextRunAt))
	assert.Nil(t, nextWakeupTime)

	log.Infof("execMap of schedule1 %v", execMap[schedID])
	log.Infof("nextRunAt: %s", nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	schedule, err = GetSchedule(logEntry, schedID)
	assert.Nil(t, err)
	assert.Equal(t, schedule.Status, ScheduleStatusSuccess)
}
