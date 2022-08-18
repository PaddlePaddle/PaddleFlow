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
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

const (
	MockRootUser   = "root"
	MockNormalUser = "user1"
	MockFsName     = "mockFs"
	MockFsID       = "root-mockFs"
)

func insertPipeline(t *testing.T, logEntry *log.Entry) (pplID1, pplID2, pplVersionID1, pplVersionID2 string) {
	pplYaml, err := os.ReadFile("../../../../example/pipeline/base_pipeline/run.yaml")
	assert.Nil(t, err)

	ppl1 := model.Pipeline{
		Name:     "ppl1",
		Desc:     "ppl1",
		UserName: "user1",
	}
	pplVersion1 := model.PipelineVersion{
		FsID:         "user1-fsname",
		FsName:       "fsname",
		YamlPath:     "./run.yml",
		PipelineYaml: string(pplYaml),
		PipelineMd5:  "md5_1",
		UserName:     "user1",
	}

	ppl2 := model.Pipeline{
		Name:     "ppl2",
		Desc:     "ppl2",
		UserName: "root",
	}
	pplVersion2 := model.PipelineVersion{
		FsID:         "root-fsname2",
		FsName:       "fsname2",
		YamlPath:     "./run.yml",
		PipelineYaml: string(pplYaml),
		PipelineMd5:  "md5_2",
		UserName:     "root",
	}

	pplID1, pplVersionID1, err = storage.Pipeline.CreatePipeline(logEntry, &ppl1, &pplVersion1)
	assert.Nil(t, err)
	assert.Equal(t, ppl1.Pk, int64(1))
	assert.Equal(t, pplID1, ppl1.ID)
	assert.Equal(t, pplID1, "ppl-000001")

	assert.Equal(t, pplVersion1.Pk, int64(1))
	assert.Equal(t, pplVersionID1, pplVersion1.ID)
	assert.Equal(t, pplVersionID1, "1")
	assert.Equal(t, pplVersion1.PipelineID, ppl1.ID)

	pplID2, pplVersionID2, err = storage.Pipeline.CreatePipeline(logEntry, &ppl2, &pplVersion2)
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

// ------ 周期调度逻辑需要的函数 ------

// 测试创建schedule expire interval 参数
func TestExpireInterval(t *testing.T) {
	driver.InitMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, _, pplVersionID1, _ := insertPipeline(t, logEntry)

	// 开启catchup，设置expireinterval = 60s（1min）, 同时设置开始的 NextRunAt 为 2min前，周期频率为1/min
	// 有两次nextRunAt会被校验，最终第一次会被过滤掉，只有第二次才会被加进execMap
	duration, err := time.ParseDuration("-2m")
	assert.Nil(t, err)

	catchup := true
	expire_interval := 60
	concurrency := 0
	concurrencyPolicy := models.ConcurrencyPolicySuspend
	scheduleOptions, err := models.NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	strOptions, err := scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	fsConfig := models.FsConfig{Username: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := models.Schedule{
		ID:                "", // to be backfilled according to db pk
		Name:              "schedule1",
		Desc:              "schedule1",
		PipelineID:        pplID1,
		PipelineVersionID: pplVersionID1,
		UserName:          "user1",
		FsConfig:          StrFsConfig,
		Crontab:           "*/1 * * * *",
		Options:           strOptions,
		Status:            models.ScheduleStatusRunning,
		StartAt:           sql.NullTime{},
		EndAt:             sql.NullTime{},
		NextRunAt:         time.Now().Add(duration),
	}
	log.Infof("start nextRunAt: %s", schedule.NextRunAt.Format("2006-01-02 15:04:05"))
	schedID, err := models.CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000001")

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	assert.Nil(t, err)

	patch1 := gomonkey.ApplyFunc(checkFs, func(string, *schema.WorkflowSource) error {
		return nil
	})
	patch2 := gomonkey.ApplyFunc(StartWf, func(models.Run, *pipeline.Workflow) error {
		return nil
	})
	defer patch1.Reset()
	defer patch2.Reset()

	scheduler := Scheduler{}
	nextWakeupTime, err := scheduler.dealWithTimeout()
	assert.Nil(t, err)
	nextRunAt := cronSchedule.Next(cronSchedule.Next(cronSchedule.Next(schedule.NextRunAt)))
	assert.Equal(t, nextWakeupTime.Equal(nextRunAt), true)

	// test cron schedule
	scheduleIDList := []string{schedID}
	runs, err := models.ListRun(logger.Logger(), 0, 0, []string{}, []string{}, []string{}, []string{}, []string{}, scheduleIDList)
	assert.Nil(t, err)
	assert.Equal(t, len(runs), 3)
	nextRunAt = schedule.NextRunAt
	assert.Equal(t, runs[0].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[0].Status, common.StatusRunSkipped)
	nextRunAt = cronSchedule.Next(schedule.NextRunAt)
	assert.Equal(t, runs[1].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[1].Status, common.StatusRunSkipped)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, runs[2].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[2].Status, common.StatusRunInitiating)

	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")
}

// 测试创建schedule endtime 参数
// 此处不测试starttime参数，因为start time只用于create schedule时，确定nextRunAt的值，该功能在createSchedule会测试
func TestScheduleTime(t *testing.T) {
	driver.InitMockDB()
	logEntry := log.WithFields(log.Fields{})
	pplID1, _, pplVersionID1, _ := insertPipeline(t, logEntry)

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
	concurrencyPolicy := models.ConcurrencyPolicySuspend
	scheduleOptions, err := models.NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	strOptions, err := scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	fsConfig := models.FsConfig{Username: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := models.Schedule{
		ID:                "", // to be back filled according to db pk
		Name:              "schedule1",
		Desc:              "schedule1",
		PipelineID:        pplID1,
		PipelineVersionID: pplVersionID1,
		UserName:          "user1",
		FsConfig:          StrFsConfig,
		Crontab:           "*/1 * * * *",
		Options:           strOptions,
		Status:            models.ScheduleStatusRunning,
		StartAt:           sql.NullTime{},
		EndAt:             sql.NullTime{Time: time.Now().Add(endTimeDuration), Valid: true},
		NextRunAt:         time.Now().Add(NextRunAtDuration),
	}

	log.Infof("start nextRunAt: %s", schedule.NextRunAt.Format("2006-01-02 15:04:05"))
	log.Infof("start endTime: %s", schedule.EndAt.Time.Format("2006-01-02 15:04:05"))
	schedID, err := models.CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000001")

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	assert.Nil(t, err)

	patch1 := gomonkey.ApplyFunc(checkFs, func(string, *schema.WorkflowSource) error {
		return nil
	})
	patch2 := gomonkey.ApplyFunc(StartWf, func(models.Run, *pipeline.Workflow) error {
		return nil
	})
	defer patch1.Reset()
	defer patch2.Reset()

	scheduler := Scheduler{}
	nextWakeupTime, err := scheduler.dealWithTimeout()
	assert.Nil(t, err)
	assert.Nil(t, nextWakeupTime)

	// test cron schedule
	scheduleIDList := []string{schedID}
	runs, err := models.ListRun(logger.Logger(), 0, 0, []string{}, []string{}, []string{}, []string{}, []string{}, scheduleIDList)
	assert.Nil(t, err)
	assert.Equal(t, len(runs), 2)
	nextRunAt := schedule.NextRunAt
	assert.Equal(t, runs[0].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[0].Status, common.StatusRunInitiating)
	nextRunAt = cronSchedule.Next(schedule.NextRunAt)
	assert.Equal(t, runs[1].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[1].Status, common.StatusRunInitiating)

	log.Infof("nextRunAt: %s", nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	schedule, err = models.GetSchedule(logEntry, schedID)
	assert.Nil(t, err)
	assert.Equal(t, schedule.Status, models.ScheduleStatusSuccess)
}

// 测试创建schedule concurrency, 以及concurrencyPolicy 参数
func TestConcurrency(t *testing.T) {
	driver.InitMockDB()
	logEntry := log.WithFields(log.Fields{})

	patch1 := gomonkey.ApplyFunc(checkFs, func(string, *schema.WorkflowSource) error {
		return nil
	})
	patch2 := gomonkey.ApplyFunc(StartWf, func(models.Run, *pipeline.Workflow) error {
		return nil
	})
	defer patch1.Reset()
	defer patch2.Reset()

	pplID1, _, pplVersionID1, _ := insertPipeline(t, logEntry)

	// 开启catchup，设置expireinterval = 0（没有expire限制）, 同时设置开始的 NextRunAt 为 2min前，周期频率为1/min
	// concurrency = 1，且policy = suspend，所以只校验并发起第一个run
	// 同时，因为被suspend了，所以nextWakeupTime 为nil
	duration, err := time.ParseDuration("-2m")
	assert.Nil(t, err)

	catchup := true
	expire_interval := 0
	concurrency := 1
	concurrencyPolicy := models.ConcurrencyPolicySuspend
	scheduleOptions, err := models.NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	strOptions, err := scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	fsConfig := models.FsConfig{Username: "user1"}
	StrFsConfig, err := fsConfig.Encode(logEntry)
	assert.Nil(t, err)

	schedule := models.Schedule{
		ID:                "", // to be back filled according to db pk
		Name:              "schedule1",
		Desc:              "schedule1",
		PipelineID:        pplID1,
		PipelineVersionID: pplVersionID1,
		UserName:          "user1",
		FsConfig:          StrFsConfig,
		Crontab:           "*/1 * * * *",
		Options:           strOptions,
		Status:            models.ScheduleStatusRunning,
		StartAt:           sql.NullTime{},
		EndAt:             sql.NullTime{},
		NextRunAt:         time.Now().Add(duration),
	}
	log.Infof("start nextRunAt: %s", schedule.NextRunAt.Format("2006-01-02 15:04:05"))
	schedID, err := models.CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000001")

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	assert.Nil(t, err)

	scheduler := Scheduler{}
	nextWakeupTime, err := scheduler.dealWithTimeout()
	assert.Nil(t, err)

	// test cron schedule
	scheduleIDList := []string{schedID}
	runs, err := models.ListRun(logger.Logger(), 0, 0, []string{}, []string{}, []string{}, []string{}, []string{}, scheduleIDList)
	assert.Nil(t, err)
	assert.Equal(t, len(runs), 1)
	nextRunAt := schedule.NextRunAt
	assert.Equal(t, runs[0].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[0].Status, common.StatusRunInitiating)

	nextRunAt = cronSchedule.Next(schedule.NextRunAt)
	assert.Nil(t, nextWakeupTime)

	log.Infof("nextRunAt: %s", nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// ----
	// 将concurrency设置为10，足够大，concurrencyPolicy设置为suspend，所有到时间的都会被执行
	// ----

	err = models.DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	concurrency = 10
	scheduleOptions, err = models.NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now().Add(duration)
	schedID, err = models.CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000002")

	nextWakeupTime, err = scheduler.dealWithTimeout()
	assert.Nil(t, err)

	// test cron schedule
	scheduleIDList = []string{schedID}
	runs, err = models.ListRun(logger.Logger(), 0, 0, []string{}, []string{}, []string{}, []string{}, []string{}, scheduleIDList)
	assert.Nil(t, err)
	assert.Equal(t, len(runs), 3)
	nextRunAt = schedule.NextRunAt
	assert.Equal(t, runs[0].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[0].Status, common.StatusRunInitiating)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, runs[1].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[1].Status, common.StatusRunInitiating)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, runs[2].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[2].Status, common.StatusRunInitiating)

	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.NotNil(t, nextWakeupTime)
	assert.Equal(t, nextWakeupTime.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))

	log.Infof("nextRunAt: %s", nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")
	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*&nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// 将concurrency设置为1，concurrencyPolicy设置为skip
	// 只有第一个到期任务会被执行，后面的都会被skip掉，而且nextRunAt会更新到第一个以后的时间
	err = models.DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	concurrency = 1
	concurrencyPolicy = models.ConcurrencyPolicySkip
	scheduleOptions, err = models.NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now().Add(duration)
	schedID, err = models.CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000003")

	nextWakeupTime, err = scheduler.dealWithTimeout()
	assert.Nil(t, err)

	// test cron schedule
	scheduleIDList = []string{schedID}
	runs, err = models.ListRun(logger.Logger(), 0, 0, []string{}, []string{}, []string{}, []string{}, []string{}, scheduleIDList)
	assert.Nil(t, err)
	assert.Equal(t, len(runs), 3)
	nextRunAt = schedule.NextRunAt
	assert.Equal(t, runs[0].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[0].Status, common.StatusRunInitiating)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, runs[1].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[1].Status, common.StatusRunSkipped)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, runs[2].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[2].Status, common.StatusRunSkipped)

	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.NotNil(t, nextWakeupTime)
	assert.Equal(t, nextWakeupTime.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))

	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*&nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// 将concurrency设置为1，concurrencyPolicy设置为replace
	// 只有最后一个到期任务会被执行，前面的都会被skip掉，而且nextRunAt会更新到第一个以后的时间
	err = models.DeleteSchedule(logEntry, schedID)
	assert.Nil(t, err)

	concurrency = 1
	concurrencyPolicy = models.ConcurrencyPolicyReplace
	scheduleOptions, err = models.NewScheduleOptions(logEntry, catchup, expire_interval, concurrency, concurrencyPolicy)
	assert.Nil(t, err)

	schedule.Options, err = scheduleOptions.Encode(logEntry)
	assert.Nil(t, err)

	schedule.NextRunAt = time.Now().Add(duration)
	schedID, err = models.CreateSchedule(logEntry, schedule)
	assert.Nil(t, err)
	assert.Equal(t, schedID, "schedule-000004")

	nextWakeupTime, err = scheduler.dealWithTimeout()
	assert.Nil(t, err)

	// test cron schedule
	scheduleIDList = []string{schedID}
	runs, err = models.ListRun(logger.Logger(), 0, 0, []string{}, []string{}, []string{}, []string{}, []string{}, scheduleIDList)
	assert.Nil(t, err)
	assert.Equal(t, len(runs), 3)
	nextRunAt = schedule.NextRunAt
	assert.Equal(t, runs[0].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[0].Status, common.StatusRunSkipped)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, runs[1].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[1].Status, common.StatusRunSkipped)
	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.Equal(t, runs[2].ScheduledAt.Time.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	assert.Equal(t, runs[2].Status, common.StatusRunInitiating)

	nextRunAt = cronSchedule.Next(nextRunAt)
	assert.NotNil(t, nextWakeupTime)
	assert.Equal(t, nextWakeupTime.Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))

	log.Infof("nextWakeupTime: %s, nextRunAt: %s", (*&nextWakeupTime).Format("2006-01-02 15:04:05"), nextRunAt.Format("2006-01-02 15:04:05"))
	println("\n")

	// 带测试：concurrencyPolicy是replace，而且有运行中的任务
}
