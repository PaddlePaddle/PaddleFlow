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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	cron "github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
)

type Schedule struct {
	Pk               int64          `gorm:"primaryKey;autoIncrement;not null" json:"-"`
	ID               string         `gorm:"type:varchar(60);not null"         json:"scheduleID"`
	Name             string         `gorm:"type:varchar(60);not null"         json:"name"`
	Desc             string         `gorm:"type:varchar(1024);not null"       json:"desc"`
	PipelineID       string         `gorm:"type:varchar(60);not null"         json:"pipelineID"`
	PipelineDetailPk int64          `gorm:"type:bigint(20);not null"          json:"-"`
	UserName         string         `gorm:"type:varchar(60);not null"         json:"username"`
	FsID             string         `gorm:"type:varchar(60);not null"         json:"fsID"`
	FsName           string         `gorm:"type:varchar(60);not null"         json:"fsName"`
	Crontab          string         `gorm:"type:varchar(60);not null"         json:"crontab"`
	Options          string         `gorm:"type:text;size:65535"              json:"options"`
	Message          string         `gorm:"type:text;size:65535"              json:"scheduleMsg"`
	Status           string         `gorm:"type:varchar(32)"                  json:"status"`
	StartAt          sql.NullTime   `                                         json:"-"`
	EndAt            sql.NullTime   `                                         json:"-"`
	NextRunAt        time.Time      `                                         json:"-"`
	CreatedAt        time.Time      `                                         json:"-"`
	UpdatedAt        time.Time      `                                         json:"-"`
	DeletedAt        gorm.DeletedAt `gorm:"index"                             json:"-"`
}

type ScheduleOptions struct {
	Catchup           bool   `json:"catchup"`
	ExpireInterval    int    `json:"expire_interval"`
	Concurrency       int    `json:"concurrency"`
	ConcurrencyPolicy string `json:"concurrency_policy"`
}

func checkContains(val string, list []string) bool {
	for _, item := range list {
		if val == item {
			return true
		}
	}

	return false
}

func NewScheduleOptions(logEntry *log.Entry, catchup bool, expireInterval int, concurrency int, concurrencyPolicy string) (so ScheduleOptions, err error) {
	// 校验 concurrency
	if concurrency < 0 {
		errMsg := fmt.Sprintf("concurrency should not be negative")
		logEntry.Errorf(errMsg)
		return so, fmt.Errorf(errMsg)
	}

	// 校验 currencyPolicy
	if concurrencyPolicy == "" {
		concurrencyPolicy = ConcurrencyPolicySuspend
	}

	if !checkContains(concurrencyPolicy, ConcurrencyPolicyList) {
		errMsg := fmt.Sprintf("concurrency policy[%s] not supported", concurrencyPolicy)
		logEntry.Errorf(errMsg)
		return so, fmt.Errorf(errMsg)
	}

	// 校验expire interval
	if expireInterval < 0 {
		errMsg := fmt.Sprintf("expire interval should not be negative")
		logEntry.Errorf(errMsg)
		return so, fmt.Errorf(errMsg)
	}
	so = ScheduleOptions{
		Catchup:           catchup,
		ExpireInterval:    expireInterval,
		Concurrency:       concurrency,
		ConcurrencyPolicy: concurrencyPolicy,
	}

	return so, nil
}

func DecodeScheduleOptions(StrOptions string) (so ScheduleOptions, err error) {
	if err := json.Unmarshal([]byte(StrOptions), &so); err != nil {
		errMsg := fmt.Sprintf("decode scheduleOptions failed. error:%v", err)
		return ScheduleOptions{}, fmt.Errorf(errMsg)
	}

	return so, nil
}

func (so *ScheduleOptions) Encode(logEntry *log.Entry) (string, error) {
	strOptions, err := json.Marshal(so)
	if err != nil {
		logEntry.Errorf("encode scheduleOptions failed. error:%v", err)
		return "", err
	}

	return string(strOptions), nil
}

const (
	ConcurrencyPolicySuspend = "suspend"
	ConcurrencyPolicyReplace = "replace"
	ConcurrencyPolicySkip    = "skip"

	ScheduleStatusSuccess    = "success"
	ScheduleStatusRunning    = "running"
	ScheduleStatusFailed     = "failed"
	ScheduleStatusTerminated = "terminated"
)

var ConcurrencyPolicyList = []string{
	ConcurrencyPolicySuspend,
	ConcurrencyPolicyReplace,
	ConcurrencyPolicySkip,
}

var ScheduleStatusList = []string{
	ScheduleStatusSuccess,
	ScheduleStatusRunning,
	ScheduleStatusFailed,
	ScheduleStatusTerminated,
}

var ScheduleFinalStatusList = []string{
	ScheduleStatusSuccess,
	ScheduleStatusFailed,
	ScheduleStatusTerminated,
}

var ScheduleNotFinalStatusList = []string{
	ScheduleStatusRunning,
}

func (Schedule) TableName() string {
	return "schedule"
}

func CreateSchedule(logEntry *log.Entry, schedule Schedule) (scheduleID string, err error) {
	logEntry.Debugf("begin create schedule:%+v", schedule)
	err = withTransaction(database.DB, func(tx *gorm.DB) error {
		result := tx.Model(&Schedule{}).Create(&schedule)
		if result.Error != nil {
			logEntry.Errorf("create schedule failed. schedule:%v, error:%s",
				schedule, result.Error.Error())
			return result.Error
		}
		schedule.ID = common.PrefixSchedule + fmt.Sprintf("%06d", schedule.Pk)
		logEntry.Debugf("created schedule with pk[%d], scheduleID[%s]", schedule.Pk, schedule.ID)

		// update ID
		result = tx.Model(&Schedule{}).Where("pk = ?", schedule.Pk).Update("id", schedule.ID)
		if result.Error != nil {
			logEntry.Errorf("backfilling scheduleID failed. pk[%d], error:%v",
				schedule.Pk, result.Error)
			return result.Error
		}
		return nil
	})

	return schedule.ID, err
}

func ListSchedule(logEntry *log.Entry, pipelineID string, PipelineDetailPk, pk int64, maxKeys int, userFilter, fsFilter, scheduleFilter, nameFilter, statusFilter []string) ([]Schedule, error) {
	logEntry.Debugf("begin list schedule. ")
	tx := database.DB.Model(&Schedule{}).Where("pk > ?", pk).Where("pipeline_id = ?", pipelineID)

	if PipelineDetailPk != 0 {
		tx = tx.Where("pipeline_detail_pk = ?", PipelineDetailPk)
	}

	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(scheduleFilter) > 0 {
		tx = tx.Where("id IN (?)", scheduleFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}
	if len(statusFilter) > 0 {
		tx = tx.Where("status IN (?)", statusFilter)
	}

	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	var scheduleList []Schedule
	tx = tx.Find(&scheduleList)
	if tx.Error != nil {
		logEntry.Errorf("list schedule failed. Filters: user{%v}, fs{%v}, schedule{%v}, name{%v}, status{%v}, scheduleID{%v}. error:%s",
			userFilter, fsFilter, scheduleFilter, nameFilter, statusFilter, scheduleFilter, tx.Error.Error())
		return []Schedule{}, tx.Error
	}

	return scheduleList, nil
}

func GetLastSchedule(logEntry *log.Entry, pipelineID string) (Schedule, error) {
	logEntry.Debugf("get last schedule for pipeline[%s].", pipelineID)
	schedule := Schedule{}
	tx := database.DB.Model(&Schedule{}).Where("pipelineID > ?", pipelineID).Last(&schedule)
	if tx.Error != nil {
		logEntry.Errorf("get last schedule for pipeline[%s] failed. error:%s", pipelineID, tx.Error.Error())
		return Schedule{}, tx.Error
	}
	return schedule, nil
}

func checkScheduleStatus(status string) bool {
	for _, validStatus := range ScheduleStatusList {
		if status == validStatus {
			return true
		}
	}

	return false
}

func IsScheduleFinalStatus(status string) bool {
	for _, finalStatus := range ScheduleFinalStatusList {
		if status == finalStatus {
			return true
		}
	}

	return false
}

func GetSchedule(logEntry *log.Entry, scheduleID string) (Schedule, error) {
	logEntry.Debugf("begin to get schedule of ID[%s]", scheduleID)

	var schedule Schedule
	result := database.DB.Model(&Schedule{}).Where("id = ?", scheduleID).First(&schedule)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			errMsg := fmt.Sprintf("schedule[%s] not found!", scheduleID)
			logEntry.Errorf(errMsg)
			return Schedule{}, fmt.Errorf(errMsg)
		} else {
			return Schedule{}, result.Error
		}
	}

	return schedule, nil
}

func GetSchedules(logEntry *log.Entry, status string) (schedules []Schedule, err error) {
	if !checkScheduleStatus(status) {
		errMsg := fmt.Sprintf("get schedules failed: status[%s] invalid!", status)
		logEntry.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	logEntry.Debugf("begin to get schedules of status[%s]", status)
	result := database.DB.Model(&Schedule{}).Where("status = ?", status).Find(&schedules)
	if result.Error != nil {
		errMsg := fmt.Sprintf("get schedules failed: error:%s", result.Error.Error())
		logEntry.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return schedules, nil
}

func UpdateScheduleStatus(logEntry *log.Entry, scheduleID, status string) error {
	logEntry.Debugf("begin update schedule status. scheduleID:%s, status:%s", scheduleID, status)
	tx := database.DB.Model(&Schedule{}).Where("id = ?", scheduleID).Update("status", status)
	if tx.Error != nil {
		logEntry.Errorf("update schedule status failed. scheduleID:%s, error:%s",
			scheduleID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteSchedule(logEntry *log.Entry, scheduleID string) error {
	logEntry.Debugf("begin delete schedule. scheduleID:%s", scheduleID)
	result := database.DB.Model(&Schedule{}).Where("id = ?", scheduleID).Delete(&Schedule{})
	if result.Error != nil {
		logEntry.Errorf("delete schedule failed. scheduleID:%s, error:%s",
			scheduleID, result.Error.Error())
		return result.Error
	}
	return nil
}

// ------ 周期调度逻辑需要的函数 ------

func checkNextRunAt(nextRunAt, currentTime time.Time, endAt sql.NullTime) bool {
	if nextRunAt.After(currentTime) {
		return false
	}
	if endAt.Valid && nextRunAt.After(endAt.Time) {
		return false
	}

	return true
}

func getEarlierTime(time1, time2 time.Time) time.Time {
	if time1.Before(time2) {
		return time1
	} else {
		return time2
	}
}

// 查询数据库
// - 获取需要发起的周期调度，更新对应next_run_at
// - 更新到达end_time的周期调度状态
// - 获取下一个wakeup时间(如果不存在则是空指针)
func GetAvailableSchedule(logEntry *log.Entry, checkCatchup bool) (killMap map[string][]string, execMap map[string][]time.Time, nextWakeupTime *time.Time, err error) {
	// todo: 查询时，要添加for update锁，避免被同时update
	execMap = map[string][]time.Time{}
	killMap = map[string][]string{}
	nextWakeupTime = nil

	currentTime := time.Now()
	logEntry.Debugf("begin to search available schedule before [%s]", currentTime.Format("01-02-2006 15:04:05"))

	schedules, err := GetSchedules(logEntry, ScheduleStatusRunning)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, schedule := range schedules {
		nextRunAt := schedule.NextRunAt
		cronSchedule, err := cron.ParseStandard(schedule.Crontab)
		if err != nil {
			errMsg := fmt.Sprintf("parse crontab spec[%s] for schedule[%s] of pipeline detail[%d] failed, errMsg[%s]",
				schedule.Crontab, schedule.ID, schedule.PipelineDetailPk, err.Error())
			return nil, nil, nil, fmt.Errorf(errMsg)
		}

		options := ScheduleOptions{}
		if err := json.Unmarshal([]byte(schedule.Options), &options); err != nil {
			errMsg := fmt.Sprintf("decode optinos[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, schedule.ID, err)
			logEntry.Errorf(errMsg)
			return nil, nil, nil, fmt.Errorf(errMsg)
		}

		catchup := true
		if checkCatchup && options.Catchup == false {
			catchup = false
		}

		notEndedList := []string{common.StatusRunInitiating, common.StatusRunPending, common.StatusRunRunning, common.StatusRunTerminating}
		scheduleIDFilter := []string{schedule.ID}
		count, err := CountRun(logEntry, 0, 0, nil, nil, nil, nil, notEndedList, scheduleIDFilter)
		if err != nil {
			errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", schedule.ID, err.Error())
			log.Errorf(errMsg)
			return nil, nil, nil, fmt.Errorf(errMsg)
		}

		totalCount := int(count)
		for checkNextRunAt(nextRunAt, currentTime, schedule.EndAt) {
			// 如果不需要catchup，则所有miss的周期任务都被抛弃不发起
			// 所以下面只需要处理 catchup == true 的case
			if catchup == true {
				if totalCount < options.Concurrency {
					// 当没有达到concurrency，所有ConcurrencyPolicy处理方式一致
					// 有效的next_run_at需要满足三个条件
					// 1. 不大于当前时间
					// 2. 不大于endtime（如果有的话）
					// 3. 在expire_interval内（如果有的话）
					Expire_interval_durtion := time.Duration(options.ExpireInterval) * time.Second
					if !nextRunAt.Add(Expire_interval_durtion).Before(currentTime) {
						execMap[schedule.ID] = append(execMap[schedule.ID], nextRunAt)
						totalCount += 1
					}
				} else {
					switch options.ConcurrencyPolicy {
					case ConcurrencyPolicySuspend:
						// 直接跳出循环，不会继续更新nextRunAt
						errMsg := fmt.Sprintf("concurrency of schedule with ID[%s] already reach[%d], so suspend", schedule.ID, options.Concurrency)
						logEntry.Debug(errMsg)
						break

					case ConcurrencyPolicyReplace:
						// 停止该schedule最早的run，并发起新的run，更新next_run_at
						execMap[schedule.ID] = append(execMap[schedule.ID], nextRunAt)
						totalCount += 1

					case ConcurrencyPolicySkip:
						// 不跳出循环，会继续更新nextRunAt
						errMsg := fmt.Sprintf("concurrency of schedule with ID[%s] already reach[%d], so skip", schedule.ID, options.Concurrency)
						logEntry.Debug(errMsg)
					}
				}
			}

			nextRunAt = cronSchedule.Next(schedule.NextRunAt)
		}

		// 有可能【待发起任务 + 运行中任务】>= concurrency，此时判断是否需要截取一部分
		if totalCount > options.Concurrency {
			var stopCount int
			if len(execMap[schedule.ID]) >= options.Concurrency {
				execMap[schedule.ID] = execMap[schedule.ID][len(execMap[schedule.ID])-options.Concurrency:]
				stopCount = int(count)
			} else {
				stopCount = totalCount - options.Concurrency
			}

			// 获取待停止的runid
			scheduleIdList := []string{schedule.ID}
			stopRunList, err := ListRun(logEntry, 0, stopCount, []string{}, []string{}, []string{}, []string{}, notEndedList, scheduleIdList)
			if err != nil {
				errMsg := fmt.Sprintf("get runs to stop for schedule[%s] failed, err: %s", schedule.ID, err.Error())
				return nil, nil, nil, fmt.Errorf(errMsg)
			}
			for _, run := range stopRunList {
				killMap[schedule.ID] = append(killMap[schedule.ID], run.ID)
			}
		}

		// 更新 NextRunAt 字段
		to_update := false
		if !nextRunAt.Equal(schedule.NextRunAt) {
			schedule.NextRunAt = nextRunAt
			to_update = true
		}

		// 更新 status 字段
		if schedule.EndAt.Valid && nextRunAt.After(schedule.EndAt.Time) {
			schedule.Status = ScheduleStatusSuccess
			to_update = true
		}

		if to_update {
			result := database.DB.Model(&schedule).Save(schedule)
			if result.Error != nil {
				errMsg := fmt.Sprintf("update schedule[%s] of pipeline detail[%d] failed, error:%v",
					schedule.ID, schedule.PipelineDetailPk, result.Error)
				logEntry.Errorf(errMsg)
				return nil, nil, nil, fmt.Errorf(errMsg)
			}
		}

		// 更新全局wakeup时间
		if schedule.Status == ScheduleStatusRunning {
			var earlierTime time.Time
			if schedule.EndAt.Valid {
				earlierTime = getEarlierTime(schedule.NextRunAt, schedule.EndAt.Time)
			} else {
				earlierTime = schedule.NextRunAt
			}

			if nextWakeupTime == nil {
				nextWakeupTime = &earlierTime
			} else {
				earlierTime = getEarlierTime(earlierTime, *nextWakeupTime)
				nextWakeupTime = &earlierTime
			}
		}
	}

	return killMap, execMap, nextWakeupTime, err
}

// 该函数只有只读操作，所以不加锁
func GetNextGlobalWakeupTime(logEntry *log.Entry) (*time.Time, error) {
	currentTime := time.Now()
	logEntry.Debugf("begin to get next wakeup time after[%s]", currentTime.Format("01-02-2006 15:04:05"))

	var schedules []Schedule
	result := database.DB.Model(&Schedule{}).Where("status = ?", ScheduleStatusRunning).Find(&schedules)
	if result.Error != nil {
		errMsg := fmt.Sprintf("search running schedules failed. error:%s", result.Error.Error())
		logEntry.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	var nextWakeupTime *time.Time = nil
	for _, schedule := range schedules {
		// 如果当前【run并发度】>= concurrency，而且policy是suspend时，则跳过该schedule
		options := ScheduleOptions{}
		if err := json.Unmarshal([]byte(schedule.Options), &options); err != nil {
			errMsg := fmt.Sprintf("decode optinos[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, schedule.ID, err)
			logEntry.Errorf(errMsg)
			return nil, fmt.Errorf(errMsg)
		}

		notEndedList := []string{common.StatusRunInitiating, common.StatusRunPending, common.StatusRunRunning, common.StatusRunTerminating}
		scheduleIDFilter := []string{schedule.ID}
		count, err := CountRun(logEntry, 0, 0, nil, nil, nil, nil, notEndedList, scheduleIDFilter)
		if err != nil {
			errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", schedule.ID, err.Error())
			log.Errorf(errMsg)
			return nil, fmt.Errorf(errMsg)
		}

		if int(count) >= options.Concurrency && options.ConcurrencyPolicy == ConcurrencyPolicySuspend {
			continue
		}

		// 计算nextWakeupTime，通过比较nextRunAt 和 EndAt
		// 无论nextRunAt是否在expire_interval内，都会直接拿来比较
		var earlierTime time.Time
		if !schedule.EndAt.Valid {
			earlierTime = schedule.NextRunAt
		} else {
			earlierTime = getEarlierTime(schedule.EndAt.Time, schedule.NextRunAt)
		}

		if nextWakeupTime == nil {
			nextWakeupTime = &earlierTime
		} else {
			earlierTime = getEarlierTime(earlierTime, *nextWakeupTime)
			nextWakeupTime = &earlierTime
		}
	}

	return nextWakeupTime, nil
}

// 为指定周期调度，计算下一次wakeup时间
//   - 如果end_at不存在，则根据next_run_at
//   - 如果end_at存在，且next_run_at不早于end_at，则根据end_at计算
//   - 如果end_at存在，且next_run_at早于end_at，则根据next_run_at计算
func GetNextWakeupTime(logEntry *log.Entry, scheduleID string) (*time.Time, error) {
	logEntry.Debugf("begin to get next wakeup time of ID[%s]", scheduleID)

	var schedule Schedule
	result := database.DB.Model(&Schedule{}).Where("id = ?", scheduleID).First(&schedule)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			errMsg := fmt.Sprintf("get schedule failed: schedule of ID[%s] not found!", scheduleID)
			logEntry.Errorf(errMsg)
			return nil, fmt.Errorf(errMsg)
		} else {
			return nil, result.Error
		}
	}

	if schedule.Status != ScheduleStatusRunning {
		return nil, nil
	}

	// 如果当前【run并发度】>= concurrency，而且policy是suspend时，则跳过该schedule
	options := ScheduleOptions{}
	if err := json.Unmarshal([]byte(schedule.Options), &options); err != nil {
		errMsg := fmt.Sprintf("decode optinos[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, schedule.ID, err)
		logEntry.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	notEndedList := []string{common.StatusRunInitiating, common.StatusRunPending, common.StatusRunRunning, common.StatusRunTerminating}
	scheduleIDFilter := []string{schedule.ID}
	count, err := CountRun(logEntry, 0, 0, nil, nil, nil, nil, notEndedList, scheduleIDFilter)
	if err != nil {
		errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", schedule.ID, err.Error())
		log.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	if int(count) >= options.Concurrency && options.ConcurrencyPolicy == ConcurrencyPolicySuspend {
		return nil, nil
	}

	if !schedule.EndAt.Valid || schedule.NextRunAt.Before(schedule.EndAt.Time) {
		return &schedule.NextRunAt, nil
	} else {
		return &schedule.EndAt.Time, nil
	}
}
