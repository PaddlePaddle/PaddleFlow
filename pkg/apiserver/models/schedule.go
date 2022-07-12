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
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

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

type Schedule struct {
	Pk               int64          `gorm:"primaryKey;autoIncrement;not null" json:"-"`
	ID               string         `gorm:"type:varchar(60);not null"         json:"scheduleID"`
	Name             string         `gorm:"type:varchar(60);not null"         json:"name"`
	Desc             string         `gorm:"type:varchar(256);not null"       json:"desc"`
	PipelineID       string         `gorm:"type:varchar(60);not null"         json:"pipelineID"`
	PipelineDetailID string         `gorm:"type:varchar(60);not null"         json:"pipelineDetailID"`
	UserName         string         `gorm:"type:varchar(60);not null"         json:"username"`
	FsConfig         string         `gorm:"type:varchar(1024);not null"       json:"fsConfig"`
	Crontab          string         `gorm:"type:varchar(60);not null"         json:"crontab"`
	Options          string         `gorm:"type:text;size:65535;not null"     json:"options"`
	Message          string         `gorm:"type:text;size:65535;not null"     json:"scheduleMsg"`
	Status           string         `gorm:"type:varchar(32);not null"         json:"status"`
	StartAt          sql.NullTime   `                                         json:"-"`
	EndAt            sql.NullTime   `                                         json:"-"`
	NextRunAt        time.Time      `                                         json:"-"`
	CreatedAt        time.Time      `                                         json:"-"`
	UpdatedAt        time.Time      `                                         json:"-"`
	DeletedAt        gorm.DeletedAt `                                         json:"-"`
}

type FsConfig struct {
	GlobalFsName string `json:"globalFsName"`
	UserName     string `json:"userName"`
}

func DecodeFsConfig(strConfig string) (fc FsConfig, err error) {
	if err := json.Unmarshal([]byte(strConfig), &fc); err != nil {
		errMsg := fmt.Sprintf("decode fsConfig failed. error:%v", err)
		return FsConfig{}, fmt.Errorf(errMsg)
	}

	return fc, nil
}

func (fc *FsConfig) Encode(logEntry *log.Entry) (string, error) {
	strConfig, err := json.Marshal(fc)
	if err != nil {
		logEntry.Errorf("encode fsConfig failed. error:%v", err)
		return "", err
	}

	return string(strConfig), nil
}

type ScheduleOptions struct {
	Catchup           bool   `json:"catchup"`
	ExpireInterval    int    `json:"expireInterval"`
	Concurrency       int    `json:"concurrency"`
	ConcurrencyPolicy string `json:"concurrencyPolicy"`
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
		errMsg := fmt.Sprintf("decode scheduleOptions[%s] failed. error:%v", StrOptions, err)
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

func (Schedule) TableName() string {
	return "schedule"
}

func CreateSchedule(logEntry *log.Entry, schedule Schedule) (scheduleID string, err error) {
	logEntry.Debugf("begin create schedule:%+v", schedule)
	err = WithTransaction(storage.DB, func(tx *gorm.DB) error {
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

func ListSchedule(logEntry *log.Entry, pk int64, maxKeys int, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter []string) ([]Schedule, error) {
	logEntry.Debugf("begin list schedule.")
	tx := storage.DB.Model(&Schedule{}).Where("pk > ?", pk)

	if len(pplFilter) > 0 {
		tx = tx.Where("pipeline_id IN (?)", pplFilter)
	}
	if len(pplDetailFilter) > 0 {
		tx = tx.Where("pipeline_detail_id IN (?)", pplDetailFilter)
	}
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
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
		logEntry.Errorf("list schedule failed. Filters: pplDetail[%v], user{%v}, schedule{%v}, name{%v}, status{%v}. error:%s",
			pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter, tx.Error.Error())
		return []Schedule{}, tx.Error
	}

	return scheduleList, nil
}

func IsLastSchedulePk(logEntry *log.Entry, pk int64, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter []string) (bool, error) {
	logEntry.Debugf("get last schedule, Filters: ppl[%v], pplDetail[%v], user[%v], schedule[%v], name[%v], status[%v]",
		pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	tx := storage.DB.Model(&Schedule{})

	if len(pplFilter) > 0 {
		tx = tx.Where("pipeline_id IN (?)", pplFilter)
	}

	if len(pplDetailFilter) > 0 {
		tx = tx.Where("pipeline_detail_id IN (?)", pplDetailFilter)
	}
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
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

	schedule := Schedule{}
	tx = tx.Last(&schedule)
	if tx.Error != nil {
		logEntry.Errorf("get last schedule failed. error:%s", tx.Error.Error())
		return false, tx.Error
	}

	return schedule.Pk == pk, nil
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
	result := storage.DB.Model(&Schedule{}).Where("id = ?", scheduleID).First(&schedule)
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

func GetScheduleByName(logEntry *log.Entry, name, userName string) (Schedule, error) {
	logEntry.Debugf("begin to get schedule of name[%s], userName[%s]", name, userName)

	var schedule Schedule
	result := storage.DB.Model(&Schedule{}).Where("name = ?", name).Where("user_name = ?", userName).First(&schedule)
	if result.Error != nil {
		return Schedule{}, result.Error
	}

	return schedule, nil
}

func GetSchedulesByStatus(logEntry *log.Entry, status string) (schedules []Schedule, err error) {
	if !checkScheduleStatus(status) {
		errMsg := fmt.Sprintf("get schedules failed: status[%s] invalid!", status)
		logEntry.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	logEntry.Debugf("begin to get schedules of status[%s]", status)
	result := storage.DB.Model(&Schedule{}).Where("status = ?", status).Find(&schedules)
	if result.Error != nil {
		errMsg := fmt.Sprintf("get schedules failed: error:%s", result.Error.Error())
		logEntry.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	return schedules, nil
}

func UpdateScheduleStatus(logEntry *log.Entry, scheduleID, status string) error {
	logEntry.Debugf("begin update schedule status. scheduleID:%s, status:%s", scheduleID, status)
	tx := storage.DB.Model(&Schedule{}).Where("id = ?", scheduleID).Update("status", status)
	if tx.Error != nil {
		logEntry.Errorf("update schedule status failed. scheduleID:%s, error:%s",
			scheduleID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteSchedule(logEntry *log.Entry, scheduleID string) error {
	logEntry.Debugf("begin delete schedule. scheduleID:%s", scheduleID)
	result := storage.DB.Model(&Schedule{}).Where("id = ?", scheduleID).Delete(&Schedule{})
	if result.Error != nil {
		logEntry.Errorf("delete schedule failed. scheduleID:%s, error:%s",
			scheduleID, result.Error.Error())
		return result.Error
	}
	return nil
}

// ------ job / fs模块需要的函数 ------

func ScheduleUsedFsIDs() (map[string]bool, error) {
	tx := storage.DB.Model(&Schedule{}).Select("id", "user_name", "fs_config").Where("status = ?", ScheduleStatusRunning)
	var scheduleList []Schedule
	tx = tx.Find(&scheduleList)
	if tx.Error != nil {
		return nil, tx.Error
	}

	fsIDMap := make(map[string]bool, 0)
	for _, schedule := range scheduleList {
		fsConfig, err := DecodeFsConfig(schedule.FsConfig)
		if err != nil {
			return nil, err
		}

		var fsID string
		if fsConfig.UserName != "" {
			fsID = common.ID(fsConfig.UserName, fsConfig.GlobalFsName)
		} else {
			fsID = common.ID(schedule.UserName, fsConfig.GlobalFsName)
		}

		fsIDMap[fsID] = true
	}
	return fsIDMap, nil
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

// 先处理同时满足currentTime之前，而且schedule.EndAt之前的任务
// 只需要处理 catchup == true 的case
// - 如果catchup == false，即不需要catchup，则currentTime和schedule.EndAt前，所有miss的周期任务都被抛弃，不再发起
func findExectableRunBeforeCurrentTime(logEntry *log.Entry, schedule Schedule, currentTime time.Time, checkCatchup bool, totalCount int, execMap map[string][]time.Time) (time.Time, error) {
	options, err := DecodeScheduleOptions(schedule.Options)
	if err != nil {
		errMsg := fmt.Sprintf("decode options of schedule[%s] failed. error: %v", schedule.ID, err)
		return time.Time{}, fmt.Errorf(errMsg)
	}

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	if err != nil {
		errMsg := fmt.Sprintf("parse crontab spec[%s] for schedule[%s] of pipeline detail[%s] failed, errMsg[%s]",
			schedule.Crontab, schedule.ID, schedule.PipelineDetailID, err.Error())
		return time.Time{}, fmt.Errorf(errMsg)
	}

	catchup := true
	if checkCatchup && options.Catchup == false {
		catchup = false
	}

	logEntry.Infof("findExectableRunBeforeCurrentTime with catchup[%t], init totalCount[%d], schedule[%v]", catchup, totalCount, schedule)

	nextRunAt := schedule.NextRunAt
	expire_interval_durtion := time.Duration(options.ExpireInterval) * time.Second
	for ; checkNextRunAt(nextRunAt, currentTime, schedule.EndAt); nextRunAt = cronSchedule.Next(nextRunAt) {
		logEntry.Infof("start to check schedule[%s] at %s, with schedule.EndAt[%s]", schedule.ID, nextRunAt.Format("2006-01-02 15:04:05"), schedule.EndAt.Time.Format("2006-01-02 15:04:05"))

		if catchup == false {
			continue
		}

		if options.ExpireInterval != 0 && nextRunAt.Add(expire_interval_durtion).Before(currentTime) {
			logEntry.Infof("skip nextRunAt[%s] of schedule[%s], beyond expire interval[%d] from currentTime[%s]",
				nextRunAt.Format("2006-01-02 15:04:05"), schedule.ID, options.ExpireInterval, currentTime.Format("2006-01-02 15:04:05"))
			continue
		}

		if options.Concurrency == 0 || totalCount < options.Concurrency {
			execMap[schedule.ID] = append(execMap[schedule.ID], nextRunAt)
			totalCount += 1
		} else {
			if options.ConcurrencyPolicy == ConcurrencyPolicySuspend {
				// 直接跳出循环，不会继续更新nextRunAt
				errMsg := fmt.Sprintf("concurrency of schedule with ID[%s] already reach[%d], so suspend", schedule.ID, options.Concurrency)
				logEntry.Info(errMsg)
				break
			} else if options.ConcurrencyPolicy == ConcurrencyPolicyReplace {
				// 停止该schedule最早的run，并发起新的run，更新next_run_at
				execMap[schedule.ID] = append(execMap[schedule.ID], nextRunAt)
				totalCount += 1
			} else if options.ConcurrencyPolicy == ConcurrencyPolicySkip {
				// 不跳出循环，会继续更新nextRunAt
				errMsg := fmt.Sprintf("concurrency of schedule with ID[%s] already reach[%d], so skip", schedule.ID, options.Concurrency)
				logEntry.Info(errMsg)
			}
		}
	}

	return nextRunAt, nil
}

func updateKillMap(logEntry *log.Entry, scheduleID string, notEndedCount, concurrency int, execMap map[string][]time.Time, killMap map[string][]string) error {
	if notEndedCount+len(execMap[scheduleID]) > concurrency {
		var stopCount int
		if len(execMap[scheduleID]) >= concurrency {
			execMap[scheduleID] = execMap[scheduleID][len(execMap[scheduleID])-concurrency:]
			stopCount = int(notEndedCount)
		} else {
			stopCount = notEndedCount + len(execMap[scheduleID]) - concurrency
		}

		// 获取待停止的runid
		notEndedList := []string{common.StatusRunInitiating, common.StatusRunPending, common.StatusRunRunning, common.StatusRunTerminating}
		scheduleIDList := []string{scheduleID}
		stopRunList, err := ListRun(logEntry, 0, stopCount, []string{}, []string{}, []string{}, []string{}, notEndedList, scheduleIDList)
		if err != nil {
			errMsg := fmt.Sprintf("get runs to stop for schedule[%s] failed, err: %s", scheduleID, err.Error())
			logEntry.Error(errMsg)
			return err
		}
		for _, run := range stopRunList {
			killMap[scheduleID] = append(killMap[scheduleID], run.ID)
		}
	}

	return nil
}

// 查询数据库
// - 获取需要发起的周期调度，更新对应next_run_at
// - 更新到达end_time的周期调度状态
// - 获取下一个wakeup时间(如果不存在则是空指针)
func GetAvailableSchedule(logEntry *log.Entry, checkCatchup bool) (killMap map[string][]string, execMap map[string][]time.Time, nextWakeupTime *time.Time, err error) {
	// todo: 查询时，要添加for update锁，避免多个paddleFlow实例同时调度时，记录被同时update
	execMap = map[string][]time.Time{}
	killMap = map[string][]string{}
	nextWakeupTime = nil

	currentTime := time.Now()
	logEntry.Infof("begin to search available schedule before [%s]", currentTime.Format("01-02-2006 15:04:05"))

	schedules, err := GetSchedulesByStatus(logEntry, ScheduleStatusRunning)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, schedule := range schedules {
		options, err := DecodeScheduleOptions(schedule.Options)
		if err != nil {
			errMsg := fmt.Sprintf("decode options[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, schedule.ID, err)
			logEntry.Errorf(errMsg)
			return nil, nil, nil, fmt.Errorf(errMsg)
		}

		count, err := CountActiveRunsForSchedule(logEntry, schedule.ID)
		if err != nil {
			errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", schedule.ID, err.Error())
			logEntry.Errorf(errMsg)
			return nil, nil, nil, fmt.Errorf(errMsg)
		}

		// 先处理同时满足currentTime之前，而且schedule.EndAt之前的任务
		notEndedCount := int(count)
		nextRunAt, err := findExectableRunBeforeCurrentTime(logEntry, schedule, currentTime, checkCatchup, notEndedCount, execMap)
		if err != nil {
			return nil, nil, nil, err
		}

		logEntry.Infof("after findExectableRunBeforeCurrentTime, execMap[%v], notEndedCount:[%d]", execMap, notEndedCount)

		// Concurrency != 0，即存在并发度限制，而且ConcurrencyPolicy == replace时，有可能【待发起任务 + 运行中任务】>= concurrency
		// 此时判断是否需要截取一部分待运行任务，以及停止一些已经启动的任务
		if options.Concurrency != 0 && options.ConcurrencyPolicy == ConcurrencyPolicyReplace {
			err = updateKillMap(logEntry, schedule.ID, notEndedCount, options.Concurrency, execMap, killMap)
			if err != nil {
				errMsg := fmt.Sprintf("getKillMap for schedule[%s] failed, err: %s", schedule.ID, err.Error())
				return nil, nil, nil, fmt.Errorf(errMsg)
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
			result := storage.DB.Model(&schedule).Save(schedule)
			if result.Error != nil {
				errMsg := fmt.Sprintf("update schedule[%s] of pipeline detail[%s] failed, error:%v",
					schedule.ID, schedule.PipelineDetailID, result.Error)
				logEntry.Errorf(errMsg)
				return nil, nil, nil, fmt.Errorf(errMsg)
			}
		}

		// 更新全局wakeup时间
		// 1. 如果NextRunAt <= currentTime，证明目前schedule超过concurrency，而且concurrency是suspend，导致调度被阻塞，此时nextRunAt不能被纳入nextWakeupTime
		// 2. 如果status是终止态，nextRunAt也不能被纳入nextWakeupTime
		if schedule.Status == ScheduleStatusRunning && schedule.NextRunAt.After(currentTime) {
			var earlierTime time.Time
			if schedule.EndAt.Valid {
				earlierTime = getEarlierTime(schedule.NextRunAt, schedule.EndAt.Time)
			} else {
				earlierTime = schedule.NextRunAt
			}

			if nextWakeupTime != nil {
				earlierTime = getEarlierTime(earlierTime, *nextWakeupTime)
			}
			nextWakeupTime = &earlierTime
		}
	}

	return killMap, execMap, nextWakeupTime, err
}

// 计算timeout先不加事务，虽然select和 CountActiveRunsForSchedule 是非原子性，因为只影响休眠时间的计算结果
func GetNextGlobalWakeupTime(logEntry *log.Entry) (*time.Time, error) {
	currentTime := time.Now()
	logEntry.Debugf("begin to get next wakeup time after[%s]", currentTime.Format("01-02-2006 15:04:05"))

	var schedules []Schedule
	result := storage.DB.Model(&Schedule{}).Where("status = ?", ScheduleStatusRunning).Find(&schedules)
	if result.Error != nil {
		errMsg := fmt.Sprintf("search running schedules failed. error:%s", result.Error.Error())
		logEntry.Errorf(errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	var nextWakeupTime *time.Time = nil
	for _, schedule := range schedules {
		// 如果当前【run并发度】>= concurrency，而且policy是suspend时，则跳过该schedule
		options, err := DecodeScheduleOptions(schedule.Options)
		if err != nil {
			errMsg := fmt.Sprintf("decode options[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, schedule.ID, err)
			logEntry.Errorf(errMsg)
			return nil, fmt.Errorf(errMsg)
		}

		count, err := CountActiveRunsForSchedule(logEntry, schedule.ID)
		if err != nil {
			errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", schedule.ID, err.Error())
			logEntry.Errorf(errMsg)
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

func CountActiveRunsForSchedule(logEntry *log.Entry, scheduleID string) (int64, error) {
	// todo：加索引，或者schedule记录添加count字段，通过run更新状态时同时更新schedule count字段，减少扫表
	notEndedList := []string{common.StatusRunInitiating, common.StatusRunPending, common.StatusRunRunning, common.StatusRunTerminating}
	scheduleIDFilter := []string{scheduleID}
	count, err := CountRun(logEntry, 0, 0, nil, nil, nil, nil, notEndedList, scheduleIDFilter)
	return count, err
}
