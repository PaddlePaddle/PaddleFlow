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

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
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
	Pk                int64          `gorm:"primaryKey;autoIncrement;not null" json:"-"`
	ID                string         `gorm:"type:varchar(60);not null"         json:"scheduleID"`
	Name              string         `gorm:"type:varchar(60);not null"         json:"name"`
	Desc              string         `gorm:"type:varchar(256);not null"       json:"desc"`
	PipelineID        string         `gorm:"type:varchar(60);not null"         json:"pipelineID"`
	PipelineVersionID string         `gorm:"type:varchar(60);not null"         json:"pipelineVersionID"`
	UserName          string         `gorm:"type:varchar(60);not null"         json:"username"`
	FsConfig          string         `gorm:"type:varchar(1024);not null"       json:"fsConfig"`
	Crontab           string         `gorm:"type:varchar(60);not null"         json:"crontab"`
	Options           string         `gorm:"type:text;size:65535;not null"     json:"options"`
	Message           string         `gorm:"type:text;size:65535;not null"     json:"scheduleMsg"`
	Status            string         `gorm:"type:varchar(32);not null"         json:"status"`
	StartAt           sql.NullTime   `                                         json:"-"`
	EndAt             sql.NullTime   `                                         json:"-"`
	NextRunAt         time.Time      `                                         json:"-"`
	CreatedAt         time.Time      `                                         json:"-"`
	UpdatedAt         time.Time      `                                         json:"-"`
	DeletedAt         gorm.DeletedAt `                                         json:"-"`
}

func (Schedule) TableName() string {
	return "schedule"
}

// ------- 存放周期调度相关的配置 -------

type FsConfig struct {
	Username string `json:"username"`
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

func CreateSchedule(logEntry *log.Entry, schedule Schedule) (scheduleID string, err error) {
	logEntry.Debugf("begin create schedule:%+v", schedule)
	err = storage.DB.Transaction(func(tx *gorm.DB) error {
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

func ListSchedule(logEntry *log.Entry, pk int64, maxKeys int, pplFilter, pplVersionFilter, userFilter, scheduleFilter, nameFilter, statusFilter []string) ([]Schedule, error) {
	logEntry.Debugf("begin list schedule.")
	tx := storage.DB.Model(&Schedule{}).Where("pk > ?", pk)

	if len(pplFilter) > 0 {
		tx = tx.Where("pipeline_id IN (?)", pplFilter)
	}
	if len(pplVersionFilter) > 0 {
		tx = tx.Where("pipeline_version_id IN (?)", pplVersionFilter)
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
		logEntry.Errorf("list schedule failed. Filters: pplVersion[%v], user{%v}, schedule{%v}, name{%v}, status{%v}. error:%s",
			pplVersionFilter, userFilter, scheduleFilter, nameFilter, statusFilter, tx.Error.Error())
		return []Schedule{}, tx.Error
	}

	return scheduleList, nil
}

func IsLastSchedulePk(logEntry *log.Entry, pk int64, pplFilter, pplVersionFilter, userFilter, scheduleFilter, nameFilter, statusFilter []string) (bool, error) {
	logEntry.Debugf("get last schedule, Filters: ppl[%v], pplVersion[%v], user[%v], schedule[%v], name[%v], status[%v]",
		pplFilter, pplVersionFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	tx := storage.DB.Model(&Schedule{})

	if len(pplFilter) > 0 {
		tx = tx.Where("pipeline_id IN (?)", pplFilter)
	}

	if len(pplVersionFilter) > 0 {
		tx = tx.Where("pipeline_version_id IN (?)", pplVersionFilter)
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
	type result struct {
		UserName     string
		FsConfig     string
		PipelineYaml string
	}
	results := []result{}
	tx := storage.DB.Model(&Schedule{}).Select("schedule.user_name, schedule.fs_config, pipeline_version.pipeline_yaml").
		Joins("join pipeline_version on schedule.pipeline_version_id = pipeline_version.id and schedule.pipeline_id = pipeline_version.pipeline_id").
		Where("schedule.status = ?", ScheduleStatusRunning).Find(&results)

	if tx.Error != nil {
		return nil, tx.Error
	}

	fsIDMap := make(map[string]bool, 0)

	for _, res := range results {
		wfs, err := schema.GetWorkflowSource([]byte(res.PipelineYaml))
		if err != nil {
			return nil, err
		}
		mounts, err := wfs.GetFsMounts()
		if err != nil {
			return nil, err
		}
		fsConfig, err := DecodeFsConfig(res.FsConfig)
		if err != nil {
			return nil, err
		}

		username := res.UserName
		if fsConfig.Username != "" {
			username = fsConfig.Username
		}

		for _, mount := range mounts {
			mount.ID = common.ID(username, mount.Name)
			fsIDMap[mount.ID] = true
		}
		if wfs.FsOptions.MainFS.Name != "" {
			mainFSID := common.ID(username, wfs.FsOptions.MainFS.Name)
			fsIDMap[mainFSID] = true
		}
	}

	return fsIDMap, nil
}

// ------ 周期调度逻辑需要的函数 ------

func getEarlierTime(time1, time2 time.Time) time.Time {
	if time1.Before(time2) {
		return time1
	} else {
		return time2
	}
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

		if options.Concurrency > 0 && options.ConcurrencyPolicy == ConcurrencyPolicySuspend {
			count, err := CountActiveRunsForSchedule(logEntry, schedule.ID)
			if err != nil {
				errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", schedule.ID, err.Error())
				logEntry.Errorf(errMsg)
				return nil, fmt.Errorf(errMsg)
			}

			if int(count) >= options.Concurrency {
				continue
			}
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
	scheduleIDFilter := []string{scheduleID}
	count, err := CountRun(logEntry, 0, 0, nil, nil, nil, nil, common.RunActiveStatus, scheduleIDFilter)
	return count, err
}
