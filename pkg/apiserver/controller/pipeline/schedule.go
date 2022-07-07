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
	"errors"
	"fmt"
	"time"

	cron "github.com/robfig/cron/v3"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type CreateScheduleRequest struct {
	Name              string `json:"name"`
	Desc              string `json:"desc"` // optional
	PipelineID        string `json:"pipelineID"`
	PipelineDetailID  string `json:"pipelineDetailID"`
	Crontab           string `json:"crontab"`
	StartTime         string `json:"startTime"`         // optional
	EndTime           string `json:"endTime"`           // optional
	Concurrency       int    `json:"concurrency"`       // optional, 默认 0, 表示不限制
	ConcurrencyPolicy string `json:"concurrencyPolicy"` // optional, 默认 suspend
	ExpireInterval    int    `json:"expireInterval"`    // optional, 默认 0, 表示不限制
	Catchup           bool   `json:"catchup"`           // optional, 默认 false
	GlobalFsName      string `json:"globalFsName"`      // optional
	UserName          string `json:"username"`          // optional, 只有root用户使用其他用户fsname时，需要指定对应username
}

type CreateScheduleResponse struct {
	ScheduleID string `json:"scheduleID"`
}

type ScheduleBrief struct {
	ID               string                 `json:"scheduleID"`
	Name             string                 `json:"name"`
	Desc             string                 `json:"desc"`
	PipelineID       string                 `json:"pipelineID"`
	PipelineDetailID string                 `json:"pipelineDetailID"`
	UserName         string                 `json:"username"`
	FsConfig         models.FsConfig        `json:"fsConfig"`
	Crontab          string                 `json:"crontab"`
	Options          models.ScheduleOptions `json:"options"`
	StartTime        string                 `json:"startTime"`
	EndTime          string                 `json:"endTime"`
	CreateTime       string                 `json:"createTime"`
	UpdateTime       string                 `json:"updateTime"`
	NextRunTime      string                 `json:"nextRunTime"`
	Message          string                 `json:"scheduleMsg"`
	Status           string                 `json:"status"`
}

type ListScheduleResponse struct {
	common.MarkerInfo
	ScheduleList []ScheduleBrief `json:"scheduleList"`
}

type GetScheduleResponse struct {
	ScheduleBrief
	ListRunResponse ListRunResponse `json:"runs"`
}

func (b *ScheduleBrief) updateFromScheduleModel(schedule models.Schedule) (err error) {
	b.ID = schedule.ID
	b.Name = schedule.Name
	b.Desc = schedule.Desc
	b.PipelineID = schedule.PipelineID
	b.PipelineDetailID = schedule.PipelineDetailID
	b.UserName = schedule.UserName
	b.Crontab = schedule.Crontab
	b.CreateTime = schedule.CreatedAt.Format("2006-01-02 15:04:05")
	b.UpdateTime = schedule.UpdatedAt.Format("2006-01-02 15:04:05")
	b.NextRunTime = schedule.NextRunAt.Format("2006-01-02 15:04:05")
	b.Message = schedule.Message
	b.Status = schedule.Status

	b.FsConfig, err = models.DecodeFsConfig(schedule.FsConfig)
	if err != nil {
		return err
	}

	b.Options, err = models.DecodeScheduleOptions(schedule.Options)
	if err != nil {
		return err
	}

	if schedule.StartAt.Valid {
		b.StartTime = schedule.StartAt.Time.Format("2006-01-02 15:04:05")
	} else {
		b.StartTime = ""
	}

	if schedule.EndAt.Valid {
		b.EndTime = schedule.EndAt.Time.Format("2006-01-02 15:04:05")
	} else {
		b.EndTime = ""
	}

	return nil
}

func validateScheduleTime(startTime, endTime string, currentTime time.Time) (startAt sql.NullTime, endAt sql.NullTime, err error) {
	// 校验starttime格式，starttime取值,根据starttime，生成sqlNullTime类型的startAt
	if startTime == "" {
		startAt = sql.NullTime{Valid: false}
	} else {
		startAt.Valid = true
		startAt.Time, err = time.ParseInLocation("2006-01-02 15:04:05", startTime, time.Local)
		if err != nil {
			errMsg := fmt.Sprintf("starttime[%s] format not correct, should be YYYY-MM-DD hh:mm:ss", startTime)
			return startAt, endAt, fmt.Errorf(errMsg)
		}

		if !startAt.Time.After(currentTime) {
			errMsg := fmt.Sprintf("starttime[%s] not after currentTime[%s]", startTime, currentTime.Format("2006-01-02 15:04:05"))
			return startAt, endAt, fmt.Errorf(errMsg)
		}
	}

	// 校验endTime格式，endtime取值，根据endTime，生成sqlNullTime
	if endTime == "" {
		endAt = sql.NullTime{Valid: false}
	} else {
		endAt.Valid = true
		endAt.Time, err = time.ParseInLocation("2006-01-02 15:04:05", endTime, time.Local)
		if err != nil {
			errMsg := fmt.Sprintf("endtime[%s] format not correct, should be YYYY-MM-DD hh:mm:ss", endTime)
			return startAt, endAt, fmt.Errorf(errMsg)
		}

		if !endAt.Time.After(currentTime) {
			errMsg := fmt.Sprintf("endtime[%s] not after currentTime[%s]", endTime, currentTime.Format("2006-01-02 15:04:05"))
			return startAt, endAt, fmt.Errorf(errMsg)
		}

		if startAt.Valid && !endAt.Time.After(startAt.Time) {
			errMsg := fmt.Sprintf("endtime[%s] not after startTime[%s]", endTime, startTime)
			return startAt, endAt, fmt.Errorf(errMsg)
		}
	}

	return startAt, endAt, nil
}

func CheckFsAndGetID(userName, fsUserName, fsName string) (fsID string, err error) {
	if fsUserName != "" {
		fsID = common.ID(fsUserName, fsName)
	} else {
		fsID = common.ID(userName, fsName)
	}

	fsService := fs.GetFileSystemService()
	hasPermission, err := fsService.HasFsPermission(userName, fsID)
	if err != nil {
		err := fmt.Errorf("check permission of user[%s] fsID[%s] failed, err: %v", userName, fsID, err)
		return fsID, err
	}

	if !hasPermission {
		err := fmt.Errorf("user[%s] has no permission to fsName[%s] with fsUser[%s]", userName, fsName, fsUserName)
		return fsID, err
	}

	return fsID, nil
}

func CreateSchedule(ctx *logger.RequestContext, request *CreateScheduleRequest) (CreateScheduleResponse, error) {
	// check schedule name pattern
	if !schema.CheckReg(request.Name, common.RegPatternScheduleName) {
		ctx.ErrorCode = common.InvalidNamePattern
		err := common.InvalidNamePatternError(request.Name, common.ResourceTypeSchedule, common.RegPatternScheduleName)
		ctx.Logging().Errorf("create schedule failed as schedule name illegal. error:%v", err)
		return CreateScheduleResponse{}, err
	}

	// 校验desc长度
	if len(request.Desc) > util.MaxDescLength {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("desc too long, should be less than %d", util.MaxDescLength)
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	// 校验Fs参数，并生成FsConfig对象
	_, err := CheckFsAndGetID(ctx.UserName, request.UserName, request.GlobalFsName)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		ctx.Logging().Errorf(err.Error())
		return CreateScheduleResponse{}, err
	}

	fsConfig := models.FsConfig{GlobalFsName: request.GlobalFsName, UserName: request.UserName}
	StrFsConfig, err := fsConfig.Encode(ctx.Logging())
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("create schedule failed, dump fsConfig[%v] error: %s", fsConfig, err.Error())
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	// 校验 & 生成options对象
	options, err := models.NewScheduleOptions(ctx.Logging(), request.Catchup, request.ExpireInterval, request.Concurrency, request.ConcurrencyPolicy)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("create schedule failed, err:[%s]", err.Error())
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	StrOptions, err := options.Encode(ctx.Logging())
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("create schedule failed, dump options[%v] error: %s", options, err.Error())
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	// 校验 starttime, endtime
	currentTime := time.Now()
	startAt, endAt, err := validateScheduleTime(request.StartTime, request.EndTime, currentTime)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("create schedule failed, %s", err.Error())
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	// 校验crontab
	cronSchedule, err := cron.ParseStandard(request.Crontab)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("check crontab failed in creating schedule. error:%v", err)
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	// 根据crontab, 以及startTime, 生成nextRunAt
	var nextRunAt time.Time
	if startAt.Valid {
		nextRunAt = cronSchedule.Next(startAt.Time)
	} else {
		nextRunAt = cronSchedule.Next(currentTime)
	}

	// 校验用户对pplID pplDetailID是否有权限
	hasAuth, _, _, err := CheckPipelineDetailPermission(ctx.UserName, request.PipelineID, request.PipelineDetailID)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		errMsg := fmt.Sprintf("create schedule failed, %s", err.Error())
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	} else if !hasAuth {
		ctx.ErrorCode = common.AccessDenied
		err := common.NoAccessError(ctx.UserName, common.ResourceTypePipeline, request.PipelineID)
		return CreateScheduleResponse{}, err
	}

	// 校验schedule是否存在，一个用户不能创建同名schedule
	_, err = models.GetScheduleByName(ctx.Logging(), request.Name, ctx.UserName)
	if err == nil {
		ctx.ErrorCode = common.DuplicatedName
		errMsg := fmt.Sprintf("CreateSchedule failed: user[%s] already has schedule with name[%s]", ctx.UserName, request.Name)
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("CreateSchedule failed: %s", err)
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	// create schedule in db after run.yaml validated
	schedule := models.Schedule{
		ID:               "", // to be back filled according to db pk
		Name:             request.Name,
		Desc:             request.Desc,
		PipelineID:       request.PipelineID,
		PipelineDetailID: request.PipelineDetailID,
		UserName:         ctx.UserName,
		FsConfig:         string(StrFsConfig),
		Crontab:          request.Crontab,
		Options:          string(StrOptions),
		Status:           models.ScheduleStatusRunning,
		StartAt:          startAt,
		EndAt:            endAt,
		NextRunAt:        nextRunAt,
	}

	scheduleID, err := models.CreateSchedule(ctx.Logging(), schedule)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("create schdule failed. error:%v", err)
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}

	// 给scheduler发创建channel信号
	err = SendSingnal(OpTypeCreate, scheduleID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("create schedule failed in sending create channel signal. error:%v", err)
		ctx.Logging().Errorf(errMsg)
		return CreateScheduleResponse{}, fmt.Errorf(errMsg)
	}
	ctx.Logging().Debugf("send create schedule channel succeed. scheduleID:%s", scheduleID)

	return CreateScheduleResponse{ScheduleID: scheduleID}, nil
}

// 给scheduler发创建channel信号
func SendSingnal(opType, scheduleID string) error {
	globalScheduler := GetGlobalScheduler()
	schduleOp, err := NewOpInfo(opType, scheduleID)
	if err != nil {
		return err
	}

	globalScheduler.OpsChannel <- schduleOp
	return nil
}

func ListSchedule(ctx *logger.RequestContext, marker string, maxKeys int, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter []string) (ListScheduleResponse, error) {
	ctx.Logging().Debugf("begin list schedule.")
	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return ListScheduleResponse{}, err
		}
	}

	// 只有root用户才能设置userFilter，否则只能查询当前普通用户创建的schedule列表
	if !common.IsRootUser(ctx.UserName) {
		if len(userFilter) != 0 {
			ctx.ErrorCode = common.InvalidArguments
			errMsg := fmt.Sprint("only root user can set userFilter!")
			ctx.Logging().Errorf(errMsg)
			return ListScheduleResponse{}, fmt.Errorf(errMsg)
		} else {
			userFilter = []string{ctx.UserName}
		}
	}

	// model list
	scheduleList, err := models.ListSchedule(ctx.Logging(), pk, maxKeys, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
	if err != nil {
		ctx.Logging().Errorf("models list schedule failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
		return ListScheduleResponse{}, err
	}
	listScheduleResponse := ListScheduleResponse{ScheduleList: []ScheduleBrief{}}

	// get next marker
	listScheduleResponse.IsTruncated = false
	if len(scheduleList) > 0 {
		schedule := scheduleList[len(scheduleList)-1]
		isLastPk, err := models.IsLastSchedulePk(ctx.Logging(), schedule.Pk, pplFilter, pplDetailFilter, userFilter, scheduleFilter, nameFilter, statusFilter)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			errMsg := fmt.Sprintf("get last schedule Pk failed. err:[%s]", err.Error())
			ctx.Logging().Errorf(errMsg)
			return ListScheduleResponse{}, fmt.Errorf(errMsg)
		}

		if !isLastPk {
			nextMarker, err := common.EncryptPk(schedule.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					schedule.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return ListScheduleResponse{}, err
			}
			listScheduleResponse.NextMarker = nextMarker
			listScheduleResponse.IsTruncated = true
		}
	}
	listScheduleResponse.MaxKeys = maxKeys

	// append schedule briefs
	for _, schedule := range scheduleList {
		scheduleBrief := ScheduleBrief{}
		err := scheduleBrief.updateFromScheduleModel(schedule)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			return ListScheduleResponse{}, err
		}
		listScheduleResponse.ScheduleList = append(listScheduleResponse.ScheduleList, scheduleBrief)
	}
	return listScheduleResponse, nil
}

func getSchedule(ctx *logger.RequestContext, scheduleID string) (models.Schedule, error) {
	ctx.Logging().Debugf("begin get schedule by id. scheduleID:%s", scheduleID)
	schedule, err := models.GetSchedule(ctx.Logging(), scheduleID)
	if err != nil {
		ctx.Logging().Errorln(err.Error())
		return models.Schedule{}, err
	}

	if !common.IsRootUser(ctx.UserName) && ctx.UserName != schedule.UserName {
		err := common.NoAccessError(ctx.UserName, common.ResourceTypeSchedule, scheduleID)
		ctx.Logging().Errorln(err.Error())
		return models.Schedule{}, err
	}
	return schedule, nil
}

func GetSchedule(ctx *logger.RequestContext, scheduleID string,
	marker string, maxKeys int, runFilter, statusFilter []string) (GetScheduleResponse, error) {
	ctx.Logging().Debugf("begin get schedule[%s]", scheduleID)

	// check schedule exist && user access right
	schedule, err := getSchedule(ctx, scheduleID)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		err := fmt.Errorf("get schedule[%s] failed. err:%v", scheduleID, err)
		ctx.Logging().Errorf(err.Error())
		return GetScheduleResponse{}, err
	}

	userFilter, fsFilter, nameFilter := make([]string, 0), make([]string, 0), make([]string, 0)
	scheduleIDFilter := []string{scheduleID}
	listRunResponse, err := ListRun(ctx, marker, maxKeys, userFilter, fsFilter, runFilter, nameFilter, statusFilter, scheduleIDFilter)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("list run for schedule[%s] failed. err:[%s]", scheduleID, err.Error())
		return GetScheduleResponse{}, err
	}

	getScheduleResponse := GetScheduleResponse{ListRunResponse: listRunResponse}
	err = getScheduleResponse.ScheduleBrief.updateFromScheduleModel(schedule)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		return GetScheduleResponse{}, err
	}

	return getScheduleResponse, nil
}

// todo: 支持 StopRun
func StopSchedule(ctx *logger.RequestContext, scheduleID string) error {
	ctx.Logging().Debugf("begin stop schedule: %s", scheduleID)
	// check schedule exist && user access right
	schedule, err := getSchedule(ctx, scheduleID)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		err := fmt.Errorf("stop schedule[%s] failed. %s", scheduleID, err.Error())
		ctx.Logging().Errorf(err.Error())
		return err
	}

	// check schedule current status
	if models.IsScheduleFinalStatus(schedule.Status) {
		ctx.ErrorCode = common.ActionNotAllowed
		err := fmt.Errorf("stop schedule[%s] failed, already in status[%s]", scheduleID, schedule.Status)
		ctx.Logging().Errorln(err.Error())
		return err
	}

	if err := models.UpdateScheduleStatus(ctx.Logging(), scheduleID, models.ScheduleStatusTerminated); err != nil {
		errMsg := fmt.Sprintf("stop schedule failed updating db")
		ctx.ErrorCode = common.InternalError
		return fmt.Errorf(errMsg)
	}

	// 给scheduler发stop channel信号
	err = SendSingnal(OpTypeStop, scheduleID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("stop schedule failed in sending stop channel signal. error:%v", err)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	ctx.Logging().Debugf("send stop schedule channel succeed. scheduleID:%s", scheduleID)

	return nil
}

// todo: 支持 StopRun
func DeleteSchedule(ctx *logger.RequestContext, scheduleID string) error {
	ctx.Logging().Debugf("begin delete schedule: %s", scheduleID)
	// check schedule exist && user access right
	schedule, err := getSchedule(ctx, scheduleID)
	if err != nil {
		ctx.ErrorCode = common.InvalidArguments
		err := fmt.Errorf("delete schedule[%s] failed. %s", scheduleID, err.Error())
		ctx.Logging().Errorf(err.Error())
		return err
	}

	// check final status
	if !models.IsScheduleFinalStatus(schedule.Status) {
		ctx.ErrorCode = common.ActionNotAllowed
		err := fmt.Errorf("delete schedule[%s] in status[%s] failed. only schedules in final status: %v can be deleted", schedule.ID, schedule.Status, models.ScheduleFinalStatusList)
		ctx.Logging().Errorln(err.Error())
		return err
	}

	// delete
	if err := models.DeleteSchedule(ctx.Logging(), scheduleID); err != nil {
		errMsg := fmt.Sprintf("models delete run[%s] failed. error:%s", scheduleID, err.Error())
		ctx.ErrorCode = common.InternalError
		return fmt.Errorf(errMsg)
	}

	// 给scheduler发delete channel信号
	err = SendSingnal(OpTypeDelete, scheduleID)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		errMsg := fmt.Sprintf("stop schedule failed in sending stop channel signal. error:%v", err)
		ctx.Logging().Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	ctx.Logging().Debugf("send delete schedule channel succeed. scheduleID:%s", scheduleID)

	return nil
}
