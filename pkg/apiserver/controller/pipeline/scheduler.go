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
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

const (
	OpTypeCreate = "create"
	OpTypeStop   = "stop"
	OpTypeDelete = "delete"
)

type OpInfo struct {
	opType     string
	scheduleID string
}

func NewOpInfo(opType string, scheduleID string) (OpInfo, error) {
	if opType != OpTypeCreate && opType != OpTypeStop && opType != OpTypeDelete {
		errMsg := fmt.Sprintf("optype[%s] not supported", opType)
		return OpInfo{}, fmt.Errorf(errMsg)
	}

	opInfo := OpInfo{opType: opType, scheduleID: scheduleID}
	return opInfo, nil
}

func (opInfo OpInfo) GetOpType() string {
	return opInfo.opType
}

func (opInfo OpInfo) GetScheduleID() string {
	return opInfo.scheduleID
}

type Scheduler struct {
	OpsChannel         chan OpInfo //用于监听用户操作的channel
	ConcurrencyChannel chan string //用于监听任务结束导致concurrency变化的channel
}

// Scheduler初始化函数，但是别的脚本不能访问，只能通过下面 GetGlobalScheduler 单例函数获取 Scheduler 实例
func newScheduler() Scheduler {
	scheduler := Scheduler{}
	scheduler.OpsChannel = make(chan OpInfo)
	scheduler.ConcurrencyChannel = make(chan string)
	return scheduler
}

var globalScheduler *Scheduler
var mu sync.Mutex

// 单例函数，获取 Scheduler 实例
func GetGlobalScheduler() *Scheduler {
	if globalScheduler == nil {
		mu.Lock()
		defer mu.Unlock()

		if globalScheduler == nil {
			scheduler := newScheduler()
			globalScheduler = &scheduler
		}
	}

	return globalScheduler
}

// 开启scheduler
// 1. 查询数据库，寻找是否有到时的周期调度，有的话就发起任务，并更新休眠时间(下一个最近的周期调度时间)
// 2. 开始for循环，每个循环监听三类信号：超时信号，用户操作信号，并发空闲信号
func (s *Scheduler) Start() {
	// todo：异常处理要怎么做

	// 在start scheduler初始阶段，在考虑catchup的前提下，发起任务
	nextWakeupTime, err := s.dealWithTimout(true)
	if err != nil {
		logger.Logger().Errorf("start scheduler failed, %s", err.Error())
		return
	}
	timeout := s.getTimeout(nextWakeupTime)
	if nextWakeupTime != nil {
		logger.Logger().Infof("after initialization: nextWakeupTime[%s]", nextWakeupTime.Format("2006-01-02 15:04:05"))
	} else {
		logger.Logger().Info("after initialization: no need to wakeup")
	}

	var toUpdate bool
	var tmpNextWakeupTime *time.Time
	for {
		select {
		case opInfo := <-s.OpsChannel:
			logger.Logger().Infof("begin deal with op[%v]", opInfo)
			toUpdate, tmpNextWakeupTime, err = s.dealWithOps(opInfo)
			if err != nil {
				logger.Logger().Errorf("scheduler deal with op[%v] failed, %s", opInfo, err.Error())
				continue
			}
		case <-timeout:
			// 在循环过程中，发起任务不需要检查catchup配置（肯定catchup==true）
			logger.Logger().Infof("begin deal with timeout")
			checkCatchup := false
			tmpNextWakeupTime, err = s.dealWithTimout(checkCatchup)
			if err != nil {
				logger.Logger().Errorf("scheduler deal with timeout failed, %s", err.Error())
				continue
			}
			toUpdate = true
		case scheduleID := <-s.ConcurrencyChannel:
			logger.Logger().Infof("begin deal with concurrency change of schedule[%s]", scheduleID)
			toUpdate, tmpNextWakeupTime, err = s.dealWithConcurrency(scheduleID, nextWakeupTime)
			if err != nil {
				logger.Logger().Errorf("scheduler deal with cncurrency change of schedule[%s] failed, %s", scheduleID, err.Error())
				continue
			}
		}

		if toUpdate {
			logger.Logger().Infof("update nextWakeupTime, origin:[%s], new:[%s]", s.formatTime(nextWakeupTime), s.formatTime(tmpNextWakeupTime))
			nextWakeupTime = tmpNextWakeupTime
			timeout = s.getTimeout(nextWakeupTime)
			toUpdate = false
		}
	}
}

func (s *Scheduler) formatTime(timeToFormat *time.Time) string {
	if timeToFormat == nil {
		return "None"
	} else {
		return timeToFormat.Format("2006-01-02 15:04:05")
	}
}

func (s *Scheduler) getTimeout(nextWakeupTime *time.Time) <-chan time.Time {
	var timeout <-chan time.Time
	if nextWakeupTime == nil {
		timeout = nil
	} else {
		currentTime := time.Now()
		if nextWakeupTime.Before(currentTime) || nextWakeupTime.Equal(currentTime) {
			timeout = time.After(0 * time.Second)
		} else {
			timeout = time.After(nextWakeupTime.Sub(currentTime))
		}
	}

	return timeout
}

// 该函数不会根据用户操作更新数据库
// - 例如创建schedule记录，更新schedule状态为stop，deleted等
// - 这是api-server controller已经做了的事情
//
// 该函数只需要重新计算timeout时间
// - 该函数不会执行周期任务，如果有到时的周期调度，只需设置timeout为0即可
// - 计算timeout，如果有 schedule 的 next_run_at 在 expire_interval以外，会直接把 timeout 设置为0
//   - 有过期任务，此处不会更新next_run_at，而是马上触发dealWithTimout函数处理
//
// 对于 stop/delete 操作，可以不再计算timeout
// - 如果停止的schedule，【不是】下一次wakeup要执行的，那对timeout毫无影响
// - 如果停止的schedule恰好是下一次wakeup要执行的，那只是导致一次无效的wakeup而已
//   - 一次无效的timeout，代价是一次扫表；但是为了避免无效的timeout，这里也要扫表，代价是一致的。
func (s *Scheduler) dealWithOps(opInfo OpInfo) (toUpdate bool, timeout *time.Time, err error) {
	logger.Logger().Debugf("begin to deal with shedule op[%s] of schedule[%s]", opInfo.GetOpType(), opInfo.GetScheduleID())

	opType := opInfo.GetOpType()
	if opType == OpTypeStop || opType == OpTypeDelete {
		return false, nil, nil
	}

	nextWakeupTime, err := models.GetNextGlobalWakeupTime(logger.Logger())
	if err != nil {
		return false, nil, err
	}

	return true, nextWakeupTime, nil
}

// 处理到时信号，主要分成以下步骤：
// 1. 查询数据库
//  - 获取需要发起的周期调度，更新对应next_run_at
//  - 更新到达end_time的周期调度状态
//  - 获取下一个wakeup时间(如果不存在则是空指针)
// 2. 发起到期的任务
// 3. 休眠
func (s *Scheduler) dealWithTimout(checkCatchup bool) (*time.Time, error) {
	killMap, execMap, nextWakeupTime, err := models.GetAvailableSchedule(logger.Logger(), checkCatchup)
	if err != nil {
		logger.Logger().Errorf("GetAvailableSchedule failed, err:[%s]", err.Error())
		return nil, err
	}

	logger.Logger().Infof("execMap:[%v], killMap:[%v]", execMap, killMap)

	// 根据execMap，发起周期任务，发起任务失败了只打日志，不影响调度
	for scheduleID, nextRunAtList := range execMap {
		logger.Logger().Infof("start to create runs[%v] for schedule[%s]", nextRunAtList, scheduleID)
		schedule, err := models.GetSchedule(logger.Logger(), scheduleID)
		if err != nil {
			logger.Logger().Errorf("skip createRun for schedule[%s] with nextRunAtList[%v] in scheduler, getSchedule err:[%s]", scheduleID, nextRunAtList, err.Error())
			continue
		}

		fsConfig, err := models.DecodeFsConfig(schedule.FsConfig)
		if err != nil {
			logger.Logger().Errorf("skip createRun for schedule[%s] with nextRunAtList[%v] in scheduler, decodeFsConfig err:[%s]", scheduleID, nextRunAtList, err.Error())
			continue
		}

		for _, nextRunAt := range nextRunAtList {
			logger.Logger().Infof("start to create run in ScheduledAt[%s] for schedule[%s]", nextRunAt.Format("2006-01-02 15:04:05"), scheduleID)
			createRequest := CreateRunRequest{
				FsName:           fsConfig.FsName,
				UserName:         fsConfig.UserName,
				Name:             schedule.Name,
				Description:      schedule.Desc,
				PipelineID:       schedule.PipelineID,
				PipelineDetailID: schedule.PipelineDetailID,
				ScheduleID:       schedule.ID,
				ScheduledAt:      nextRunAt.Format("2006-01-02 15:04:05"),
			}

			// generate request id for run create
			ctx := logger.RequestContext{
				UserName:  schedule.UserName,
				RequestID: uuid.NewString(),
			}
			_, err := CreateRun(ctx, &createRequest)
			if err != nil {
				logger.Logger().Errorf("create run for schedule[%s] in ScheduledAt[%s] failed, err:[%s]", scheduleID, nextRunAt.Format("2006-01-02 15:04:05"), err.Error())
				continue
			}
		}
	}

	// 根据killMap，停止run
	for scheduleID, runIDList := range killMap {
		logger.Logger().Infof("start to stop runs[%v] for schedule[%s]", runIDList, scheduleID)
		schedule, err := models.GetSchedule(logger.Logger(), scheduleID)
		if err != nil {
			logger.Logger().Errorf("skip StopRun for schedule[%s] with runIDList[%s] in scheduler, getSchedule err:[%s]", scheduleID, runIDList, err.Error())
			continue
		}

		for _, runID := range runIDList {
			logger.Logger().Infof("start to stop run[%s] for schedule[%s]", runID, scheduleID)
			request := UpdateRunRequest{StopForce: false}
			err = StopRun(logger.Logger(), schedule.UserName, runID, request)
			if err != nil {
				logger.Logger().Errorf("stop run[%s] failed for schedule[%s], err:[%s]", runID, scheduleID, err.Error())
				continue
			}
		}
	}

	return nextWakeupTime, nil
}

// 1. 判断要不要重新计算全局timeout（计算耗时，尽量过滤非必需场景）
// - 如果当前schdule状态不是running，不做任何处理
// - 查询当前schedule的并发度，如果当前并发度>=concurrency，不做任何处理
// - 如果 concurrencyPolicy 不是suspend，不用处理
//
// 2. 计算下一个全局timeout时间
// - 计算timeout，如果有 schedule 的 next_run_at 在 expire_interval以外，会直接把 timeout 设置为0
//   - 有过期任务，此处不会更新next_run_at，而是马上触发dealWithTimout函数处理
// - todo：可以改成计算这个周期调度的下一次timeout时间，并且与全局timeout进行比较&替换（如果有需要），效率可能有提升
//
// 注意：该函数并不会真正运行任务，或者更新schedule数据库状态。跟dealWithOps一样，只会更新timeout
func (s *Scheduler) dealWithConcurrency(scheduleID string, originNextWakeupTime *time.Time) (toUpdate bool, timeout *time.Time, err error) {
	schedule, err := models.GetSchedule(logger.Logger(), scheduleID)
	if err != nil {
		return false, nil, err
	}

	if schedule.Status != models.ScheduleStatusRunning {
		logger.Logger().Infof("schedule[%s] not running, doing nothing", scheduleID)
		return false, nil, nil
	}

	options, err := models.DecodeScheduleOptions(schedule.Options)
	if err != nil {
		errMsg := fmt.Sprintf("decode options[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, scheduleID, err)
		logger.Logger().Errorf(errMsg)
		return false, nil, fmt.Errorf(errMsg)
	}

	// 如果scheduler的 ConcurrencyPolicy 不是 Suspend，就只需要到时间就运行 or skip，这些操作在deal with timeout中处理
	if options.ConcurrencyPolicy != models.ConcurrencyPolicySuspend {
		logger.Logger().Infof("schedule[%s] ConcurrencyPolicy not suspend, doing nothing", scheduleID)
		return false, nil, nil
	}

	// 查询当前schedule的并发度，如果当前并发度>=concurrency，不做任何处理
	count, err := models.CountActiveRunsForSchedule(logger.Logger(), scheduleID)
	if err != nil {
		errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", scheduleID, err.Error())
		logger.Logger().Errorf(errMsg)
		return false, nil, fmt.Errorf(errMsg)
	}

	if int(count) >= options.Concurrency {
		return false, nil, nil
	}

	if originNextWakeupTime == nil || (*originNextWakeupTime).After(schedule.NextRunAt) {
		return true, &schedule.NextRunAt, nil
	} else {
		return false, nil, nil
	}
}
