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
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
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
	ConcurrencyChannel chan string
}

func NewScheduler() Scheduler {
	scheduler := Scheduler{}
	scheduler.OpsChannel = make(chan OpInfo)
	scheduler.ConcurrencyChannel = make(chan string)
	return scheduler
}

var globalScheduler *Scheduler

func GetGlobalScheduler() *Scheduler {
	if globalScheduler == nil {
		scheduler := NewScheduler()
		globalScheduler = &scheduler
	}

	return globalScheduler
}

// 开启scheduler
// 1. 查询数据库，寻找是否有到时的周期调度，有的话就发起任务，并更新休眠时间(下一个最近的周期调度时间)
// 2. 开始for循环，每个循环监听三类信号：超时信号，用户操作信号，并发空闲信号
func (s *Scheduler) Start() {
	// todo：异常处理要怎么做

	// 在start scheduler初始阶段，在考虑catchup的前提下，发起任务
	timeout, err := s.dealWithTimout(true)
	if err != nil {
		log.Errorf("start scheduler failed, %s", err.Error())
		return
	}

	for {
		select {
		case opInfo := <-s.OpsChannel:
			toUpdate, tmpTimeout, err := s.dealWithOps(opInfo)
			if err != nil {
				log.Errorf("scheduler deal with op[%v] failed, %s", err.Error())
				return
			}
			if toUpdate {
				timeout = tmpTimeout
			}
		case <-timeout:
			// 在循环过程中，发起任务不需要考虑catchup配置（肯定catchup）
			tmpTimeout, err := s.dealWithTimout(false)
			if err != nil {
				log.Errorf("scheduler deal with timeout failed, %s", err.Error())
				return
			}
			timeout = tmpTimeout
		case scheduleID := <-s.ConcurrencyChannel:
			toUpdate, tmpTimeout, err := s.dealWithConcurrency(scheduleID)
			if err != nil {
				log.Errorf("scheduler deal with cncurrency change of schedule[%s] failed, %s", scheduleID, err.Error())
				return
			}
			if toUpdate {
				timeout = tmpTimeout
			}
		}
	}
}

func (s *Scheduler) getTimeout(nextWakeupTime *time.Time) <-chan time.Time {
	var timeout <-chan time.Time
	if nextWakeupTime == nil {
		timeout = nil
	} else {
		currentTime := time.Now()
		if currentTime.Before(*nextWakeupTime) || currentTime.Equal(*nextWakeupTime) {
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
// - 计算timeout不需要加行级锁，因为只影响休眠时间
//
// 对于 stop/delete 操作，可以不再计算timeout
// - 如果停止的schedule，【不是】下一次wakeup要执行的，那对timeout毫无影响
// - 如果停止的schedule恰好是下一次wakeup要执行的，那只是导致一次无效的wakeup而已
//   - 一次无效的timeout，代价是一次扫表；但是为了避免无效的timeout，这里也要扫表，代价是一致的。
func (s *Scheduler) dealWithOps(opInfo OpInfo) (toUpdate bool, timeout <-chan time.Time, err error) {
	log.Debugf("begin to deal with shedule op[%s] of schedule[%s]", opInfo.GetOpType(), opInfo.GetScheduleID())

	opType := opInfo.GetOpType()
	if opType == OpTypeStop || opType == OpTypeDelete {
		return false, nil, nil
	}

	logEntry := log.WithFields(log.Fields{})
	nextWakeupTime, err := models.GetNextGlobalWakeupTime(logEntry)
	if err != nil {
		return false, nil, err
	}

	timeout = s.getTimeout(nextWakeupTime)
	return true, timeout, nil
}

// 处理到时信号，主要分成以下步骤：
// 1. 查询数据库
//  - 获取需要发起的周期调度，更新对应next_run_at
//  - 更新到达end_time的周期调度状态
//  - 获取下一个wakeup时间(如果不存在则是空指针)
// 2. 发起到期的任务
// 3. 休眠
func (s *Scheduler) dealWithTimout(checkCatchup bool) (<-chan time.Time, error) {
	logEntry := log.WithFields(log.Fields{})
	killMap, execMap, nextWakeupTime, err := models.GetAvailableSchedule(logEntry, checkCatchup)
	if err != nil {
		return nil, err
	}

	// todo：根据execMap，发起周期任务
	fmt.Print(execMap)
	fmt.Print(killMap)

	timeout := s.getTimeout(nextWakeupTime)
	return timeout, nil
}

// 1. 判断要不要重新计算全局timeout（计算耗时，尽量过滤非必需场景）
// - 如果当前schdule状态不是running，不做任何处理
// - 如果 concurrencyPolicy 不是suspend，不用处理
// - 查询当前schedule的并发度，如果当前并发度>=concurrency，不做任何处理
//
// 2. 计算下一个全局timeout时间
// - 计算timeout，如果有 schedule 的 next_run_at 在 expire_interval以外，会直接把 timeout 设置为0
//   - 有过期任务，此处不会更新next_run_at，而是马上触发dealWithTimout函数处理
// - todo：可以改成计算这个周期调度的下一次timeout时间，并且与全局timeout进行比较&替换（如果有需要），效率可能有提升
//
// 注意：该函数并不会真正运行任务，或者更新schedule数据库状态。跟dealWithOps一样，只会更新timeout
func (s *Scheduler) dealWithConcurrency(scheduleID string) (toUpdate bool, timeout <-chan time.Time, err error) {
	logEntry := log.WithFields(log.Fields{})
	schedule, err := models.GetSchedule(logEntry, scheduleID)
	if err != nil {
		return false, nil, err
	}

	if schedule.Status != models.ScheduleStatusRunning {
		return false, nil, nil
	}

	options := models.ScheduleOptions{}
	if err := json.Unmarshal([]byte(schedule.Options), &options); err != nil {
		errMsg := fmt.Sprintf("decode optinos[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, scheduleID, err)
		log.Errorf(errMsg)
		return false, nil, fmt.Errorf(errMsg)
	}

	if options.ConcurrencyPolicy != models.ConcurrencyPolicySuspend {
		return false, nil, nil
	}

	// 查询当前schedule的并发度，如果当前并发度>=concurrency，不做任何处理
	notEndedList := []string{common.StatusRunInitiating, common.StatusRunPending, common.StatusRunRunning, common.StatusRunTerminating}
	scheduleIDFilter := []string{scheduleID}
	count, err := models.CountRun(logEntry, 0, 0, nil, nil, nil, nil, notEndedList, scheduleIDFilter)
	if err != nil {
		errMsg := fmt.Sprintf("count notEnded runs for schedule[%s] failed. error:%s", scheduleID, err.Error())
		log.Errorf(errMsg)
		return false, nil, fmt.Errorf(errMsg)
	}

	if int(count) >= options.Concurrency {
		return false, nil, nil
	}

	// 计算timeout，如果有 schedule 的 next_run_at 在 expire_interval以外，会直接把 timeout 设置为0
	// 有过期任务，此处不会更新next_run_at，而是马上触发dealWithTimout函数处理
	var nextWakeupTime *time.Time
	nextWakeupTime, err = models.GetNextGlobalWakeupTime(logEntry)
	if err != nil {
		return false, nil, err
	}

	timeout = s.getTimeout(nextWakeupTime)
	return true, timeout, nil
}
