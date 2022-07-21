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
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	cron "github.com/robfig/cron/v3"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
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

	nextWakeupTime := time.Now()
	nextWakeupTimePtr := &nextWakeupTime
	timeout := s.getTimeout(nextWakeupTimePtr)

	var toUpdate bool
	var tmpNextWakeupTime *time.Time
	var err error
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
			tmpNextWakeupTime, err = s.dealWithTimeout()
			if err != nil {
				logger.Logger().Errorf("scheduler deal with timeout failed, %s", err.Error())
				continue
			}
			toUpdate = true
		case scheduleID := <-s.ConcurrencyChannel:
			logger.Logger().Infof("begin deal with concurrency change of schedule[%s]", scheduleID)
			toUpdate, tmpNextWakeupTime, err = s.dealWithConcurrency(scheduleID, nextWakeupTimePtr)
			if err != nil {
				logger.Logger().Errorf("scheduler deal with cncurrency change of schedule[%s] failed, %s", scheduleID, err.Error())
				continue
			}
		}

		if toUpdate {
			logger.Logger().Infof("update nextWakeupTime, origin:[%s], new:[%s]", s.formatTime(nextWakeupTimePtr), s.formatTime(tmpNextWakeupTime))
			nextWakeupTimePtr = tmpNextWakeupTime
			timeout = s.getTimeout(nextWakeupTimePtr)
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
//   - 有过期任务，此处不会更新next_run_at，而是马上触发dealWithTimeout函数处理
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

func (s *Scheduler) getEarlierTime(time1, time2 time.Time) time.Time {
	if time1.Before(time2) {
		return time1
	} else {
		return time2
	}
}

func (s *Scheduler) checkNextRunAt(nextRunAt, currentTime time.Time, endAt sql.NullTime) bool {
	if nextRunAt.After(currentTime) {
		return false
	}
	if endAt.Valid && nextRunAt.After(endAt.Time) {
		return false
	}

	return true
}

// 先处理同时满足currentTime之前，而且schedule.EndAt之前的任务
// 只需要处理 catchup == true 的case
// - 如果catchup == false，即不需要catchup，则currentTime和schedule.EndAt前，所有miss的周期任务都被抛弃，不再发起
func (s *Scheduler) generateRunListForSchedule(schedule models.Schedule, currentTime time.Time, activeCount int) (expiredList, execList, skipList []time.Time, nextRunAt time.Time, stopCount int, err error) {
	logger.Logger().Infof("generateRunListForSchedule with init activeCount[%d], schedule[%v], currentTime[%s]",
		activeCount, schedule, s.formatTime(&currentTime))

	options, err := models.DecodeScheduleOptions(schedule.Options)
	if err != nil {
		errMsg := fmt.Sprintf("decode options of schedule[%s] failed. error: %v", schedule.ID, err)
		return expiredList, execList, skipList, nextRunAt, stopCount, fmt.Errorf(errMsg)
	}

	cronSchedule, err := cron.ParseStandard(schedule.Crontab)
	if err != nil {
		errMsg := fmt.Sprintf("parse crontab spec[%s] for schedule[%s] of pipeline detail[%s] failed, errMsg[%s]",
			schedule.Crontab, schedule.ID, schedule.PipelineVersionID, err.Error())
		return expiredList, execList, skipList, nextRunAt, stopCount, fmt.Errorf(errMsg)
	}

	totalCount := activeCount
	nextRunAt = schedule.NextRunAt
	expire_interval_durtion := time.Duration(options.ExpireInterval) * time.Second
	for ; s.checkNextRunAt(nextRunAt, currentTime, schedule.EndAt); nextRunAt = cronSchedule.Next(nextRunAt) {
		logger.Logger().Infof("start to check schedule[%s] at %s, with schedule.EndAt[%s]",
			schedule.ID, s.formatTime(&nextRunAt), s.formatTime(&(schedule.EndAt.Time)))

		if options.ExpireInterval != 0 && nextRunAt.Add(expire_interval_durtion).Before(currentTime) {
			logger.Logger().Infof("skip nextRunAt[%s] of schedule[%s], beyond expire interval[%d] from currentTime[%s]",
				s.formatTime(&nextRunAt), schedule.ID, options.ExpireInterval, s.formatTime(&currentTime))
			expiredList = append(expiredList, nextRunAt)
			continue
		}

		if options.Concurrency == 0 || totalCount < options.Concurrency {
			execList = append(execList, nextRunAt)
			totalCount += 1
		} else {
			if options.ConcurrencyPolicy == models.ConcurrencyPolicySuspend {
				// 直接跳出循环，不会继续更新nextRunAt
				infoMsg := fmt.Sprintf("concurrency of schedule with ID[%s] already reach[%d], so suspend", schedule.ID, options.Concurrency)
				logger.Logger().Info(infoMsg)
				break
			} else if options.ConcurrencyPolicy == models.ConcurrencyPolicyReplace {
				// replace策略下，得先记录所有可运行的run，然后才能够得出哪那些要运行，哪些不要
				execList = append(execList, nextRunAt)
				totalCount += 1
			} else if options.ConcurrencyPolicy == models.ConcurrencyPolicySkip {
				// 不跳出循环，会继续更新nextRunAt
				infoMsg := fmt.Sprintf("concurrency of schedule with ID[%s] already reach[%d], so skip", schedule.ID, options.Concurrency)
				skipList = append(skipList, nextRunAt)
				logger.Logger().Info(infoMsg)
			}
		}
	}

	// Concurrency != 0，即存在并发度限制，而且ConcurrencyPolicy == replace时，有可能【待发起任务 + 运行中任务】>= concurrency
	// 此时判断是否需要截取一部分待运行任务，以及停止一些已经启动的任务
	if options.Concurrency != 0 && options.ConcurrencyPolicy == models.ConcurrencyPolicyReplace {
		logger.Logger().Infof("process execList[%v], skipList[%v] for schedule[%s] in concurrency[%d] policy[%s]",
			execList, skipList, schedule.ID, options.Concurrency, options.ConcurrencyPolicy)

		if totalCount > options.Concurrency {
			if len(execList) >= options.Concurrency {
				skipList = append(skipList, execList[:len(execList)-options.Concurrency]...)
				execList = execList[len(execList)-options.Concurrency:]
				stopCount = int(activeCount)
			} else {
				stopCount = totalCount - options.Concurrency
			}
		}
	}

	return expiredList, execList, skipList, nextRunAt, stopCount, nil
}

func (s *Scheduler) createRun(schedule models.Schedule, fsConfig models.FsConfig, nextRunAt time.Time, status, msg string) {
	logger.Logger().Infof("start to create run in ScheduledAt[%s] for schedule[%s] with status[%s]",
		s.formatTime(&nextRunAt), schedule.ID, status)
	createRequest := CreateRunRequest{
		UserName:          fsConfig.Username,
		Name:              schedule.Name,
		Description:       schedule.Desc,
		PipelineID:        schedule.PipelineID,
		PipelineVersionID: schedule.PipelineVersionID,
		ScheduleID:        schedule.ID,
		ScheduledAt:       s.formatTime(&nextRunAt),
	}

	// generate request id for run create
	ctx := logger.RequestContext{
		UserName:  schedule.UserName,
		RequestID: uuid.NewString(),
	}

	extra := map[string]string{}
	if status != "" {
		extra[FinalRunStatus] = status
		extra[FinalRunMsg] = msg
	}
	_, err := CreateRun(ctx, &createRequest, extra)
	if err != nil {
		logger.Logger().Errorf("create run for schedule[%s] in ScheduledAt[%s] failed, err:[%s]", schedule.ID, s.formatTime(&nextRunAt), err.Error())
	}
}

func (s *Scheduler) stopRun(runID string, schedule models.Schedule) {
	logger.Logger().Infof("start to stop run[%s] for schedule[%s]", runID, schedule.ID)
	request := UpdateRunRequest{StopForce: false}
	err := StopRun(logger.Logger(), schedule.UserName, runID, request)
	if err != nil {
		logger.Logger().Errorf("stop run[%s] failed for schedule[%s], err:[%s]", runID, schedule.ID, err.Error())
	}
}

func (s *Scheduler) processRunList(
	schedule models.Schedule, options models.ScheduleOptions, fsConfig models.FsConfig, currentTime time.Time,
	expiredList, skipList, execList []time.Time, stopCount int, activeRuns []models.Run) {
	// 根据调度时间，先处理expiredList，创建状态为skipped的run，发起任务失败了只打日志，不影响周期调度
	for _, expiredRunAt := range expiredList {
		status := common.StatusRunSkipped
		runMsg := fmt.Sprintf("skip run of schedule[%s] with schedule time[%s], beyond expire interval[%d] before currentTime[%s]",
			schedule.ID, s.formatTime(&expiredRunAt), options.ExpireInterval, s.formatTime(&currentTime))
		logger.Logger().Info(runMsg)
		s.createRun(schedule, fsConfig, expiredRunAt, status, runMsg)
	}

	if options.ConcurrencyPolicy == models.ConcurrencyPolicyReplace {
		// 根据调度时间，replace策略下，需要为在创建正常run前，处理skipList，创建状态为skipped的run
		// 发起任务失败了只打日志，不影响周期调度
		for _, skipRunAt := range skipList {
			status := common.StatusRunSkipped
			runMsg := fmt.Sprintf("skip run of schedule[%s] with schedule time[%s], concurrency already reach[%d] in policy[%s]",
				schedule.ID, s.formatTime(&skipRunAt), options.Concurrency, options.ConcurrencyPolicy)
			logger.Logger().Info(runMsg)
			s.createRun(schedule, fsConfig, skipRunAt, status, runMsg)
		}
	}

	// 再根据 execList，发起run，发起任务失败了只打日志，不影响周期调度
	for _, execRunAt := range execList {
		s.createRun(schedule, fsConfig, execRunAt, "", "")
	}

	if options.ConcurrencyPolicy == models.ConcurrencyPolicySkip {
		// 根据调度时间，skip策略下，需要为在创建正常run后，处理skipList，创建状态为skipped的run
		// 发起任务失败了只打日志，不影响周期调度
		for _, skipRunAt := range skipList {
			status := common.StatusRunSkipped
			runMsg := fmt.Sprintf("skip run of schedule[%s] with schedule time[%s], concurrency already reach[%d] in policy[%s]",
				schedule.ID, s.formatTime(&skipRunAt), options.Concurrency, options.ConcurrencyPolicy)
			logger.Logger().Info(runMsg)
			s.createRun(schedule, fsConfig, skipRunAt, status, runMsg)
		}
	}

	// 最后根据 stopCount，stop周期调度前的active run，停止任务失败了只打日志，不影响周期调度
	for i := 0; i < stopCount; i++ {
		s.stopRun(activeRuns[i].ID, schedule)
	}
}

func (s *Scheduler) updateScheduleAndWakeupTime(schedule models.Schedule, currentTime, nextRunAt time.Time, nextWakeupTime *time.Time) *time.Time {
	logger.Logger().Infof("before updateScheduleAndWakeupTime for schedule[%s], nextRunAt[%s], currentTime[%s]",
		schedule.ID, s.formatTime(&nextRunAt), s.formatTime(&currentTime))

	// 更新 NextRunAt 字段
	to_update := false
	if !nextRunAt.Equal(schedule.NextRunAt) {
		schedule.NextRunAt = nextRunAt
		to_update = true
	}

	// 更新 status 字段
	if schedule.EndAt.Valid && nextRunAt.After(schedule.EndAt.Time) {
		schedule.Status = models.ScheduleStatusSuccess
		to_update = true
	}

	// 更新异常不能影响调度，先只打日志（否则影响整个server的逻辑）
	// todo: 这就需要每次schedule开始调度前，判断当前的schedule run发起情况，那是否还需要nextRunAt？
	if to_update {
		result := storage.DB.Model(&schedule).Save(schedule)
		if result.Error != nil {
			errMsg := fmt.Sprintf("update schedule[%s] of pipeline detail[%s] failed, error:%v",
				schedule.ID, schedule.PipelineVersionID, result.Error)
			logger.Logger().Errorf(errMsg)
		}
	}

	// 更新全局wakeup时间
	// 1. 如果NextRunAt <= currentTime，证明目前schedule超过concurrency，而且concurrency是suspend，导致调度被阻塞，此时nextRunAt不能被纳入nextWakeupTime
	// 2. 如果status是终止态，nextRunAt也不能被纳入nextWakeupTime
	//	 - 另外如果schedule非终态， 一定满足 schedule.nextRunAt <= schedule.EndAt，所以比较时间不用考虑schedule.EndAt
	if schedule.Status == models.ScheduleStatusRunning && schedule.NextRunAt.After(currentTime) {
		earlierTime := schedule.NextRunAt
		if nextWakeupTime != nil {
			earlierTime = s.getEarlierTime(earlierTime, *nextWakeupTime)
		}
		nextWakeupTime = &earlierTime
	}

	return nextWakeupTime
}

// 处理到时信号，主要分成以下步骤：
// 1. 查询数据库
//  - 获取需要发起的周期调度，更新对应next_run_at
//  - 更新到达end_time的周期调度状态
//  - 获取下一个wakeup时间(如果不存在则是空指针)
// 2. 发起到期的任务
// 3. 休眠
func (s *Scheduler) dealWithTimeout() (nextWakeupTime *time.Time, err error) {
	// todo: 查询时，要添加for update锁，避免多个paddleFlow实例同时调度时，记录被同时update
	currentTime := time.Now()
	schedules, err := models.GetSchedulesByStatus(logger.Logger(), models.ScheduleStatusRunning)
	if err != nil {
		return nil, err
	}

	nextWakeupTime = nil
	for _, schedule := range schedules {
		options, err := models.DecodeScheduleOptions(schedule.Options)
		if err != nil {
			errMsg := fmt.Sprintf("decode options[%s] of schedule of ID[%s] failed. error: %v", schedule.Options, schedule.ID, err)
			logger.Logger().Errorf(errMsg)
			continue
		}

		fsConfig, err := models.DecodeFsConfig(schedule.FsConfig)
		if err != nil {
			logger.Logger().Errorf("decode fsConfig[%s] of schedule[%s] with failed, err:[%s]", schedule.FsConfig, schedule.ID, err.Error())
			continue
		}

		scheduleIDList := []string{schedule.ID}
		activeRuns, err := models.ListRun(logger.Logger(), 0, 0, []string{}, []string{}, []string{}, []string{}, common.RunActiveStatus, scheduleIDList)
		if err != nil {
			errMsg := fmt.Sprintf("get runs to stop for schedule[%s] failed, err: %s", schedule.ID, err.Error())
			logger.Logger().Error(errMsg)
			continue
		}

		// 先处理同时满足currentTime之前，而且schedule.EndAt之前的任务
		activeCount := len(activeRuns)
		expiredList, execList, skipList, nextRunAt, stopCount, err := s.generateRunListForSchedule(schedule, currentTime, activeCount)
		if err != nil {
			continue
		}

		// 为expiredList, skipList, execList发起对应任务
		// 根据stopCount停止activeRuns
		logger.Logger().Infof("before processRunList, expiredList[%v], execList[%v], skipList[%v], activeCount:[%d], stopCount[%d]", expiredList, execList, skipList, activeCount, stopCount)
		s.processRunList(schedule, options, fsConfig, currentTime, expiredList, skipList, execList, stopCount, activeRuns)

		// 更新数据库记录（如果nextRunAt，或者status字段有更新的话），以及更新 nextWakeupTime
		nextWakeupTime = s.updateScheduleAndWakeupTime(schedule, currentTime, nextRunAt, nextWakeupTime)
		logger.Logger().Infof("after updateScheduleAndWakeupTime for schedule[%s], nextWakeupTime[%s]", schedule.ID, s.formatTime(nextWakeupTime))
	}

	return nextWakeupTime, err
}

// 1. 判断要不要重新计算全局timeout（计算耗时，尽量过滤非必需场景）
// - 如果当前schdule状态不是running，不做任何处理
// - 查询当前schedule的并发度，如果当前并发度>=concurrency，不做任何处理
// - 如果 concurrencyPolicy 不是suspend，不用处理
//
// 2. 计算下一个全局timeout时间
// - 计算timeout，如果有 schedule 的 next_run_at 在 expire_interval以外，会直接把 timeout 设置为0
//   - 有过期任务，此处不会更新next_run_at，而是马上触发dealWithTimeout函数处理
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
