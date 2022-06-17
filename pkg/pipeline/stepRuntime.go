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
	"context"
	"fmt"
	"strconv"
	"time"

	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type StepRuntime struct {
	*baseComponentRuntime
	submitted         bool // 表示是否从run中发过ready信号过来。如果pipeline服务宕机重启，run会从该字段判断是否需要再次发ready信号，触发运行
	job               Job
	firstFingerprint  string
	secondFingerprint string
	CacheRunID        string
}

func generateJobName(runID, stepName string, seq int) string {
	if seq == 0 {
		return fmt.Sprintf("%s-%s", runID, stepName)
	}

	return fmt.Sprintf("%s-%s-%s", runID, stepName, seq)
}

func NewStepRuntime(fullName string, step *schema.WorkflowSourceStep, seq int, ctx context.Context,
	failureOpitonsCtx context.Context, eventChannel chan<- WorkflowEvent, config *runConfig, ParentDagID string) *StepRuntime {
	cr := NewBaseComponentRuntime(fullName, step, seq, ctx, failureOpitonsCtx, eventChannel, config, ParentDagID)
	srt := &StepRuntime{
		baseComponentRuntime: cr,
	}

	jobName := generateJobName(config.runID, step.GetName(), seq)
	job := NewPaddleFlowJob(jobName, srt.DockerEnv)
	srt.job = job

	srt.updateStatus(StatusRunttimePending)
	srt.logger.Debugf("step[%s] of runid[%s] before starting job: param[%s], env[%s], command[%s], artifacts[%s], deps[%s]",
		srt.getName(), srt.runID, step.Parameters, step.Env, step.Command, step.Artifacts, step.Deps)

	return srt
}

func (srt *StepRuntime) getWorkFlowStep() *schema.WorkflowSourceStep {
	step := srt.getComponent().(*schema.WorkflowSourceStep)
	return step
}

// NewStepRuntimeWithStaus: 在创建Runtime 的同时，指定runtime的状态
// 主要用于重启或者父节点调度子节点的失败时调用， 将相关信息通过evnet 的方式同步给其父节点， 并同步至数据库中
func newStepRuntimeWithStatus(fullName string, step *schema.WorkflowSourceStep, seq int, ctx context.Context,
	failureOpitonsCtx context.Context, eventChannel chan<- WorkflowEvent, config *runConfig,
	ParentDagID string, status RuntimeStatus, msg string) *StepRuntime {
	srt := NewStepRuntime(fullName, step, seq, ctx, failureOpitonsCtx, eventChannel, config, ParentDagID)
	srt.updateStatus(status)

	view := srt.newJobView(msg)
	srt.syncToApiServerAndParent(WfEventJobUpdate, view, msg)
	return srt
}

func (srt *StepRuntime) processStartAbnormalStatus(msg string, status RuntimeStatus) {
	// TODO: 1、更新节点状态， 2、生成 event 将相关信息同步至父节点
	srt.updateStatus(status)

	view := srt.newJobView(msg)

	srt.syncToApiServerAndParent(WfEventJobUpdate, view, msg)
}

func (srt *StepRuntime) Start() {
	// 1、计算 condition
	conditon, err := srt.CalculateCondition()
	if err != nil {
		errMsg := fmt.Sprintf("caculate the condition field for component[%s] faild:\n%s",
			srt.getName(), err.Error())

		srt.logger.Errorln(errMsg)
		srt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
	}

	if conditon {
		skipMsg := fmt.Sprintf("the result of condition for Component [%s] is true, skip running", srt.getName())
		srt.logger.Infoln(skipMsg)
		srt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	// 判断节点是否被 disabled
	if srt.isDisabled() {
		skipMsg := fmt.Sprintf("Component [%s] is disabled, skip running", srt.getName())
		srt.logger.Infoln(skipMsg)
		srt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
	}

	// 监听channel, 及时除了时间
	go srt.Listen()

	srt.Execute()
}

func (srt StepRuntime) Listen() {
	return
}

func (st *StepRuntime) update(done bool, submitted bool, job Job) {
	st.done = done
	st.submitted = submitted
	st.job = job
}

func (srt *StepRuntime) updateJob(forCacheFingerprint bool) error {
	// 替换command, envs, parameter 与 artifact 已经在创建Step前便已经替换完成
	// 这个为啥要在这里替换，而不是在runtime初始化的时候呢？ 计算cache 需要进行的替换和运行时进行的替换有些许不同

	// 替换env
	if err := srt.innerSolver.resolveEnv(forCacheFingerprint); err != nil {
		return err
	}

	// 替换command
	if err := srt.innerSolver.resolveCommand(forCacheFingerprint); err != nil {
		return err
	}

	var params = make(map[string]string)
	for paramName, paramValue := range srt.GetParameters() {
		params[paramName] = fmt.Sprintf("%v", paramValue)
	}

	artifacts := schema.Artifacts{Input: map[string]string{}, Output: map[string]string{}}
	for atfName, atfValue := range srt.GetArtifacts().Input {
		artifacts.Input[atfName] = atfValue
	}
	for atfName, atfValue := range srt.GetArtifacts().Output {
		artifacts.Output[atfName] = atfValue
	}

	// 获取替换后的 env, 替换后，将内置参数也添加到环境变量中
	var newEnvs = make(map[string]string)
	for envName, envVal := range srt.getWorkFlowStep().Env {
		newEnvs[envName] = envVal
	}

	// 对于 cache 相关场景，下面的信息无需添加到环境变量中
	if !forCacheFingerprint {
		sysParams := srt.sysParams
		for integratedParam, integratedParamVal := range sysParams {
			newEnvs[integratedParam] = integratedParamVal
		}

		// artifact 也添加到环境变量中
		for atfName, atfValue := range srt.GetArtifacts().Input {
			newEnvs[GetInputArtifactEnvName(atfName)] = atfValue
		}
		for atfName, atfValue := range srt.GetArtifacts().Output {
			newEnvs[GetOutputArtifactEnvName(atfName)] = atfValue
		}
	}

	srt.job.Update(srt.getWorkFlowStep().Command, params, newEnvs, &artifacts)
	srt.logger.Debugf("step[%s] after resolve template: param[%s], artifacts[%s], command[%s], env[%s]",
		srt.componentFullName, params, artifacts, srt.getWorkFlowStep().Command, newEnvs)
	return nil
}

func (st *StepRuntime) logInputArtifact() {
	for atfName, atfValue := range st.getComponent().GetArtifacts().Input {
		req := schema.LogRunArtifactRequest{
			RunID:        st.runID,
			FsID:         st.fsID,
			FsName:       st.fsName,
			UserName:     st.userName,
			ArtifactPath: atfValue,
			Step:         st.CompoentFullName,
			JobID:        st.job.Job().ID,
			ArtifactName: atfName,
			Type:         schema.ArtifactTypeInput,
		}
		for i := 0; i < 3; i++ {
			st.logger.Infof("callback log input artifact [%+v]s", req)
			if err := st.callbacks.LogArtifactCb(req); err != nil {
				st.logger.Errorf("callback log input artifact [%+v] failed. err:%s", req, err.Error())
				continue
			}
			break
		}
	}
}

func (st *StepRuntime) logOutputArtifact() {
	for atfName, atfValue := range st.component.(*schema.WorkflowSourceStep).Artifacts.Output {
		req := schema.LogRunArtifactRequest{
			RunID:        st.runID,
			FsID:         st.fsID,
			FsName:       st.fsName,
			UserName:     st.userName,
			ArtifactPath: atfValue,
			Step:         st.CompoentFullName,
			JobID:        st.job.Job().ID,
			ArtifactName: atfName,
			Type:         schema.ArtifactTypeOutput,
		}
		for i := 0; i < 3; i++ {
			st.logger.Infof("callback log output artifact [%+v]", req)
			if err := st.callbacks.LogArtifactCb(req); err != nil {
				st.logger.Errorf("callback log out artifact [%+v] failed. err:%s", req, err.Error())
				continue
			}
			break
		}
	}
}

func (srt *StepRuntime) checkCached() (cacheFound bool, err error) {
	/*
		计算cache key，并查看是否存在可用的cache
	*/

	// check Cache前，先替换参数（参数替换逻辑与运行前的参数替换逻辑不一样）
	forCacheFingerprint := true
	err = srt.updateJob(forCacheFingerprint)
	if err != nil {
		return false, err
	}

	err = srt.job.Validate()
	if err != nil {
		return false, err
	}

	cacheCaculator, err := NewCacheCalculator(*srt, srt.Cache)
	if err != nil {
		return false, err
	}

	srt.firstFingerprint, err = cacheCaculator.CalculateFirstFingerprint()
	if err != nil {
		return false, err
	}

	runCacheList, err := srt.callbacks.ListCacheCb(srt.firstFingerprint, srt.fsID, srt.component.GetName(), srt.pplSource)
	if err != nil {
		return false, err
	}
	if len(runCacheList) == 0 {
		// 这里不能直接返回，因为还要计算secondFingerprint，用来在节点运行成功时，记录到数据库
		logMsg := fmt.Sprintf("cache list empty for step[%s] in runid[%s], with first fingerprint[%s]", srt.component.GetName(), srt.runID, srt.firstFingerprint)
		srt.logger.Infof(logMsg)
	} else {
		logMsg := fmt.Sprintf("cache list length(%d) for step[%s] in runid[%s], with first fingerprint[%s]", len(runCacheList), srt.component.GetName(), srt.runID, srt.firstFingerprint)
		srt.logger.Infof(logMsg)
	}

	srt.secondFingerprint, err = cacheCaculator.CalculateSecondFingerprint()
	if err != nil {
		return false, err
	}

	cacheFound = false
	var cacheRunID string
	for _, runCache := range runCacheList {
		if srt.secondFingerprint == runCache.SecondFp {
			if runCache.ExpiredTime == CacheExpiredTimeNever {
				cacheFound = true
				cacheRunID = runCache.RunID
				break
			} else {
				runCacheExpiredTime, _ := strconv.Atoi(runCache.ExpiredTime)
				expiredTime := runCache.UpdatedAt.Add(time.Second * time.Duration(runCacheExpiredTime))
				logMsg := fmt.Sprintf("runCache.SecondFp: %s, runCacheExpiredTime: %d, runCache.UpdatedAt: %s, expiredTime: %s, time.Now(): %s", runCache.SecondFp, runCacheExpiredTime, runCache.UpdatedAt, expiredTime, time.Now())
				srt.logger.Infof(logMsg)
				if time.Now().Before(expiredTime) {
					logMsg := fmt.Sprintf("time.now() before expiredTime")
					srt.logger.Infof(logMsg)
					cacheFound = true
					cacheRunID = runCache.RunID
					break
				} else {
					logMsg := fmt.Sprintf("time.now() after expiredTime")
					srt.logger.Infof(logMsg)
				}
			}
		}
	}

	if cacheFound {
		jobView, err := srt.callbacks.GetJobCb(cacheRunID, srt.component.GetName())
		if err != nil {
			return false, err
		}

		forCacheFingerprint := false
		for name, _ := range srt.GetArtifacts().Output {
			value, ok := jobView.Artifacts.Output[name]
			if !ok {
				err := fmt.Errorf("cannot get the output Artifact[%s] path for component[%s] from cache job[%s] of run[%s]",
					name, srt.componentFullName, jobView.JobID, srt.CacheRunID)
				return false, err
			}

			srt.GetArtifacts().Output[name] = value
		}

		err = srt.updateJob(forCacheFingerprint)
		if err != nil {
			return false, err
		}

		srt.CacheRunID = cacheRunID
		logMsg := fmt.Sprintf("cache found in former runid[%s] for step[%s] of runid[%s], with fingerprint[%s] and [%s]", cacheRunID, srt.component.GetName(), srt.runID, srt.firstFingerprint, srt.secondFingerprint)
		srt.logger.Infof(logMsg)
	} else {
		logMsg := fmt.Sprintf("NO cache found for step[%s] in runid[%s], with fingerprint[%s] and [%s]", srt.component.GetName(), srt.runID, srt.firstFingerprint, srt.secondFingerprint)
		srt.logger.Infof(logMsg)
	}
	return cacheFound, nil
}

func (st *StepRuntime) logCache() error {
	// 写cache记录到数据库
	req := schema.LogRunCacheRequest{
		FirstFp:     st.firstFingerprint,
		SecondFp:    st.secondFingerprint,
		Source:      st.pplSource,
		RunID:       st.runID,
		Step:        st.component.GetName(),
		FsID:        st.fsID,
		FsName:      st.fsName,
		UserName:    st.userName,
		ExpiredTime: st.Cache.MaxExpiredTime,
		Strategy:    CacheStrategyConservative,
	}

	// logcache失败，不影响job正常结束，但是把cache失败添加日志
	_, err := st.callbacks.LogCacheCb(req)
	if err != nil {
		return fmt.Errorf("log cache for job[%s], step[%s] with runid[%s] failed: %s", st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID, err.Error())
	} else {
		InfoMsg := fmt.Sprintf("log cache for job[%s], step[%s] with runid[%s] success", st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID)
		st.logger.Infof(InfoMsg)
		return nil
	}

}

// 运行步骤
func (srt *StepRuntime) Execute() {
	logMsg := fmt.Sprintf("start execute step[%s] with runid[%s]", srt.component.GetName(), srt.runID)
	srt.logger.Infof(logMsg)

	// 1、 查看是否命中cache
	if srt.Cache.Enable {
		cachedFound, err := srt.checkCached()
		if err != nil {
			logMsg = fmt.Sprintf("check cache for step[%s] with runid[%s] failed: [%s]", srt.component.GetName(), srt.runID, err.Error())
			srt.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, logMsg)
			return
		}

		if cachedFound {
			for {
				jobView, err := srt.callbacks.GetJobCb(srt.CacheRunID, srt.component.GetName())
				if err != nil {
					// TODO: 此时是否应该继续运行，创建一个新的Job？
					srt.processStartAbnormalStatus(err.Error(), schema.StatusJobFailed)
					break
				}

				cacheStatus := jobView.Status
				if cacheStatus == schema.StatusJobFailed || cacheStatus == schema.StatusJobSucceeded {
					// 通过讲workflow event传回去，就能够在runtime中callback，将job更新后的参数存到数据库中
					logMsg = fmt.Sprintf("skip job for step[%s] in runid[%s], use cache of runid[%s]",
						srt.component.GetName(), srt.runID, srt.CacheRunID)

					srt.processStartAbnormalStatus(logMsg, cacheStatus)
					return

				} else if cacheStatus == schema.StatusJobInit || cacheStatus == schema.StatusJobPending ||
					cacheStatus == schema.StatusJobRunning || cacheStatus == schema.StatusJobTerminating {
					time.Sleep(time.Second * 3)
				} else {
					// 如果过往job的状态属于其他状态，如 StatusJobTerminated StatusJobCancelled，则无视该cache，继续运行当前job
					// 不过按照正常逻辑，不存在处于Cancelled，但是有cache的job，这里单纯用于兜底

					// 命中cachekey的时候，已经将cacheRunID添加了，但是此时不会利用cache记录，所以要删掉该字段
					srt.CacheRunID = ""
					break
				}
			}
		}

		err = srt.logCache()
		if err != nil {
			srt.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, err.Error())
			return
		}
	}

	// 2、更新outputArtifact 的path
	if len(srt.GetArtifacts().Output) != 0 {
		rh, err := NewResourceHandler(srt.runID, srt.fsID, srt.logger)
		if err != nil {
			logMsg = fmt.Sprintf("cannot generate output artifact's path for step[%s]: %s", srt.name, err.Error())
			srt.logger.Error(logMsg)
			srt.processStartAbnormalStatus(logMsg, StatusRuntimeFailed)
			return
		}

		for artName, _ := range srt.GetArtifacts().Output {
			artPath, err := rh.generateOutAtfPath(srt.runConfig.WorkflowSource.Name, srt.name, artName, true)
			if err != nil {
				logMsg = fmt.Sprintf("cannot generate output artifact[%s] for step[%s] path: %s",
					artName, srt.name, err.Error())
				srt.logger.Error(logMsg)
				srt.processStartAbnormalStatus(logMsg, StatusRuntimeFailed)
				return
			}

			srt.GetArtifacts().Output[artName] = artPath
		}
	}
	// 节点运行前，先替换参数（参数替换逻辑与check Cache的参数替换逻辑不一样，多了一步替换output artifact，并利用output artifact参数替换command以及添加到env）
	forCacheFingerprint := false
	err := srt.updateJob(forCacheFingerprint)
	if err != nil {
		logMsg = fmt.Sprintf("update output artifacts value for step[%s] with runid[%s] failed: [%s]", srt.component.GetName(), srt.runID, err.Error())
		srt.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, logMsg)
		return
	}

	err = srt.job.Validate()
	if err != nil {
		logMsg = fmt.Sprintf("validating step[%s] with runid[%s] failed: [%s]", srt.component.GetName(), srt.runID, err.Error())
		srt.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, logMsg)
		return
	}

	srt.parallelismManager.increase() // 如果达到并行Job上限，将会Block

	// 有可能在跳出block的时候，run已经结束了，此时直接退出，不发起
	if srt.ctx.Err() != nil {
		srt.parallelismManager.decrease()
		logMsg = fmt.Sprintf("context of step[%s] with runid[%s] has stopped with msg:[%s], no need to execute",
			srt.component.GetName(), srt.runID, srt.ctx.Err())
		srt.updateJobStatus(WfEventJobUpdate, schema.StatusJobCancelled, logMsg)
		return
	}

	// todo: 正式运行前，需要将更新后的参数更新到数据库中（通过传递workflow event到runtime即可）
	_, err = srt.job.Start()
	if err != nil {
		// 异常处理，塞event，不返回error是因为统一通过channel与run沟通
		// todo：要不要改成WfEventJobUpdate的event？
		logMsg = fmt.Sprintf("start job for step[%s] with runid[%s] failed: [%s]", srt.component.GetName(), srt.runID, err.Error())
		srt.updateJobStatus(WfEventJobSubmitErr, schema.StatusJobFailed, logMsg)
		return
	}
	srt.logger.Debugf("step[%s] of runid[%s]: jobID[%s]", srt.component.GetName(), srt.runID, srt.job.(*PaddleFlowJob).ID)

	srt.logInputArtifact()
	// watch不需要做异常处理，因为在watch函数里面已经做了
	srt.Watch()
}

func (st *StepRuntime) stopJob() {
	<-st.ctx.Done()

	logMsg := fmt.Sprintf("context of job[%s] step[%s] with runid[%s] has stopped in step watch, with msg:[%s]",
		st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID, st.ctx.Err())
	st.logger.Infof(logMsg)

	tryCount := 1
	for {
		if st.done {
			logMsg = fmt.Sprintf("job[%s] step[%s] with runid[%s] has finished, no need to stop",
				st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID)
			st.logger.Infof(logMsg)
			return
		}
		// 异常处理, 塞event，不返回error是因为统一通过channel与run沟通
		err := st.job.Stop()
		if err != nil {
			ErrMsg := fmt.Sprintf("stop job[%s] for step[%s] with runid[%s] failed [%d] times: [%s]",
				st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID, tryCount, err.Error())
			st.logger.Errorf(ErrMsg)

			// TODO: 这里的信息是否需要进行更改？
			extra := map[string]interface{}{
				"step": st,
			}
			wfe := NewWorkflowEvent(WfEventJobStopErr, ErrMsg, extra)
			st.sendEventToParent <- *wfe

			tryCount += 1
			time.Sleep(time.Second * 3)
		} else {
			return
		}
	}
}

// 步骤监控
func (st *StepRuntime) Watch() {
	// TODO: 在生成事件时需要调用回调函数将相关信息记录至数据库中
	logMsg := fmt.Sprintf("start to watch job[%s] of step[%s] with runid[%s]",
		st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID)
	st.logger.Infof(logMsg)

	ch := make(chan WorkflowEvent, 1)
	go st.job.Watch(ch)
	go st.stopJob()

	for {
		event, ok := <-ch
		if !ok {
			ErrMsg := fmt.Sprintf("watch job[%s] for step[%s] with runid[%s] failed, channel already closed",
				st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID)
			st.logger.Errorf(ErrMsg)
			wfe := NewWorkflowEvent(WfEventJobWatchErr, ErrMsg, nil)
			st.sendEventToParent <- *wfe
		}

		if event.isJobWatchErr() {
			ErrMsg := fmt.Sprintf("receive watch error of job[%s] step[%s] with runid[%s], with errmsg:[%s]", st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID, event.Message)
			st.logger.Errorf(ErrMsg)
		} else {
			extra, ok := event.getJobUpdate()
			if ok {
				logMsg = fmt.Sprintf("receive watch update of job[%s] step[%s] with runid[%s], with errmsg:[%s], extra[%s]", st.job.(*PaddleFlowJob).ID, st.component.GetName(), st.runID, event.Message, event.Extra)
				st.logger.Infof(logMsg)
				if extra["status"] == schema.StatusJobSucceeded || extra["status"] == schema.StatusJobFailed || extra["status"] == schema.StatusJobTerminated {
					st.done = true
					st.parallelismManager.decrease()
				}
				if extra["status"] == schema.StatusJobSucceeded {
					st.logOutputArtifact()
				}
				event.Extra["step"] = st
			}
		}
		st.sendEventToParent <- event
		if st.done {
			return
		}
	}
}

func (srt *StepRuntime) newJobView(msg string) schema.JobView {
	return schema.JobView{}

	step := srt.getComponent().(*schema.WorkflowSourceStep)
	params := map[string]string{}
	for name, value := range step.GetParameters() {
		params[name] = fmt.Sprintf("%v", value)
	}

	job := srt.job.Job()

	view := schema.JobView{
		JobID:       job.ID,
		JobName:     job.Name,
		Command:     job.Command,
		Parameters:  params,
		Env:         job.Env,
		StartTime:   job.StartTime,
		EndTime:     job.EndTime,
		Status:      srt.status,
		Deps:        step.Deps,
		DockerEnv:   step.DockerEnv,
		JobMessage:  msg,
		ParentDagID: srt.parentDagID,
	}

	return view
}
