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
	"strings"
	"sync"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

type StepRuntime struct {
	*baseComponentRuntime
	job               Job
	firstFingerprint  string
	secondFingerprint string
	CacheRunID        string
	CacheJobID        string

	// 需要避免在终止的同时在 创建 job 的情况，导致数据不一致
	processJobLock sync.Mutex
}

func generateJobName(runID, stepName string, seq int) string {
	if seq == 0 {
		return fmt.Sprintf("%s-%s", runID, stepName)
	}
	return fmt.Sprintf("%s-%s-%d", runID, stepName, seq)
}

func NewStepRuntime(name, fullName string, step *schema.WorkflowSourceStep, seq int, ctx context.Context,
	failureOpitonsCtx context.Context, eventChannel chan<- WorkflowEvent, config *runConfig, ParentDagID string) *StepRuntime {
	cr := NewBaseComponentRuntime(name, fullName, step, seq, ctx, failureOpitonsCtx, eventChannel, config, ParentDagID)
	srt := &StepRuntime{
		baseComponentRuntime: cr,
	}

	jobName := generateJobName(config.runID, step.GetName(), seq)
	job := NewPaddleFlowJob(jobName, srt.getWorkFlowStep().DockerEnv, srt.userName, srt.receiveEventChildren,
		srt.runConfig.mainFS, srt.getWorkFlowStep().ExtraFS)
	srt.job = job

	srt.logger.Infof("step[%s] of runid[%s] before starting job: param[%s], env[%s], command[%s], artifacts[%s], deps[%s], "+
		"extraFS[%v]", srt.getName(), srt.runID, step.Parameters, step.Env, step.Command,
		step.Artifacts, step.Deps, step.ExtraFS)

	return srt
}

// NewStepRuntimeWithStaus: 在创建Runtime 的同时，指定runtime的状态
// 主要用于重启或者父节点调度子节点的失败时调用， 将相关信息通过evnet 的方式同步给其父节点， 并同步至数据库中
func newStepRuntimeWithStatus(name, fullName string, step *schema.WorkflowSourceStep, seq int, ctx context.Context,
	failureOpitonsCtx context.Context, eventChannel chan<- WorkflowEvent, config *runConfig,
	ParentDagID string, status RuntimeStatus, msg string) *StepRuntime {
	srt := NewStepRuntime(name, fullName, step, seq, ctx, failureOpitonsCtx, eventChannel, config, ParentDagID)

	// 此时由于 stepRuntime 并不会运行，不会占坑，所以不需要降低并发度
	err := srt.baseComponentRuntime.updateStatus(status)
	if err != nil {
		config.logger.Errorf(err.Error())
	}

	view := srt.newJobView(msg)

	srt.syncToApiServerAndParent(WfEventJobUpdate, &view, msg)

	return srt
}

func (srt *StepRuntime) getWorkFlowStep() *schema.WorkflowSourceStep {
	step := srt.getComponent().(*schema.WorkflowSourceStep)
	return step
}

func (srt *StepRuntime) catchPanic() {
	if r := recover(); r != nil {
		msg := fmt.Sprintf("Inner Error for stepRuntime %s: %v", srt.name, r)
		trace_logger.KeyWithUpdate(srt.runID).Errorf(msg)
		srt.stopWithMsg(msg)

		srt.ctx.Done()
	}
}

func (srt *StepRuntime) updateStatus(status RuntimeStatus) error {
	err := srt.baseComponentRuntime.updateStatus(status)
	if err != nil {
		// 目前，只有在处于 终态时再次更新状态时才会报err
		srt.logger.Error(err.Error())
		return err
	}

	if srt.done {
		srt.parallelismManager.decrease()
		srt.logger.Infof("step[%s] has finished, and current parallelism is %d", srt.name,
			srt.parallelismManager.CurrentParallelism())
	}

	if srt.status == schema.StatusJobSucceeded {
		srt.logOutputArtifact()
	}

	return nil
}

func (srt *StepRuntime) processStartAbnormalStatus(msg string, status RuntimeStatus) {
	// 1、更新节点状态，(更新失败，则说明已经处于终态，此时直接打log 就好) 2、生成 event 将相关信息同步至父节点
	err := srt.updateStatus(status)
	if err != nil {
		srt.logger.Errorf(err.Error())
	}

	view := srt.newJobView(msg)

	srt.syncToApiServerAndParent(WfEventJobUpdate, &view, msg)
}

func (srt *StepRuntime) Start() {
	// 如果达到并行Job上限，将会Block
	defer srt.processJobLock.Unlock()
	srt.processJobLock.Lock()

	srt.parallelismManager.increase()
	defer srt.catchPanic()

	if srt.ctx.Err() != nil || srt.failureOpitonsCtx.Err() != nil {
		srt.logger.Infof("receive stop signal, step[%s] would't start", srt.name)
		return
	}

	// TODO: 此时是否需要同步至数据库？
	srt.logger.Infof("begin to run step[%s], and current parallelism is %d", srt.name,
		srt.parallelismManager.CurrentParallelism())

	err := srt.setSysParams()
	if err != nil {
		errMsg := fmt.Sprintf("set the sysparams for dag[%s] failed: %s", srt.name, err.Error())
		srt.logger.Errorln(errMsg)
		srt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	// 1、计算 condition
	conditon, err := srt.CalculateCondition()
	if err != nil {
		errMsg := fmt.Sprintf("caculate the condition field for step[%s] faild:\n%s",
			srt.getName(), err.Error())

		srt.logger.Errorf(errMsg)
		srt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	if !conditon {
		skipMsg := fmt.Sprintf("the result of condition for step[%s] is false, skip running", srt.getName())
		srt.logger.Infoln(skipMsg)
		srt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	// 判断节点是否被 disabled
	if srt.isDisabled() {
		skipMsg := fmt.Sprintf("step[%s] is disabled, skip running", srt.getName())
		srt.logger.Infoln(skipMsg)
		srt.processStartAbnormalStatus(skipMsg, StatusRuntimeSkipped)
		return
	}

	// 监听channel, 及时除了时间
	go srt.Listen()
	go srt.Stop()
	srt.Execute()
}

// Restart: 根据 jobView 来重启step
// 如果 jobView 中的状态为 Succeeded, 则直接返回，无需重启
// 如果 jobView 中的状态为 Running, 则进入监听即可
// 否则 创建一个新的job并开始调度执行
func (srt *StepRuntime) Restart(view *schema.JobView) {
	srt.logger.Infof("begin to restart step[%s]", srt.name)
	defer srt.processJobLock.Unlock()
	srt.processJobLock.Lock()

	srt.pk = view.PK

	// 这条支线只有当前节点为 postProcess 节点才会走
	go srt.Start()
}

func (srt *StepRuntime) Resume(view *schema.JobView) {
	defer srt.processJobLock.Unlock()
	srt.processJobLock.Lock()

	// 如果jobID 为空，说明此时还没有发起job， 因此重新Start
	if view.JobID == "" {
		go srt.Start()
		return
	}

	srt.parallelismManager.increase()

	defer srt.catchPanic()

	srt.job = NewPaddleFlowJobWithJobView(view, srt.getWorkFlowStep().DockerEnv,
		srt.receiveEventChildren, srt.runConfig.mainFS, srt.getWorkFlowStep().ExtraFS)

	srt.pk = view.PK
	err := srt.updateStatus(view.Status)
	if err != nil {
		errMsg := fmt.Sprintf("set the sysparams for dag[%s] failed: %s", srt.name, err.Error())
		srt.logger.Errorln(errMsg)
		srt.processStartAbnormalStatus(errMsg, StatusRuntimeFailed)
		return
	}

	err = srt.setSysParams()
	if err != nil {
		srt.logger.Errorln(err.Error())
		srt.processStartAbnormalStatus(err.Error(), StatusRuntimeFailed)
		return
	}

	for name, value := range view.Artifacts.Output {
		srt.getComponent().GetArtifacts().Output[name] = value
	}

	err = srt.updateJob(false)
	if err != nil {
		err := fmt.Errorf("update job for step[%s] failed: %s", srt.name, err.Error())
		srt.processStartAbnormalStatus(err.Error(), StatusRuntimeFailed)
		return
	}

	srt.logger.Infof("Watch Job [%s] again", srt.job.JobID())

	msg := fmt.Sprintf("resume step[%s] with status[%s]", srt.name, string(srt.status))
	newView := srt.newJobView(msg)
	srt.syncToApiServerAndParent(WfEventJobUpdate, &newView, msg)

	go srt.Listen()
	go srt.Stop()
	go srt.job.Watch()
	return
}

func (srt *StepRuntime) Listen() {
	for {
		event := <-srt.receiveEventChildren
		if srt.done {
			return
		}
		srt.processEventFromJob(event)
	}
}

func (srt *StepRuntime) Stop() {
	select {
	case <-srt.ctx.Done():
		if srt.done {
			return
		}

		defer srt.processJobLock.Unlock()
		srt.processJobLock.Lock()
		srt.stopWithMsg("receive stop signall")
	case <-srt.failureOpitonsCtx.Done():
		if srt.done {
			return
		}

		defer srt.processJobLock.Unlock()
		srt.processJobLock.Lock()
		srt.stopWithMsg("stop by failureOptions, some other component has been failed")
	}
}

func (srt *StepRuntime) updateJob(forCacheFingerprint bool) error {
	// 替换command, envs, parameter 与 artifact 已经在创建Step前便已经替换完成
	// 这个为啥要在这里替换，而不是在runtime初始化的时候呢？ 计算cache 需要进行的替换和运行时进行的替换有些许不同

	// 替换env
	if err := srt.innerSolver.resolveEnv(); err != nil {
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

	// 在进行 job 校验时，需要这部分参数
	sysParams := srt.sysParams
	for integratedParam, integratedParamVal := range sysParams {
		newEnvs[integratedParam] = integratedParamVal
	}

	// 对于 cache 相关场景，下面的信息无需添加到环境变量中
	if !forCacheFingerprint {
		// artifact 也添加到环境变量中
		for atfName, atfValue := range srt.GetArtifacts().Input {
			newEnvs[GetInputArtifactEnvName(atfName)] = GetArtifactMountPath(srt.runConfig.mainFS, atfValue)
		}

		for atfName, atfValue := range srt.GetArtifacts().Output {
			newEnvs[GetOutputArtifactEnvName(atfName)] = GetArtifactMountPath(srt.runConfig.mainFS, atfValue)
		}
	}

	srt.job.Update(srt.getWorkFlowStep().Command, params, newEnvs, &artifacts)
	srt.logger.Infof("step[%s] after resolve template: param[%s], artifacts[%s], command[%s], env[%s]， FsMount[%v]",
		srt.name, params, artifacts, srt.getWorkFlowStep().Command, newEnvs, srt.getWorkFlowStep().ExtraFS)
	return nil
}

func (srt *StepRuntime) logInputArtifact() {
	for atfName, atfValue := range srt.getComponent().GetArtifacts().Input {
		req := schema.LogRunArtifactRequest{
			RunID:        srt.runID,
			FsID:         srt.runConfig.mainFS.ID,
			FsName:       srt.runConfig.mainFS.Name,
			UserName:     srt.userName,
			ArtifactPath: atfValue,
			Step:         srt.getWorkFlowStep().Name,
			JobID:        srt.job.Job().ID,
			ArtifactName: atfName,
			Type:         schema.ArtifactTypeInput,
		}
		for i := 0; i < 3; i++ {
			srt.logger.Infof("callback log input artifact [%+v]s", req)
			if err := srt.callbacks.LogArtifactCb(req); err != nil {
				srt.logger.Errorf("callback log input artifact [%+v] failed. err:%s", req, err.Error())
				continue
			}
			break
		}
	}
}

func (srt *StepRuntime) logOutputArtifact() {
	for atfName, atfValue := range srt.component.(*schema.WorkflowSourceStep).Artifacts.Output {
		req := schema.LogRunArtifactRequest{
			RunID:        srt.runID,
			FsID:         srt.runConfig.mainFS.ID,
			FsName:       srt.runConfig.mainFS.Name,
			UserName:     srt.userName,
			ArtifactPath: atfValue,
			Step:         srt.getWorkFlowStep().Name,
			JobID:        srt.job.Job().ID,
			ArtifactName: atfName,
			Type:         schema.ArtifactTypeOutput,
		}
		for i := 0; i < 3; i++ {
			srt.logger.Infof("callback log output artifact [%+v]", req)
			if err := srt.callbacks.LogArtifactCb(req); err != nil {
				srt.logger.Errorf("callback log out artifact [%+v] failed. err:%s", req, err.Error())
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

	job := srt.job.(*PaddleFlowJob)
	cacheCaculator, err := NewCacheCalculator(*job, srt.getWorkFlowStep().Cache, srt.logger, srt.runConfig.mainFS,
		srt.getWorkFlowStep().ExtraFS)
	if err != nil {
		return false, err
	}

	srt.firstFingerprint, err = cacheCaculator.CalculateFirstFingerprint()
	if err != nil {
		return false, err
	}

	runCacheList, err := srt.callbacks.ListCacheCb(srt.firstFingerprint, srt.runConfig.mainFS.ID, srt.pplSource)
	if err != nil {
		return false, err
	}
	if len(runCacheList) == 0 {
		// 这里不能直接返回，因为还要计算secondFingerprint，用来在节点运行成功时，记录到数据库
		logMsg := fmt.Sprintf("cache list empty for step[%s] in runid[%s], with first fingerprint[%s]",
			srt.name, srt.runID, srt.firstFingerprint)
		srt.logger.Infof(logMsg)
	} else {
		logMsg := fmt.Sprintf("cache list length(%d) for step[%s] in runid[%s], with first fingerprint[%s]",
			len(runCacheList), srt.name, srt.runID, srt.firstFingerprint)
		srt.logger.Infof(logMsg)
	}

	srt.secondFingerprint, err = cacheCaculator.CalculateSecondFingerprint()
	if err != nil {
		return false, err
	}

	cacheFound = false
	var cacheRunID string
	var cacheJobID string
	for _, runCache := range runCacheList {
		if srt.secondFingerprint == runCache.SecondFp {
			if runCache.ExpiredTime == CacheExpiredTimeNever {
				cacheFound = true
				cacheRunID = runCache.RunID
				cacheJobID = runCache.JobID
				break
			} else {
				runCacheExpiredTime, _ := strconv.Atoi(runCache.ExpiredTime)
				expiredTime := runCache.UpdatedAt.Add(time.Second * time.Duration(runCacheExpiredTime))
				logMsg := fmt.Sprintf("runCache.SecondFp: %s, runCacheExpiredTime: %d, runCache.UpdatedAt: %s, expiredTime: %s, "+
					"time.Now(): %s", runCache.SecondFp, runCacheExpiredTime, runCache.UpdatedAt, expiredTime, time.Now())
				srt.logger.Infof(logMsg)
				if time.Now().Before(expiredTime) {
					logMsg := fmt.Sprintf("time.now() before expiredTime")
					srt.logger.Infof(logMsg)
					cacheFound = true
					cacheRunID = runCache.RunID
					cacheJobID = runCache.JobID
					break
				} else {
					logMsg := fmt.Sprintf("time.now() after expiredTime")
					srt.logger.Infof(logMsg)
				}
			}
		}
	}

	if cacheFound {
		jobView, err := srt.callbacks.GetJobCb(cacheJobID)
		srt.logger.Infof("the jobView for cache is: %v", jobView)
		if err != nil {
			return false, err
		}

		srt.logger.Infof("the jobView for cache is: %v", jobView)

		forCacheFingerprint := false
		for name, _ := range srt.GetArtifacts().Output {
			value, ok := jobView.Artifacts.Output[name]
			if !ok {
				err := fmt.Errorf("cannot get the output Artifact[%s] path for step[%s] from cache job[%s] of run[%s]",
					name, srt.name, jobView.JobID, srt.CacheRunID)
				return false, err
			}

			srt.GetArtifacts().Output[name] = value
		}

		err = srt.updateJob(forCacheFingerprint)
		if err != nil {
			return false, err
		}

		srt.CacheRunID = cacheRunID
		srt.CacheJobID = cacheJobID
		logMsg := fmt.Sprintf("cache found in former runid[%s] for step[%s] of runid[%s], with fingerprint[%s] and [%s]",
			cacheRunID, srt.name, srt.runID, srt.firstFingerprint, srt.secondFingerprint)
		srt.logger.Infof(logMsg)
	} else {
		logMsg := fmt.Sprintf("NO cache found for step[%s] in runid[%s], with fingerprint[%s] and [%s]",
			srt.name, srt.runID, srt.firstFingerprint, srt.secondFingerprint)
		srt.logger.Infof(logMsg)
	}
	return cacheFound, nil
}

func (srt *StepRuntime) logCache() error {
	// 写cache记录到数据库
	req := schema.LogRunCacheRequest{
		FirstFp:     srt.firstFingerprint,
		SecondFp:    srt.secondFingerprint,
		Source:      srt.pplSource,
		RunID:       srt.runID,
		Step:        srt.getComponent().GetName(),
		JobID:       srt.job.Job().ID,
		FsID:        srt.runConfig.mainFS.ID,
		FsName:      srt.runConfig.mainFS.Name,
		UserName:    srt.userName,
		ExpiredTime: srt.getWorkFlowStep().Cache.MaxExpiredTime,
		Strategy:    CacheStrategyConservative,
	}

	// logcache失败，不影响job正常结束，但是把cache失败添加日志
	_, err := srt.callbacks.LogCacheCb(req)
	if err != nil {
		return fmt.Errorf("log cache for job[%s], step[%s] with runid[%s] failed: %s",
			srt.job.(*PaddleFlowJob).ID, srt.name, srt.runID, err.Error())
	} else {
		InfoMsg := fmt.Sprintf("log cache for job[%s], step[%s] with runid[%s] success",
			srt.job.(*PaddleFlowJob).ID, srt.name, srt.runID)
		srt.logger.Infof(InfoMsg)
		return nil
	}

}

func (srt *StepRuntime) generateOutArtPathOnFs() (err error) {
	rh, err := NewResourceHandler(srt.runID, srt.runConfig.mainFS.ID, srt.logger)
	if err != nil {
		err = fmt.Errorf("cannot generate output artifact's path for step[%s]: %s", srt.name, err.Error())
		return err
	}

	for artName, _ := range srt.GetArtifacts().Output {
		artPath, err := rh.GenerateOutAtfPath(srt.runConfig.WorkflowSource.Name, srt.mainFS.SubPath,
			srt.getComponent().GetName(), srt.name, srt.loopSeq, artName, true)
		if err != nil {
			err = fmt.Errorf("cannot generate output artifact[%s] for step[%s] path: %s",
				artName, srt.name, err.Error())

			return err
		}
		srt.GetArtifacts().Output[artName] = artPath
	}
	return
}

func (srt *StepRuntime) generateOutArtPathForJob(paths string) string {
	pathsForJob := []string{}

	for _, path := range strings.Split(paths, ",") {
		path = strings.TrimSpace(path)
		pathsForJob = append(pathsForJob, strings.Join([]string{ArtMountDir, path}, "/"))
	}
	return strings.Join(pathsForJob, ",")
}

func (srt *StepRuntime) startJob() (err error) {
	err = nil

	if srt.status != "" {
		// 此时说明其余的协程正在处理或者已经处理完当前的运行时，因此直接退出当前协程
		err = fmt.Errorf("inner error: cannot restart step[%s], because it's already in status[%s]",
			srt.name, srt.status)
		return err
	}

	if srt.ctx.Err() != nil || srt.failureOpitonsCtx.Err() != nil {
		srt.logger.Infof("received  terminal or failureOpitons signal, wouldn't start job")
		return
	}

	// TODO: 正式运行前，需要将更新后的参数更新到数据库中（通过传递workflow event到runtime即可）
	_, err = srt.job.Start()
	if err != nil {
		err = fmt.Errorf("start job for step[%s] with runid[%s] failed: [%s]", srt.name, srt.runID, err.Error())
		return err
	}

	if srt.getWorkFlowStep().Cache.Enable {
		err = srt.logCache()
		if err != nil {
			// 如果 cache 信息存储失败，只打印日志，不做额外处理
			srt.logger.Errorf(err.Error())
		}
	}

	return
}

// 运行步骤
func (srt *StepRuntime) Execute() {
	logMsg := fmt.Sprintf("start execute step[%s] with runid[%s]", srt.name, srt.runID)

	// use closure to get latest log
	defer func() {
		if logMsg != "" {
			trace_logger.KeyWithUpdate(srt.runID).Errorf(logMsg)
		}
	}()
	srt.logger.Infof(logMsg)
	logMsg = ""
	// 1、 查看是否命中cache
	if srt.getWorkFlowStep().Cache.Enable {
		cachedFound, err := srt.checkCached()
		if err != nil {
			logMsg = fmt.Sprintf("check cache for step[%s] with runid[%s] failed: [%s]",
				srt.name, srt.runID, err.Error())
			srt.logger.Errorf(logMsg)
			srt.processStartAbnormalStatus(logMsg, schema.StatusJobFailed)
			return
		}

		if cachedFound {
			for {
				jobView, err := srt.callbacks.GetJobCb(srt.CacheJobID)
				if err != nil {
					// TODO: 此时是否应该继续运行，创建一个新的Job？
					srt.logger.Errorf("get cache job info for step[%s] failed: %s", srt.name, err.Error())
					srt.processStartAbnormalStatus(err.Error(), schema.StatusJobFailed)
					break
				}

				cacheStatus := jobView.Status
				// TODO: 区分由于服务原因导致之前的job 失败的情况？
				if cacheStatus == schema.StatusJobFailed || cacheStatus == schema.StatusJobSucceeded {
					// 通过讲workflow event传回去，就能够在runtime中callback，将job更新后的参数存到数据库中
					logMsg = fmt.Sprintf("skip job for step[%s] in runid[%s], use cache of runid[%s]",
						srt.name, srt.runID, srt.CacheRunID)
					srt.logger.Infoln(logMsg)
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
	}

	// 2、更新outputArtifact 的path
	if len(srt.GetArtifacts().Output) != 0 {
		err := srt.generateOutArtPathOnFs()
		if err != nil {
			logMsg = err.Error()
			srt.logger.Error(logMsg)
			srt.processStartAbnormalStatus(err.Error(), StatusRuntimeFailed)
			return
		}
	}

	// 节点运行前，先替换参数（参数替换逻辑与check Cache的参数替换逻辑不一样，多了一步替换output artifact，并利用output artifact参数替换command以及添加到env）
	forCacheFingerprint := false
	err := srt.updateJob(forCacheFingerprint)
	if err != nil {
		logMsg = fmt.Sprintf("update output artifacts value for step[%s] in runid[%s] failed: [%s]",
			srt.name, srt.runID, err.Error())
		srt.logger.Errorf(logMsg)
		srt.processStartAbnormalStatus(logMsg, schema.StatusJobFailed)
		return
	}

	err = srt.job.Validate()
	if err != nil {
		logMsg = fmt.Sprintf("validating step[%s] failed: [%s]", srt.name, err.Error())
		srt.logger.Errorf(logMsg)
		srt.processStartAbnormalStatus(logMsg, schema.StatusJobFailed)
		return
	}

	// todo: 正式运行前，需要将更新后的参数更新到数据库中（通过传递workflow event到runtime即可）
	err = srt.startJob()
	if err != nil {
		// 异常处理，塞event，不返回error是因为统一通过channel与run沟通
		logMsg = err.Error()
		srt.logger.Errorf(logMsg)
		srt.processStartAbnormalStatus(err.Error(), schema.StatusJobFailed)
		return
	}

	srt.logger.Infof("step[%s] of runid[%s]: jobID[%s]", srt.name, srt.runID, srt.job.(*PaddleFlowJob).ID)

	srt.logInputArtifact()
}

func (srt *StepRuntime) stopWithMsg(msg string) {
	if srt.job.JobID() == "" {
		// 此时说明还没有创建job，因此直接将状态置为 failed，并通过事件进行同步即可
		var msg string
		err := srt.updateStatus(StatusRuntimeFailed)
		if err != nil {
			msg = err.Error()
		} else {
			msg = fmt.Sprintf("cannot stop stepruntime[%s] because cannot find it's jobid", srt.name)
		}

		view := srt.newJobView(msg)
		srt.syncToApiServerAndParent(WfEventJobStopErr, &view, msg)
		return
	}

	logMsg := fmt.Sprintf("begin to stop step[%s] with msg: %s", srt.name, msg)
	srt.logger.Infof(logMsg)

	tryCount := 1
	for {
		if srt.done {
			logMsg = fmt.Sprintf("job[%s] step[%s] with runid[%s] has finished, no need to stop",
				srt.job.(*PaddleFlowJob).ID, srt.name, srt.runID)
			srt.logger.Infof(logMsg)
			return
		}
		// 异常处理, 塞event，不返回error是因为统一通过channel与run沟通
		err := srt.job.Stop()
		if err != nil {
			ErrMsg := fmt.Sprintf("stop job[%s] for step[%s] with runid[%s] failed [%d] times: [%s]",
				srt.job.(*PaddleFlowJob).ID, srt.component.GetName(), srt.runID, tryCount, err.Error())
			srt.logger.Errorf(ErrMsg)

			view := srt.newJobView(ErrMsg)
			srt.syncToApiServerAndParent(WfEventJobStopErr, &view, ErrMsg)

			tryCount += 1
			time.Sleep(time.Second * 3)
		} else {
			return
		}
	}
}

// 步骤监控
func (srt *StepRuntime) processEventFromJob(event WorkflowEvent) {
	logMsg := fmt.Sprintf("receive event from job[%s] of step[%s]: \n%v",
		srt.job.(*PaddleFlowJob).ID, srt.name, event)
	srt.logger.Infof(logMsg)

	if event.isJobWatchErr() {
		ErrMsg := fmt.Sprintf("receive watch error of job[%s] for step[%s] with errmsg:[%s]",
			srt.job.(*PaddleFlowJob).ID, srt.name, event.Message)
		srt.logger.Errorf(ErrMsg)

		// 对于 WatchErr, 目前不需要传递父节点
		view := srt.newJobView(ErrMsg)
		extra := map[string]interface{}{
			common.WfEventKeyRunID:  srt.runID,
			common.WfEventKeyStatus: srt.status,
			common.WfEventKeyView:   view,
		}
		event.Extra = extra
		srt.callbacks.UpdateRuntimeCb(srt.runID, event)
	} else {
		extra, ok := event.getJobUpdate()
		if ok {
			logMsg = fmt.Sprintf("receive watch update of job[%s] step[%s] with runid[%s], with errmsg:[%s], extra[%s]",
				srt.job.(*PaddleFlowJob).ID, srt.name, srt.runID, event.Message, event.Extra)
			srt.logger.Infof(logMsg)
		}

		err := srt.updateStatus(extra["status"].(RuntimeStatus))
		if err != nil {
			srt.logger.Errorf(err.Error())
		}
		view := srt.newJobView(event.Message)
		srt.syncToApiServerAndParent(WfEventJobUpdate, &view, event.Message)
	}
}

func (srt *StepRuntime) newJobView(msg string) schema.JobView {
	step := srt.getWorkFlowStep()
	params := map[string]string{}
	for name, value := range step.GetParameters() {
		params[name] = fmt.Sprintf("%v", value)
	}

	job := srt.job.Job()

	art := srt.getWorkFlowStep().GetArtifacts()
	newArt := (&art).DeepCopy()

	view := schema.JobView{
		JobID:       job.ID,
		Name:        job.Name,
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
		CacheRunID:  srt.CacheRunID,
		CacheJobID:  srt.CacheJobID,
		StepName:    srt.getComponent().GetName(),
		Cache:       srt.getWorkFlowStep().Cache,
		PK:          srt.pk,
		LoopSeq:     srt.loopSeq,
		Artifacts:   *newArt,
		ExtraFS:     srt.getWorkFlowStep().ExtraFS,
	}

	return view
}
