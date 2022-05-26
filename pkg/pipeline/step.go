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
	. "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

type Step struct {
	name              string
	wfr               *WorkflowRuntime
	info              *schema.WorkflowSourceStep
	disabled          bool
	ready             chan bool // 由 run 来决定是否开始执行该步骤
	cancel            chan bool // 如果上游节点运行失败，本节点将不会运行
	done              bool      // 表示是否已经运行结束，done==true，则跳过该step
	submitted         bool      // 表示是否从run中发过ready信号过来。如果pipeline服务宕机重启，run会从该字段判断是否需要再次发ready信号，触发运行
	executed          bool      // 表示是否已经被调用了 Excuted 函数
	job               Job
	firstFingerprint  string
	secondFingerprint string
	CacheRunID        string
	nodeType          NodeType // 用于表示step 是在 entryPoints 中定义还是在 post_process 中定义
}

var NewStep = func(name string, wfr *WorkflowRuntime, info *schema.WorkflowSourceStep,
	disabled bool, nodeType NodeType) (*Step, error) {
	// 该函数初始化job时，只传入image, deps等，并没有替换parameter，command，env中的参数
	// 因为初始化job的操作是在所有step初始化的时候做的，此时step可能被启动一个协程，但并没有真正运行任意一个step的运行逻辑
	// 因此没法知道上游节点的参数值，没法做替换
	jobName := fmt.Sprintf("%s-%s", wfr.wf.RunID, name)
	job := NewPaddleFlowJob(jobName, info.DockerEnv, info.Deps)

	st := &Step{
		name:      name,
		wfr:       wfr,
		info:      info,
		disabled:  disabled,
		ready:     make(chan bool, 1),
		cancel:    make(chan bool, 1),
		done:      false,
		executed:  false,
		submitted: false,
		job:       job,
		nodeType:  nodeType,
	}

	st.getLogger().Debugf("step[%s] of runid[%s] before starting job: param[%s], env[%s], command[%s], artifacts[%s], deps[%s]", st.name, st.wfr.wf.RunID, st.info.Parameters, st.info.Env, st.info.Command, st.info.Artifacts, st.info.Deps)
	return st, nil
}

func (st *Step) update(done bool, submitted bool, job Job) {
	st.done = done
	st.submitted = submitted
	st.job = job
}

func (st *Step) getLogger() *logrus.Entry {
	return st.wfr.wf.log()
}

func (st *Step) generateStepParamSolver(forCacheFingerprint bool) (*StepParamSolver, error) {
	SourceSteps := make(map[string]*schema.WorkflowSourceStep)
	jobs := make(map[string]Job)

	var steps map[string]*Step
	if st.nodeType == NodeTypeEntrypoint {
		steps = st.wfr.entryPoints
	} else {
		steps = st.wfr.postProcess
	}

	for _, step := range steps {
		SourceSteps[step.name] = steps[step.name].info
		jobs[step.name] = steps[step.name].job
	}

	pfRuntimeGen := NewPFRuntimeGenerator(st.wfr.runtimeView, st.wfr.wf.Source)
	runtimeView, err := pfRuntimeGen.GetPFRuntime(st.name)

	if err != nil {
		st.getLogger().Errorf("marshal runtimeView of run[%s] for step[%s] failed: %v", st.wfr.wf.RunID, st.name, runtimeView)
		return nil, err
	}
	var sysParams = map[string]string{
		SysParamNamePFRunID:    st.wfr.wf.RunID,
		SysParamNamePFStepName: st.name,
		SysParamNamePFFsID:     st.wfr.wf.Extra[WfExtraInfoKeyFsID],
		SysParamNamePFFsName:   st.wfr.wf.Extra[WfExtraInfoKeyFsName],
		SysParamNamePFUserName: st.wfr.wf.Extra[WfExtraInfoKeyUserName],
		SysParamNamePFRuntime:  runtimeView,
	}

	paramSolver := NewStepParamSolver(SourceSteps, sysParams, jobs, forCacheFingerprint, st.wfr.wf.Source.Name, st.wfr.wf.RunID, st.wfr.wf.Extra[WfExtraInfoKeyFsID], st.getLogger())
	return &paramSolver, nil
}

func (st *Step) updateJob(forCacheFingerprint bool, cacheOutputArtifacts map[string]string) error {
	// 替换parameters， command， envs
	// 这个为啥要在这里替换，而不是在runtime初始化的时候呢？因为后续可能支持上游动态模板值。
	paramSolver, err := st.generateStepParamSolver(forCacheFingerprint)
	if err != nil {
		return err
	}

	if err := paramSolver.Solve(st.name, cacheOutputArtifacts); err != nil {
		return err
	}

	var params = make(map[string]string)
	for paramName, paramValue := range st.info.Parameters {
		params[paramName] = fmt.Sprintf("%v", paramValue)
	}

	artifacts := schema.Artifacts{Input: map[string]string{}, Output: map[string]string{}}
	for atfName, atfValue := range st.info.Artifacts.Input {
		artifacts.Input[atfName] = atfValue
	}
	for atfName, atfValue := range st.info.Artifacts.Output {
		artifacts.Output[atfName] = atfValue
	}

	// 获取替换后的 env, 替换后，将内置参数也添加到环境变量中
	var newEnvs = make(map[string]string)
	for envName, envVal := range st.info.Env {
		newEnvs[envName] = envVal
	}
	sysParams := paramSolver.getSysParams()
	for integratedParam, integratedParamVal := range sysParams {
		newEnvs[integratedParam] = integratedParamVal
	}
	// artifact 也添加到环境变量中
	for atfName, atfValue := range st.info.Artifacts.Input {
		newEnvs[GetInputArtifactEnvName(atfName)] = atfValue
	}
	for atfName, atfValue := range st.info.Artifacts.Output {
		newEnvs[GetOutputArtifactEnvName(atfName)] = atfValue
	}

	st.job.Update(st.info.Command, params, newEnvs, &artifacts)
	st.getLogger().Debugf("step[%s] of runid[%s]: param[%s], artifacts[%s], command[%s], env[%s]",
		st.name, st.wfr.wf.RunID, params, st.info.Artifacts, st.info.Command, newEnvs)
	return nil
}

func (st *Step) logInputArtifact() {
	for atfName, atfValue := range st.info.Artifacts.Input {
		req := schema.LogRunArtifactRequest{
			RunID:        st.wfr.wf.RunID,
			FsID:         st.wfr.wf.Extra[WfExtraInfoKeyFsID],
			FsName:       st.wfr.wf.Extra[WfExtraInfoKeyFsName],
			UserName:     st.wfr.wf.Extra[WfExtraInfoKeyUserName],
			ArtifactPath: atfValue,
			Step:         st.name,
			ArtifactName: atfName,
			Type:         schema.ArtifactTypeInput,
		}
		for i := 0; i < 3; i++ {
			st.getLogger().Infof("callback log input artifact [%+v]s", req)
			if err := st.wfr.wf.callbacks.LogArtifactCb(req); err != nil {
				st.getLogger().Errorf("callback log input artifact [%+v] failed. err:%s", req, err.Error())
				continue
			}
			break
		}
	}
}

func (st *Step) logOutputArtifact() {
	for atfName, atfValue := range st.info.Artifacts.Output {
		req := schema.LogRunArtifactRequest{
			RunID:        st.wfr.wf.RunID,
			FsID:         st.wfr.wf.Extra[WfExtraInfoKeyFsID],
			FsName:       st.wfr.wf.Extra[WfExtraInfoKeyFsName],
			UserName:     st.wfr.wf.Extra[WfExtraInfoKeyUserName],
			ArtifactPath: atfValue,
			Step:         st.name,
			ArtifactName: atfName,
			Type:         schema.ArtifactTypeOutput,
		}
		for i := 0; i < 3; i++ {
			st.getLogger().Infof("callback log output artifact [%+v]", req)
			if err := st.wfr.wf.callbacks.LogArtifactCb(req); err != nil {
				st.getLogger().Errorf("callback log out artifact [%+v] failed. err:%s", req, err.Error())
				continue
			}
			break
		}
	}
}

func (st *Step) checkCached() (cacheFound bool, err error) {
	/*
		计算cache key，并查看是否存在可用的cache
	*/

	// check Cache前，先替换参数（参数替换逻辑与运行前的参数替换逻辑不一样）
	forCacheFingerprint := true
	err = st.updateJob(forCacheFingerprint, nil)
	if err != nil {
		return false, err
	}

	err = st.job.Validate()
	if err != nil {
		return false, err
	}

	cacheCaculator, err := NewCacheCalculator(*st, st.info.Cache)
	if err != nil {
		return false, err
	}

	st.firstFingerprint, err = cacheCaculator.CalculateFirstFingerprint()
	if err != nil {
		return false, err
	}

	runCacheList, err := st.wfr.wf.callbacks.ListCacheCb(st.firstFingerprint, st.wfr.wf.Extra[WfExtraInfoKeyFsID], st.name, st.wfr.wf.Extra[WfExtraInfoKeySource])
	if err != nil {
		return false, err
	}
	if len(runCacheList) == 0 {
		// 这里不能直接返回，因为还要计算secondFingerprint，用来在节点运行成功时，记录到数据库
		logMsg := fmt.Sprintf("cache list empty for step[%s] in runid[%s], with first fingerprint[%s]", st.name, st.wfr.wf.RunID, st.firstFingerprint)
		st.getLogger().Infof(logMsg)
	} else {
		logMsg := fmt.Sprintf("cache list length(%d) for step[%s] in runid[%s], with first fingerprint[%s]", len(runCacheList), st.name, st.wfr.wf.RunID, st.firstFingerprint)
		st.getLogger().Infof(logMsg)
	}

	st.secondFingerprint, err = cacheCaculator.CalculateSecondFingerprint()
	if err != nil {
		return false, err
	}

	cacheFound = false
	var cacheRunID string
	for _, runCache := range runCacheList {
		if st.secondFingerprint == runCache.SecondFp {
			if runCache.ExpiredTime == CacheExpiredTimeNever {
				cacheFound = true
				cacheRunID = runCache.RunID
				break
			} else {
				runCacheExpiredTime, _ := strconv.Atoi(runCache.ExpiredTime)
				expiredTime := runCache.UpdatedAt.Add(time.Second * time.Duration(runCacheExpiredTime))
				logMsg := fmt.Sprintf("runCache.SecondFp: %s, runCacheExpiredTime: %d, runCache.UpdatedAt: %s, expiredTime: %s, time.Now(): %s", runCache.SecondFp, runCacheExpiredTime, runCache.UpdatedAt, expiredTime, time.Now())
				st.getLogger().Infof(logMsg)
				if time.Now().Before(expiredTime) {
					logMsg := fmt.Sprintf("time.now() before expiredTime")
					st.getLogger().Infof(logMsg)
					cacheFound = true
					cacheRunID = runCache.RunID
					break
				} else {
					logMsg := fmt.Sprintf("time.now() after expiredTime")
					st.getLogger().Infof(logMsg)
				}
			}
		}
	}

	if cacheFound {
		jobView, err := st.wfr.wf.callbacks.GetJobCb(cacheRunID, st.name)
		if err != nil {
			return false, err
		}

		forCacheFingerprint := false
		err = st.updateJob(forCacheFingerprint, jobView.Artifacts.Output)
		if err != nil {
			return false, err
		}

		st.CacheRunID = cacheRunID
		logMsg := fmt.Sprintf("cache found in former runid[%s] for step[%s] of runid[%s], with fingerprint[%s] and [%s]", cacheRunID, st.name, st.wfr.wf.RunID, st.firstFingerprint, st.secondFingerprint)
		st.getLogger().Infof(logMsg)
	} else {
		logMsg := fmt.Sprintf("NO cache found for step[%s] in runid[%s], with fingerprint[%s] and [%s]", st.name, st.wfr.wf.RunID, st.firstFingerprint, st.secondFingerprint)
		st.getLogger().Infof(logMsg)
	}
	return cacheFound, nil
}

func (st *Step) logCache() error {
	// 写cache记录到数据库
	req := schema.LogRunCacheRequest{
		FirstFp:     st.firstFingerprint,
		SecondFp:    st.secondFingerprint,
		Source:      st.wfr.wf.Extra[WfExtraInfoKeySource],
		RunID:       st.wfr.wf.RunID,
		Step:        st.name,
		FsID:        st.wfr.wf.Extra[WfExtraInfoKeyFsID],
		FsName:      st.wfr.wf.Extra[WfExtraInfoKeyFsName],
		UserName:    st.wfr.wf.Extra[WfExtraInfoKeyUserName],
		ExpiredTime: st.info.Cache.MaxExpiredTime,
		Strategy:    CacheStrategyConservative,
	}

	// logcache失败，不影响job正常结束，但是把cache失败添加日志
	_, err := st.wfr.wf.callbacks.LogCacheCb(req)
	if err != nil {
		return fmt.Errorf("log cache for job[%s], step[%s] with runid[%s] failed: %s", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, err.Error())
	} else {
		InfoMsg := fmt.Sprintf("log cache for job[%s], step[%s] with runid[%s] success", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
		st.getLogger().Infof(InfoMsg)
		return nil
	}

}

func (st *Step) updateJobStatus(eventValue WfEventValue, jobStatus schema.JobStatus, logMsg string) {
	if jobStatus == schema.StatusJobFailed {
		st.getLogger().Errorf(logMsg)
	} else {
		st.getLogger().Infof(logMsg)
	}

	extra := map[string]interface{}{
		"preStatus": st.job.(*PaddleFlowJob).Status,
		"status":    jobStatus,
		"jobid":     st.job.(*PaddleFlowJob).Id,
		"step":      st,
	}

	st.job.(*PaddleFlowJob).Message = logMsg
	st.job.(*PaddleFlowJob).Status = jobStatus
	if jobStatus == schema.StatusJobCancelled || jobStatus == schema.StatusJobFailed || jobStatus == schema.StatusJobSucceeded || jobStatus == schema.StatusJobSkipped {
		st.done = true
	}

	wfe := NewWorkflowEvent(eventValue, logMsg, extra)
	st.wfr.event <- *wfe
}

// 步骤执行
func (st *Step) Execute() {
	if st.executed {
		return
	} else {
		st.executed = true
	}

	if st.job.Started() {
		if st.job.NotEnded() {
			logMsg := fmt.Sprintf("start to recover job[%s] of step[%s] with runid[%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
			st.getLogger().Infof(logMsg)

			st.wfr.IncConcurrentJobs(1)
			st.Watch()
		}
	} else {
		var ctx context.Context
		if st.nodeType == NodeTypeEntrypoint {
			ctx = st.wfr.entryPointsCtx
		} else if st.nodeType == NodeTypePostProcess {
			ctx = st.wfr.postProcessPointsCtx
		} else {
			logMsg := fmt.Sprintf("inner error: cannot get NodeType of  step[%s] with runid[%s]", st.name, st.wfr.wf.RunID)
			st.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, logMsg)
		}
		select {
		case <-ctx.Done():
			logMsg := fmt.Sprintf("context of step[%s] with runid[%s] has stopped with msg:[%s], no need to execute", st.name, st.wfr.wf.RunID, ctx.Err())
			st.updateJobStatus(WfEventJobUpdate, schema.StatusJobCancelled, logMsg)
		case <-st.cancel:
			logMsg := fmt.Sprintf("step[%s] with runid[%s] has cancelled due to receiving cancellation signal, maybe upstream step failed", st.name, st.wfr.wf.RunID)
			st.updateJobStatus(WfEventJobUpdate, schema.StatusJobCancelled, logMsg)
		case <-st.ready:
			st.processReady()
		}
	}
}

func (st *Step) processReady() {
	logMsg := fmt.Sprintf("start execute step[%s] with runid[%s]", st.name, st.wfr.wf.RunID)
	st.getLogger().Infof(logMsg)

	if st.disabled {
		logMsg = fmt.Sprintf("step[%s] with runid[%s] is disabled, skip running", st.name, st.wfr.wf.RunID)
		st.updateJobStatus(WfEventJobUpdate, schema.StatusJobSkipped, logMsg)
		return
	}

	if st.info.Cache.Enable {
		cachedFound, err := st.checkCached()
		if err != nil {
			logMsg = fmt.Sprintf("check cache for step[%s] with runid[%s] failed: [%s]", st.name, st.wfr.wf.RunID, err.Error())
			st.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, logMsg)
			return
		}

		if cachedFound {
			for {
				jobView, err := st.wfr.wf.callbacks.GetJobCb(st.CacheRunID, st.name)
				if err != nil {
					st.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, err.Error())
					break
				}

				cacheStatus := jobView.Status
				if cacheStatus == schema.StatusJobFailed || cacheStatus == schema.StatusJobSucceeded {
					// 通过讲workflow event传回去，就能够在runtime中callback，将job更新后的参数存到数据库中
					logMsg = fmt.Sprintf("skip job for step[%s] in runid[%s], use cache of runid[%s]", st.name, st.wfr.wf.RunID, st.CacheRunID)
					st.updateJobStatus(WfEventJobUpdate, cacheStatus, logMsg)
					return
				} else if cacheStatus == schema.StatusJobInit || cacheStatus == schema.StatusJobPending || cacheStatus == schema.StatusJobRunning || cacheStatus == schema.StatusJobTerminating {
					time.Sleep(time.Second * 3)
				} else {
					// 如果过往job的状态属于其他状态，如 StatusJobTerminated StatusJobCancelled，则无视该cache，继续运行当前job
					// 不过按照正常逻辑，不存在处于Cancelled，但是有cache的job，这里单纯用于兜底

					// 命中cachekey的时候，已经将cacheRunID添加了，但是此时不会利用cache记录，所以要删掉该字段
					st.CacheRunID = ""
					break
				}
			}
		}

		err = st.logCache()
		if err != nil {
			st.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, err.Error())
			return
		}
	}

	// 节点运行前，先替换参数（参数替换逻辑与check Cache的参数替换逻辑不一样，多了一步替换output artifact，并利用output artifact参数替换command以及添加到env）
	forCacheFingerprint := false
	err := st.updateJob(forCacheFingerprint, nil)
	if err != nil {
		logMsg = fmt.Sprintf("update output artifacts value for step[%s] with runid[%s] failed: [%s]", st.name, st.wfr.wf.RunID, err.Error())
		st.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, logMsg)
		return
	}

	err = st.job.Validate()
	if err != nil {
		logMsg = fmt.Sprintf("validating step[%s] with runid[%s] failed: [%s]", st.name, st.wfr.wf.RunID, err.Error())
		st.updateJobStatus(WfEventJobUpdate, schema.StatusJobFailed, logMsg)
		return
	}

	st.wfr.IncConcurrentJobs(1) // 如果达到并行Job上限，将会Block

	// 有可能在跳出block的时候，run已经结束了，此时直接退出，不发起
	var ctx context.Context
	if st.nodeType == NodeTypeEntrypoint {
		ctx = st.wfr.entryPointsCtx
	} else if st.nodeType == NodeTypePostProcess {
		ctx = st.wfr.postProcessPointsCtx
	}

	if ctx.Err() != nil {
		st.wfr.DecConcurrentJobs(1)
		logMsg = fmt.Sprintf("context of step[%s] with runid[%s] has stopped with msg:[%s], no need to execute", st.name, st.wfr.wf.RunID, ctx.Err())
		st.updateJobStatus(WfEventJobUpdate, schema.StatusJobCancelled, logMsg)
		return
	}

	// todo: 正式运行前，需要将更新后的参数更新到数据库中（通过传递workflow event到runtime即可）
	_, err = st.job.Start()
	if err != nil {
		// 异常处理，塞event，不返回error是因为统一通过channel与run沟通
		// todo：要不要改成WfEventJobUpdate的event？
		logMsg = fmt.Sprintf("start job for step[%s] with runid[%s] failed: [%s]", st.name, st.wfr.wf.RunID, err.Error())
		st.updateJobStatus(WfEventJobSubmitErr, schema.StatusJobFailed, logMsg)
		return
	}
	st.getLogger().Debugf("step[%s] of runid[%s]: jobID[%s]", st.name, st.wfr.wf.RunID, st.job.(*PaddleFlowJob).Id)

	st.logInputArtifact()
	// watch不需要做异常处理，因为在watch函数里面已经做了
	st.Watch()
}

func (st *Step) stopJob() {
	var ctx context.Context
	if st.nodeType == NodeTypeEntrypoint {
		ctx = st.wfr.entryPointsCtx
	} else if st.nodeType == NodeTypePostProcess {
		ctx = st.wfr.postProcessPointsCtx
	}

	<-ctx.Done()

	logMsg := fmt.Sprintf("context of job[%s] step[%s] with runid[%s] has stopped in step watch, with msg:[%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, ctx.Err())
	st.getLogger().Infof(logMsg)

	tryCount := 1
	for {
		if st.done {
			logMsg = fmt.Sprintf("job[%s] step[%s] with runid[%s] has finished, no need to stop", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
			st.getLogger().Infof(logMsg)
			return
		}
		// 异常处理, 塞event，不返回error是因为统一通过channel与run沟通
		err := st.job.Stop()
		if err != nil {
			ErrMsg := fmt.Sprintf("stop job[%s] for step[%s] with runid[%s] failed [%d] times: [%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, tryCount, err.Error())
			st.getLogger().Errorf(ErrMsg)

			extra := map[string]interface{}{
				"step": st,
			}
			wfe := NewWorkflowEvent(WfEventJobStopErr, ErrMsg, extra)
			st.wfr.event <- *wfe

			tryCount += 1
			time.Sleep(time.Second * 3)
		} else {
			return
		}
	}
}

// 步骤监控
func (st *Step) Watch() {
	logMsg := fmt.Sprintf("start to watch job[%s] of step[%s] with runid[%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
	st.getLogger().Infof(logMsg)

	ch := make(chan WorkflowEvent, 1)
	go st.job.Watch(ch)
	go st.stopJob()

	for {
		event, ok := <-ch
		if !ok {
			ErrMsg := fmt.Sprintf("watch job[%s] for step[%s] with runid[%s] failed, channel already closed", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
			st.getLogger().Errorf(ErrMsg)
			wfe := NewWorkflowEvent(WfEventJobWatchErr, ErrMsg, nil)
			st.wfr.event <- *wfe
		}

		if event.isJobWatchErr() {
			ErrMsg := fmt.Sprintf("receive watch error of job[%s] step[%s] with runid[%s], with errmsg:[%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, event.Message)
			st.getLogger().Errorf(ErrMsg)
		} else {
			extra, ok := event.getJobUpdate()
			if ok {
				logMsg = fmt.Sprintf("receive watch update of job[%s] step[%s] with runid[%s], with errmsg:[%s], extra[%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, event.Message, event.Extra)
				st.getLogger().Infof(logMsg)
				if extra["status"] == schema.StatusJobSucceeded || extra["status"] == schema.StatusJobFailed || extra["status"] == schema.StatusJobTerminated {
					st.done = true
					st.wfr.DecConcurrentJobs(1)
				}
				if extra["status"] == schema.StatusJobSucceeded {
					st.logOutputArtifact()
				}
				event.Extra["step"] = st
			}
		}
		st.wfr.event <- event
		if st.done {
			return
		}
	}
}

func GetInputArtifactEnvName(atfName string) string {
	return "PF_INPUT_ARTIFACT_" + strings.ToUpper(atfName)
}

func GetOutputArtifactEnvName(atfName string) string {
	return "PF_OUTPUT_ARTIFACT_" + strings.ToUpper(atfName)
}
