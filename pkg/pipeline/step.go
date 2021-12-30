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
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"paddleflow/pkg/common/schema"
)

type Step struct {
	name              string
	wfr               *WorkflowRuntime
	info              *schema.WorkflowSourceStep
	ready             chan bool // 由 run 来决定是否开始执行该步骤
	done              bool      // 表示是否已经运行结束，done==true，则跳过该step
	submitted         bool      // 表示是否从run中发过ready信号过来。如果pipeline服务宕机重启，run会从该字段判断是否需要再次发ready信号，触发运行
	job               Job
	firstFingerprint  string
	secondFingerprint string
}

var NewStep = func(name string, wfr *WorkflowRuntime, info *schema.WorkflowSourceStep) (*Step, error) {
	// 该函数初始化job时，只传入image，并没有替换parameter，command，env中的参数
	// 因为初始化job的操作是在所有step初始化的时候做的，此时step可能被启动一个协程，但并没有真正运行任意一个step的运行逻辑
	// 因此没法知道上游节点的参数值，没法做替换
	st := &Step{
		name:  name,
		wfr:   wfr,
		info:  info,
		ready: make(chan bool, 1),
		done:  false,
	}

	jobName := fmt.Sprintf("%s-%s", st.wfr.wf.RunID, name)
	st.job = NewPaddleFlowJob(jobName, st.info.Image, st.info.Deps)

	st.getLogger().Debugf("before updating job: param[%s], env[%s], command[%s], deps[%s]", st.info.Parameters, st.info.Env, st.info.Command, st.info.Deps)
	err := st.updateJob()
	if err != nil {
		st.getLogger().Error(err.Error())
		return nil, err
	}
	st.getLogger().Debugf("step[%s] of runid[%s] starting job, param[%s], env[%s], command[%s]", st.name, st.wfr.wf.RunID, st.job.(*PaddleFlowJob).Parameters, st.job.(*PaddleFlowJob).Env, st.job.(*PaddleFlowJob).Command)

	err = st.job.Validate()
	if err != nil {
		st.getLogger().Error(err.Error())
		return nil, err
	}

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

func (st *Step) updateJob() error {
	// 替换parameters， command， envs
	// 这个为啥要在这里替换，而不是在runtime初始化的时候呢？因为后续可能支持上游动态模板值。
	steps := map[string]*schema.WorkflowSourceStep{st.name: st.info}
	for i, step := range st.wfr.steps {
		steps[step.name] = st.wfr.steps[i].info
	}
	var sysParams = map[string]string{
		SysParamNamePFRunID:    st.wfr.wf.RunID,
		SysParamNamePFStepName: st.name,
		SysParamNamePFFsID:     st.wfr.wf.Extra[WfExtraInfoKeyFsID],
		SysParamNamePFFsName:   st.wfr.wf.Extra[WfExtraInfoKeyFsName],
		SysParamNamePFUserName: st.wfr.wf.Extra[WfExtraInfoKeyUserName],
	}
	paramSolver := StepParamSolver{steps: steps, sysParams: sysParams, needReplace: true}
	if err := paramSolver.Solve(st.name); err != nil {
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
	st.getLogger().Debugf("step[%s] of runid[%s]: param[%s], command[%s], env[%s]",
		st.name, st.wfr.wf.RunID, params, st.info.Command, newEnvs)
	return nil
}

func (st *Step) getJobExtra(status schema.JobStatus) map[string]interface{} {
	extra := map[string]interface{}{
		"status":    status,
		"preStatus": st.job.(*PaddleFlowJob).Status,
		"jobid":     st.job.(*PaddleFlowJob).Id,
	}

	return extra
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

func (st *Step) checkCached() (bool, error) {
	// 运行前先判断是否使用，以及匹配cache
	cacheCaculator, err := NewCacheCalculator(*st, st.wfr.wf.Source.Cache)
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

	cacheFound := false
	cacheRunID := ""
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

	var logMsg string
	if cacheFound {
		logMsg = fmt.Sprintf("cache found in former runid[%s] for step[%s] of runid[%s], with fingerprint[%s] and [%s]", cacheRunID, st.name, st.wfr.wf.RunID, st.firstFingerprint, st.secondFingerprint)
	} else {
		logMsg = fmt.Sprintf("NO cache found for step[%s] in runid[%s], with fingerprint[%s] and [%s]", st.name, st.wfr.wf.RunID, st.firstFingerprint, st.secondFingerprint)
	}

	st.getLogger().Infof(logMsg)
	return cacheFound, nil
}

// 步骤执行
func (st *Step) Execute() {
	if st.job.Started() {
		if st.job.NotEnded() {
			logMsg := fmt.Sprintf("start to recover job[%s] of step[%s] with runid[%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
			st.getLogger().Infof(logMsg)

			st.wfr.IncConcurrentJobs(1)
			st.Watch()
		}
	} else {
		select {
		case <-st.wfr.ctx.Done():
			logMsg := fmt.Sprintf("context of step[%s] with runid[%s] has stopped with msg:[%s], no need to execute", st.name, st.wfr.wf.RunID, st.wfr.ctx.Err())
			st.getLogger().Infof(logMsg)

			extra := st.getJobExtra(schema.StatusJobCancelled)
			st.job.(*PaddleFlowJob).Status = schema.StatusJobCancelled
			st.done = true
			wfe := NewWorkflowEvent(WfEventJobUpdate, "", extra)
			st.wfr.event <- *wfe
		case <-st.ready:
			logMsg := fmt.Sprintf("start execute step[%s] with runid[%s]", st.name, st.wfr.wf.RunID)
			st.getLogger().Infof(logMsg)

			st.wfr.IncConcurrentJobs(1) // 如果达到并行Job上限，将会Block

			// 有可能在这一步的时候，run已经结束了，此时直接退出，不发起
			if st.wfr.ctx.Err() != nil {
				logMsg := fmt.Sprintf("context of step[%s] with runid[%s] has stopped with msg:[%s], no need to execute", st.name, st.wfr.wf.RunID, st.wfr.ctx.Err())
				st.getLogger().Infof(logMsg)

				st.wfr.DecConcurrentJobs(1)

				extra := st.getJobExtra(schema.StatusJobCancelled)
				st.job.(*PaddleFlowJob).Status = schema.StatusJobCancelled
				st.done = true
				wfe := NewWorkflowEvent(WfEventJobUpdate, "", extra)
				st.wfr.event <- *wfe
				return
			}

			cache := st.wfr.wf.Source.Cache
			if cache.Enable {
				cachedFound, err := st.checkCached()
				if err != nil {
					ErrMsg := fmt.Sprintf("check cache for step[%s] with runid[%s] failed: [%s]", st.name, st.wfr.wf.RunID, err.Error())
					st.getLogger().Errorf(ErrMsg)

					st.wfr.DecConcurrentJobs(1)
					extra := st.getJobExtra(schema.StatusJobFailed)
					st.job.(*PaddleFlowJob).Status = schema.StatusJobFailed
					st.done = true
					wfe := NewWorkflowEvent(WfEventJobSubmitErr, ErrMsg, extra)
					st.wfr.event <- *wfe
					return
				}

				if cachedFound {
					InfoMsg := fmt.Sprintf("skip job for step[%s] with runid[%s], use cache", st.name, st.wfr.wf.RunID)
					st.getLogger().Infof(InfoMsg)

					st.wfr.DecConcurrentJobs(1)
					extra := st.getJobExtra(schema.StatusJobCached)
					st.job.(*PaddleFlowJob).Status = schema.StatusJobCached
					st.done = true
					wfe := NewWorkflowEvent(WfEventJobUpdate, InfoMsg, extra)
					st.wfr.event <- *wfe
					return
				}
			}

			_, err := st.job.Start()
			if err != nil {
				// 异常处理，塞event，不返回error是因为统一通过channel与run沟通
				// todo：要不要改成WfEventJobUpdate的event？
				ErrMsg := fmt.Sprintf("start job for step[%s] with runid[%s] failed: [%s]", st.name, st.wfr.wf.RunID, err.Error())
				st.getLogger().Errorf(ErrMsg)

				extra := st.getJobExtra(schema.StatusJobFailed)
				st.job.(*PaddleFlowJob).Status = schema.StatusJobFailed
				st.done = true
				wfe := NewWorkflowEvent(WfEventJobSubmitErr, ErrMsg, extra)
				st.wfr.event <- *wfe
				return
			}
			st.getLogger().Debugf("step[%s] of runid[%s]: jobID[%s]", st.name, st.wfr.wf.RunID, st.job.(*PaddleFlowJob).Id)

			st.logInputArtifact()
			// watch不需要做异常处理，因为在watch函数里面已经做了
			st.Watch()
		}
	}
}

func (st *Step) stopJob() {
	<-st.wfr.ctx.Done()
	logMsg := fmt.Sprintf("context of job[%s] step[%s] with runid[%s] has stopped in step watch, with msg:[%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, st.wfr.ctx.Err())
	st.getLogger().Infof(logMsg)

	tryCount := 1
	for {
		if st.done {
			logMsg = fmt.Sprintf("job[%s] step[%s] with runid[%s] has finished, no need to stop", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
			st.getLogger().Infof(logMsg)
		}
		// 异常处理, 塞event，不返回error是因为统一通过channel与run沟通
		err := st.job.Stop()
		if err != nil {
			ErrMsg := fmt.Sprintf("stop job[%s] for step[%s] with runid[%s] failed [%d] times: [%s]", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, tryCount, err.Error())
			st.getLogger().Errorf(ErrMsg)
			wfe := NewWorkflowEvent(WfEventJobStopErr, ErrMsg, nil)
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
					if st.wfr.wf.Source.Cache.Enable && extra["status"] == schema.StatusJobSucceeded {
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
							ExpiredTime: st.wfr.wf.Source.Cache.MaxExpiredTime,
							Strategy:    CacheStrategyConservative,
						}

						// logcache失败，不影响job正常结束，但是把cache失败添加日志
						_, err := st.wfr.wf.callbacks.LogCacheCb(req)
						if err != nil {
							ErrMsg := fmt.Sprintf("log cache for job[%s], step[%s] with runid[%s] failed: %s", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID, err.Error())
							st.getLogger().Errorf(ErrMsg)
						} else {
							InfoMsg := fmt.Sprintf("log cache for job[%s], step[%s] with runid[%s] success", st.job.(*PaddleFlowJob).Id, st.name, st.wfr.wf.RunID)
							st.getLogger().Infof(InfoMsg)
						}
					}
					st.done = true
					st.wfr.DecConcurrentJobs(1)
				}
				if extra["status"] == schema.StatusJobSucceeded {
					st.logOutputArtifact()
				}
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
