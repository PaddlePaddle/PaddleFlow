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
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
)

var workflowCallbacks = pipeline.WorkflowCallbacks{
	GetJobCb:      GetJobFunc,
	UpdateRunCb:   UpdateRunFunc,
	LogCacheCb:    LogCacheFunc,
	ListCacheCb:   ListCacheFunc,
	LogArtifactCb: LogArtifactFunc,
}

var (
	GetJobFunc      func(runID string, stepName string) (schema.JobView, error)         = GetJobByRun
	UpdateRunFunc   func(id string, event interface{}) bool                             = UpdateRunByWfEvent
	LogCacheFunc    func(req schema.LogRunCacheRequest) (string, error)                 = LogCache
	ListCacheFunc   func(firstFp, fsID, step, source string) ([]models.RunCache, error) = ListCacheByFirstFp
	LogArtifactFunc func(req schema.LogRunArtifactRequest) error                        = LogArtifactEvent
)

func GetJobByRun(runID string, stepName string) (schema.JobView, error) {
	logging := logger.LoggerForRun(runID)
	var jobView schema.JobView

	run, err := models.GetRunByID(logging, runID)
	if err != nil {
		logging.Errorf("get Run by runID[%s] failed. Error: %v", runID, err)
		return jobView, err
	}

	if jobViewRuntime, ok := run.Runtime[stepName]; ok {
		return jobViewRuntime, nil
	}
	if jobViewPostProcess, ok := run.PostProcess[stepName]; ok {
		return jobViewPostProcess, nil
	}

	errMsg := fmt.Sprintf("get jobView from Run with stepName[%s] failed.", stepName)
	logging.Errorf(errMsg)
	return jobView, fmt.Errorf(errMsg)
}

func UpdateRunByWfEvent(id string, event interface{}) bool {
	logging := logger.LoggerForRun(id)
	wfEvent, ok := event.(*pipeline.WorkflowEvent)
	if !ok {
		logging.Errorf("event type-casting failed for run[%s]", id)
		return false
	}
	if wfEvent.Event != pipeline.WfEventRunUpdate {
		logging.Errorf("event type[%s] invalid for run[%s] callback", pipeline.WfEventRunUpdate, id)
		return false
	}
	runID := wfEvent.Extra[common.WfEventKeyRunID].(string)
	if id != runID {
		logging.Errorf("event id[%s] mismatch with runID[%s]", id, runID)
		return false
	}
	status := wfEvent.Extra[common.WfEventKeyStatus].(string)
	runtime, ok := wfEvent.Extra[common.WfEventKeyRuntime].(schema.RuntimeView)
	if !ok {
		logging.Errorf("run[%s] malformat runtime", id)
		return false
	}
	postProcess, ok := wfEvent.Extra[common.WfEventKeyPostProcess].(schema.PostProcessView)
	if !ok {
		logging.Errorf("run[%s] malformat post process", id)
		return false
	}

	if err := updateRunCache(logging, runtime, runID); err != nil {
		return false
	}

	logging.Debugf("workflow event update run[%s] status:%s message:%s, runtime:%v, post_porcess:%v",
		id, status, wfEvent.Message, runtime, postProcess)
	prevRun, err := models.GetRunByID(logging, runID)
	if err != nil {
		logging.Errorf("get run[%s] in db failed. error: %v", id, err)
		return false
	}
	message := wfEvent.Message
	if prevRun.Message != "" {
		logging.Infof("skip run message:[%s], only keep the first message for run", message)
		message = ""
	}
	runtimeJobs := map[string]schema.JobView{}
	for name, job := range runtime {
		runtimeJobs[name] = job
	}
	for name, job := range postProcess {
		runtimeJobs[name] = job
	}
	activatedAt := sql.NullTime{}
	if prevRun.Status == common.StatusRunPending {
		activatedAt.Time = time.Now()
		activatedAt.Valid = true
		// 创建run_job记录
		if err := models.CreateRunJobs(logging, runtimeJobs, id); err != nil {
			return false
		}
	}

	if err := updateRunJobs(id, runtimeJobs); err != nil {
		logging.Errorf("run[%s] update run_job failed. error: %v", id, err)
		return false
	}

	updateRun := models.Run{
		Status:      status,
		Message:     message,
		ActivatedAt: activatedAt,
	}
	if err := models.UpdateRun(logging, runID, updateRun); err != nil {
		logging.Errorf("update run[%s] in db failed. error: %v", id, err)
		return false
	}

	if common.IsRunFinalStatus(status) {
		logging.Debugf("run[%s] has reached final status[%s]", runID, status)
		delete(wfMap, runID)

		// 给scheduler发concurrency channel信号
		if prevRun.ScheduleID != "" {
			globalScheduler := GetGlobalScheduler()
			globalScheduler.ConcurrencyChannel <- prevRun.ScheduleID
			logging.Debugf("send scheduleID[%s] to concurrency channel succeed.", prevRun.ScheduleID)
		}
	}
	return true
}

func updateRunCache(logging *logrus.Entry, runtime schema.RuntimeView, runID string) error {
	// 检查每个job的cache情况
	// 多个job很可能cache同一个Run，所以用set来去重
	cacheIdSet := mapset.NewSet()
	for _, jobView := range runtime {
		if jobView.CacheRunID != "" {
			cacheIdSet.Add(jobView.CacheRunID)
		}
	}

	// 一次性读取全部 Run，避免多次调用GetRunByID
	cacheIdList := make([]string, 0, cacheIdSet.Cardinality())
	for cacheId := range cacheIdSet.Iter() {
		cacheIdList = append(cacheIdList, cacheId.(string))
	}
	runCachedList := make([]models.Run, 0)
	if len(cacheIdList) > 0 {
		var err error
		runCachedList, err = models.ListRun(logging, 0, 0, nil, nil, cacheIdList, nil, nil, nil)
		if err != nil {
			logging.Errorf("update cacheIDs failed. Get runs[%v] failed. error: %v", cacheIdList, err)
			return err
		}
	}
	logging.Debugf("number of run cached by updating run is [%v]", len(runCachedList))
	for _, runCached := range runCachedList {
		// 检查这个当前run要cache的某个run，之前被哪些run已经cache了
		runCacheIDList := runCached.GetRunCacheIDList()
		newRun := true
		for _, runCacheID := range runCacheIDList {
			if runCacheID == runID {
				// 如果之前被cache过的run已经包含了当前run，就不用添加当前run的id了
				newRun = false
				break
			}
		}
		if newRun {
			runCacheIDList = append(runCacheIDList, runID)
			newRunCacheIDs := strings.Join(runCacheIDList, common.SeparatorComma)
			models.UpdateRun(logging, runCached.ID, models.Run{RunCachedIDs: newRunCacheIDs})
		}
	}
	return nil
}

func updateRunJobs(runID string, jobs map[string]schema.JobView) error {
	logging := logger.Logger()
	for name, job := range jobs {
		runJob := models.ParseRunJob(&job)
		runJob.Encode()
		if err := models.UpdateRunJob(logging, runID, name, runJob); err != nil {
			return err
		}
	}
	return nil
}

func handleImageCallbackFunc(imageInfo handler.ImageInfo, err error) error {
	runID := imageInfo.RunID
	logEntry := logger.LoggerForRun(runID)
	logEntry.Debugf("image handler cb with imageInfo: %+v\n", imageInfo)
	// handle image failed. update db
	if err != nil {
		logEntry.Debugf("image handler cb to inform handle failure. err: %v\n", err)
		updateErr := updateRunStatusAndMsg(runID, common.StatusRunFailed, err.Error())
		return updateErr
	}
	// start workflow with image url
	imageUrl := imageInfo.Url
	if imageUrl == "" {
		logEntry.Debugf("image handler cb - retrieving image[%s] url from db", imageInfo.PFImageID)
		imageUrl, err = models.GetUrlByPFImageID(logEntry, imageInfo.PFImageID)
		if err != nil {
			logEntry.Errorf("GetUrlByImageID[%s] in db failed. error: %v",
				imageInfo.PFImageID, err)
			updateRunStatusAndMsg(runID, common.StatusRunFailed, err.Error())
			return err
		}
	}
	logEntry.Debugf("image handler cb startWfWithImageUrl[%s]\n", imageUrl)
	startWfWithImageUrl(runID, imageUrl)
	if imageInfo.UrlUpdated {
		image := models.Image{
			ID:      imageInfo.PFImageID,
			ImageID: imageInfo.ImageID,
			FsID:    imageInfo.FsID,
			Source:  imageInfo.Source,
			Url:     imageUrl,
		}
		_, err := models.GetImage(logEntry, imageInfo.PFImageID)
		if err != nil {
			if errors.GetErrorCode(err) == errors.ErrorRecordNotFound {
				// image not in db. save image info to db
				logEntry.Debugf("image handler cb store new image[%s] with url[%s]\n", imageInfo.PFImageID, imageUrl)
				if err := models.CreateImage(logEntry, &image); err != nil {
					logEntry.Errorf("CreateImage[%s] with url[%s] in db failed. error: %v",
						imageInfo.PFImageID, imageUrl, err)
				}
			} else {
				logEntry.Errorf("image handler cb get image[%s] from db failed. err: %v", imageInfo.PFImageID, err)
			}
		} else {
			// image in db, update it
			logEntry.Debugf("image handler cb update image[%s] url[%s]\n", imageInfo.PFImageID, imageUrl)
			if err := models.UpdateImage(logEntry, imageInfo.PFImageID, image); err != nil {
				logEntry.Errorf("updateImage[%s] with url[%s] in db failed. error: %v",
					imageInfo.PFImageID, imageUrl, err)
			}
		}
	}
	return nil
}

func updateRunStatusAndMsg(id, status, msg string) error {
	updateRun := models.Run{
		Status:  status,
		Message: msg,
	}
	if err := models.UpdateRun(logger.LoggerForRun(id), id, updateRun); err != nil {
		logger.LoggerForRun(id).Errorf("update with status[%s] in db failed. error: %v", status, err)
		return err
	}
	return nil
}

func startWfWithImageUrl(runID, imageUrl string) error {
	logEntry := logger.LoggerForRun(runID)
	logEntry.Debugf("start workflow with image url[%s]\n", imageUrl)
	// retrieve run
	run, err := models.GetRunByID(logEntry, runID)
	if err != nil {
		logEntry.Debugf("startWfWithImageUrl failed retrieving run. err:%v\n", err)
		return updateRunStatusAndMsg(runID, common.StatusRunFailed, err.Error())
	}
	// patch WorkflowSource from RunYaml
	wfs, err := schema.ParseWorkflowSource([]byte(run.RunYaml))
	if err != nil {
		logEntry.Errorf("get WorkflowSource by yaml failed. yaml: %s \n, err:%v", run.RunYaml, err)
		return err
	}
	// replace DockerEnv
	wfs.DockerEnv = imageUrl
	run.WorkflowSource = wfs
	// init workflow and start
	wfPtr, err := newWorkflowByRun(run)
	if err != nil {
		logEntry.Debugf("validateAndInitWorkflow failed. err:%v\n", err)
		return updateRunStatusAndMsg(runID, common.StatusRunFailed, err.Error())
	}
	// start workflow with image url
	wfPtr.Start()
	logEntry.Debugf("workflow started after image handling. run: %+v", run)
	// update run's imageUrl
	return models.UpdateRun(logger.LoggerForRun(run.ID), run.ID,
		models.Run{DockerEnv: run.WorkflowSource.DockerEnv, Status: common.StatusRunPending})
}
