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
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

var workflowCallbacks = pipeline.WorkflowCallbacks{
	GetJobCb:        GetJobFunc,
	UpdateRuntimeCb: UpdateRuntimeFunc,
	LogCacheCb:      LogCacheFunc,
	ListCacheCb:     ListCacheFunc,
	LogArtifactCb:   LogArtifactFunc,
}

var (
	GetJobFunc        func(jobID string) (schema.JobView, error)                    = GetJobByRun
	UpdateRuntimeFunc func(id string, event interface{}) (int64, bool)              = UpdateRuntimeByWfEvent
	LogCacheFunc      func(req schema.LogRunCacheRequest) (string, error)           = LogCache
	ListCacheFunc     func(firstFp, fsID, source string) ([]models.RunCache, error) = ListCacheByFirstFp
	LogArtifactFunc   func(req schema.LogRunArtifactRequest) error                  = LogArtifactEvent
)

func GetJobByRun(jobID string) (schema.JobView, error) {
	logging := logger.Logger()
	job, err := models.GetRunJob(logging, jobID)
	if err != nil {
		logging.Errorf("get run_job failed in get job cb, err: %v", err)
		return schema.JobView{}, err
	}
	mockStep := schema.WorkflowSourceStep{
		Deps: "",
	}
	resJob := job.ParseJobView(&mockStep)
	return resJob, nil
}

func UpdateRuntimeByWfEvent(id string, event interface{}) (int64, bool) {
	// TODO: 根据 event.enventType 字段判断更新的 View 类型（Job， DAG， Run）
	// 如果没有传递 id 为空字符串，则说明此时对应的数据在 数据库中没有记录，需要新创建一条记录，否则更新相关记录就行
	logging := logger.LoggerForRun(id)
	wfEvent, ok := event.(*pipeline.WorkflowEvent)
	if !ok {
		logging.Errorf("event type-casting failed for run[%s]", id)
		return 0, false
	}
	switch wfEvent.Event {
	case pipeline.WfEventRunUpdate:
		return UpdateRunByWfEvent(id, event)
	case pipeline.WfEventDagUpdate:
		return UpdateRuntimeDagByWfEvent(id, event)
	case pipeline.WfEventJobUpdate:
		return UpdateRuntimeJobByWfEvent(id, event)
	default:
		logging.Errorf("event type invalid in cb")
		return 0, false
	}
}

func UpdateRunByWfEvent(id string, event interface{}) (int64, bool) {
	logging := logger.LoggerForRun(id)
	wfEvent, ok := event.(*pipeline.WorkflowEvent)
	if !ok {
		logging.Errorf("event type-casting failed for run[%s]", id)
		return 0, false
	}
	if wfEvent.Event != pipeline.WfEventRunUpdate {
		logging.Errorf("event type[%s] invalid for run[%s] callback", pipeline.WfEventRunUpdate, id)
		return 0, false
	}
	runID, ok := wfEvent.Extra[common.WfEventKeyRunID].(string)
	if !ok || id != runID {
		logging.Errorf("event id[%s] mismatch with runID[%s]", id, runID)
		return 0, false
	}
	status, ok := wfEvent.Extra[common.WfEventKeyStatus].(string)
	if !ok {
		logging.Errorf("get run status from extra in callback failed")
		return 0, false
	}
	if common.IsRunFinalStatus(status) {
		logging.Debugf("run[%s] has reached final status[%s]", runID, status)
		delete(wfMap, runID)
	}
	startTime, ok := wfEvent.Extra[common.WfEventKeyStartTime].(string)
	if !ok {
		logging.Errorf("get run startTime from extra in callback failed")
		return 0, false
	}

	prevRun, err := models.GetRunByID(logging, runID)
	if err != nil {
		logging.Errorf("get run[%s] in db failed. error: %v", id, err)
		return 0, false
	}

	message := wfEvent.Message
	if prevRun.Message != "" {
		logging.Infof("skip run message:[%s], only keep the first message for run", message)
		message = ""
	}

	activatedAt := sql.NullTime{}
	if startTime != "" {
		activatedAt.Time, err = time.ParseInLocation("2006-01-02 15:04:05", startTime, time.Local)
		activatedAt.Valid = true
		if err != nil {
			logging.Errorf("encode run activateTime failed. error: %v", err)
			return 0, false
		}
	}

	updateRun := models.Run{
		Status:      status,
		Message:     message,
		ActivatedAt: activatedAt,
	}

	if err := models.UpdateRun(logging, runID, updateRun); err != nil {
		logging.Errorf("update run in db failed. error: %v", err)
		return 0, false
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

	return 0, true
}

func UpdateRuntimeDagByWfEvent(id string, event interface{}) (int64, bool) {
	logging := logger.LoggerForRun(id)
	wfEvent, ok := event.(*pipeline.WorkflowEvent)
	if !ok {
		logging.Errorf("event type-casting failed in update dag callback")
		return 0, false
	}
	if wfEvent.Event != pipeline.WfEventDagUpdate {
		logging.Errorf("event type[%s] invalid in update dag callback", wfEvent.Event)
		return 0, false
	}

	runID, ok := wfEvent.Extra[common.WfEventKeyRunID].(string)
	if !ok || runID != id {
		logging.Errorf("runid error(not equal) in callback")
		return 0, false
	}

	runtimeDag, ok := wfEvent.Extra[common.WfEventKeyView].(*schema.DagView)
	if !ok {
		logging.Errorf("get dag in update dag cb failed")
		return 0, false
	}

	dagName := runtimeDag.DagName
	pk := runtimeDag.PK

	runDag := models.ParseRunDag(runtimeDag)
	if err := runDag.Encode(); err != nil {
		logging.Errorf("encode runDag failed, error: %s", err.Error())
		return 0, false
	}
	if pk <= 0 {
		logging.Infof("create run_dag with pk[%d]", pk)
		// 如果pk小于等于0，则需要在数据库创建job记录
		var err error
		// dagView中没有保存DagName和RunID
		runDag.DagName = dagName
		runDag.RunID = id
		pk, err = models.CreateRunDag(logging, &runDag)
		if err != nil {
			logging.Errorf("create run_dag in callback faild")
			return 0, false
		}
	} else {
		logging.Infof("update run_dag with pk[%d]", pk)
		if err := models.UpdateRunDag(logging, pk, runDag); err != nil {
			logging.Errorf("update run_dag in callback failed")
			return 0, false
		}
	}

	return pk, true
}

func UpdateRuntimeJobByWfEvent(id string, event interface{}) (int64, bool) {
	logging := logger.LoggerForRun(id)
	wfEvent, ok := event.(*pipeline.WorkflowEvent)
	if !ok {
		logging.Errorf("event type-casting failed in update job callback")
		return 0, false
	}
	if wfEvent.Event != pipeline.WfEventJobUpdate {
		logging.Errorf("event type[%s] invalid in update job callback", wfEvent.Event)
		return 0, false
	}

	runID, ok := wfEvent.Extra[common.WfEventKeyRunID].(string)
	if !ok || runID != id {
		logging.Errorf("runid error(not equal) in callback")
		return 0, false
	}

	runtimeJob, ok := wfEvent.Extra[common.WfEventKeyView].(*schema.JobView)
	if !ok {
		logging.Errorf("get job in update job cb failed")
		return 0, false
	}

	stepName := runtimeJob.StepName
	pk := runtimeJob.PK

	runJob := models.ParseRunJob(runtimeJob)
	if err := runJob.Encode(); err != nil {
		logging.Errorf("encode runJob failed, error: %s", err.Error())
		return 0, false
	}
	if pk <= 0 {
		// 如果pk小于等于0，则需要在数据库创建job记录
		var err error
		// stepName, runID, jobView中没有
		runJob.StepName = stepName
		runJob.RunID = id
		pk, err = models.CreateRunJob(logging, &runJob)
		if err != nil {
			logging.Errorf("create run_job in callback faild")
			return 0, false
		}
	} else {
		if err := models.UpdateRunJob(logging, pk, runJob); err != nil {
			logging.Errorf("update run_job in callback failed")
			return 0, false
		}
	}

	if err := updateRunCache(logging, runtimeJob, runID); err != nil {
		return 0, false
	}
	return pk, true
}

func updateRunCache(logging *logrus.Entry, runtimeJob *schema.JobView, runID string) error {
	var runCached models.Run
	if runtimeJob.CacheRunID != "" {
		var err error
		runCached, err = models.GetRunByID(logging, runtimeJob.CacheRunID)
		if err != nil {
			logging.Errorf("update cacheIDs failed. Get run[%v] failed. error: %v", runtimeJob.CacheRunID, err)
			return err
		}

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
		imageUrl, err = storage.Image.GetUrlByPFImageID(logEntry, imageInfo.PFImageID)
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
		image := model.Image{
			ID:      imageInfo.PFImageID,
			ImageID: imageInfo.ImageID,
			FsID:    imageInfo.FsID,
			Source:  imageInfo.Source,
			Url:     imageUrl,
		}
		_, err := storage.Image.GetImage(logEntry, imageInfo.PFImageID)
		if err != nil {
			if errors.GetErrorCode(err) == errors.ErrorRecordNotFound {
				// image not in db. save image info to db
				logEntry.Debugf("image handler cb store new image[%s] with url[%s]\n", imageInfo.PFImageID, imageUrl)
				if err := storage.Image.CreateImage(logEntry, &image); err != nil {
					logEntry.Errorf("CreateImage[%s] with url[%s] in db failed. error: %v",
						imageInfo.PFImageID, imageUrl, err)
				}
			} else {
				logEntry.Errorf("image handler cb get image[%s] from db failed. err: %v", imageInfo.PFImageID, err)
			}
		} else {
			// image in db, update it
			logEntry.Debugf("image handler cb update image[%s] url[%s]\n", imageInfo.PFImageID, imageUrl)
			if err := storage.Image.UpdateImage(logEntry, imageInfo.PFImageID, image); err != nil {
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
	wfs, err := schema.GetWorkflowSource([]byte(run.RunYaml))
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
