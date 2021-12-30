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

package run

import (
	"database/sql"
	"encoding/json"
	"time"

	"gopkg.in/yaml.v2"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/handler"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/pipeline"
)

var workflowCallbacks = pipeline.WorkflowCallbacks{
	UpdateRunCb:   UpdateRunFunc,
	LogCacheCb:    LogCacheFunc,
	ListCacheCb:   ListCacheFunc,
	LogArtifactCb: LogArtifactFunc,
}

var (
	UpdateRunFunc   func(id string, event interface{}) bool                             = UpdateRunByWfEvent
	LogCacheFunc    func(req schema.LogRunCacheRequest) (string, error)                 = LogCache
	ListCacheFunc   func(firstFp, fsID, step, source string) ([]models.RunCache, error) = ListCacheByFirstFp
	LogArtifactFunc func(req schema.LogRunArtifactRequest) error                        = LogArtifactEvent
)

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
	if common.IsRunFinalStatus(status) {
		logging.Debugf("run[%s] has reached final status[%s]", runID, status)
		delete(wfMap, runID)
	}
	runtime, ok := wfEvent.Extra[common.WfEventKeyRuntime].(schema.RuntimeView)
	if !ok {
		logging.Errorf("run[%s] malformat runtime", id)
		return false
	}
	runtimeRaw, err := json.Marshal(runtime)
	if err != nil {
		logging.Errorf("run[%s] json marshal runtime failed. error: %v", id, err)
		return false
	}
	logging.Debugf("workflow event update run[%s] status:%s message:%s, runtime:%s",
		id, status, wfEvent.Message, runtimeRaw)
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
	activatedAt := sql.NullTime{}
	if prevRun.Status == common.StatusRunPending {
		activatedAt.Time = time.Now()
		activatedAt.Valid = true
	}
	updateRun := models.Run{
		Status:      status,
		RuntimeRaw:  string(runtimeRaw),
		Message:     message,
		ActivatedAt: activatedAt,
	}
	if err := models.UpdateRun(logging, runID, updateRun); err != nil {
		logging.Errorf("update run[%s] in db failed. error: %v", id, err)
		return false
	}
	return true
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
			if database.GetErrorCode(err) == database.ErrorRecordNotFound {
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
	wfs := schema.WorkflowSource{}
	if err := yaml.Unmarshal([]byte(run.RunYaml), &wfs); err != nil {
		logger.LoggerForRun(run.ID).Errorf("Unmarshal runYaml failed. err:%v\n", err)
		return updateRunStatusAndMsg(runID, common.StatusRunFailed, err.Error())
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
		models.Run{ImageUrl: run.WorkflowSource.DockerEnv, Status: common.StatusRunPending})
}
