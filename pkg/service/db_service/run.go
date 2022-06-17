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

package db_service

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

// validate & patch runtime and postProcess
func patchRuntimeAndPostProcess(r *models.Run) error {
	if r.Runtime == nil {
		r.Runtime = schema.RuntimeView{}
	}
	if r.PostProcess == nil {
		r.PostProcess = schema.PostProcessView{}
	}
	// 从数据库中获取该Run的所有Step发起的Job
	runJobs, err := GetRunJobsOfRun(logger.LoggerForRun(r.ID), r.ID)
	if err != nil {
		return err
	}
	// 将所有run_job转换成JobView之后，赋值给Runtime和PostProcess
	for _, job := range runJobs {
		if step, ok := r.WorkflowSource.PostProcess[job.StepName]; ok {
			jobView := job.ParseJobView(step)
			r.PostProcess[job.StepName] = jobView
		} else if step, ok := r.WorkflowSource.EntryPoints[job.StepName]; ok {
			jobView := job.ParseJobView(step)
			r.Runtime[job.StepName] = jobView
		} else {
			entryPointNames := []string{}
			for name := range r.Runtime {
				entryPointNames = append(entryPointNames, name)
			}
			postProcessNames := []string{}
			for name := range r.PostProcess {
				postProcessNames = append(postProcessNames, name)
			}
			return fmt.Errorf("cannot find step[%s] in either entry_points[%v]\nor post_process[%v]",
				job.StepName, entryPointNames, postProcessNames)
		}
	}
	// 初始化env中的PF_RUN_TIME
	if err := initAllPFRuntime(r); err != nil {
		return err
	}
	return nil
}

func initAllPFRuntime(r *models.Run) error {
	pfRuntimeGen := common.NewPFRuntimeGenerator(r.Runtime, r.WorkflowSource)
	for name, step := range r.Runtime {
		pfRuntimeJson, err := pfRuntimeGen.GetPFRuntime(name)
		if err != nil {
			return err
		}
		step.Env[common.SysParamNamePFRuntime] = pfRuntimeJson
		r.Runtime[name] = step
	}
	for name, step := range r.PostProcess {
		pfRuntimeJson, err := pfRuntimeGen.GetPFRuntime(name)
		if err != nil {
			return err
		}
		step.Env[common.SysParamNamePFRuntime] = pfRuntimeJson
		r.PostProcess[name] = step
	}
	return nil
}

func CreateRun(logEntry *log.Entry, run *models.Run) (string, error) {
	logEntry.Debugf("begin create run:%+v", run)
	err := WithTransaction(database.DB, func(tx *gorm.DB) error {
		result := tx.Model(&models.Run{}).Create(run)
		if result.Error != nil {
			logEntry.Errorf("create run failed. run:%v, error:%s",
				run, result.Error.Error())
			return result.Error
		}
		run.ID = schema.PrefixRun + fmt.Sprintf("%06d", run.Pk)
		logEntry.Debugf("created run with pk[%d], runID[%s]", run.Pk, run.ID)
		// update ID
		result = tx.Model(&models.Run{}).Where("pk = ?", run.Pk).Update("id", run.ID)
		if result.Error != nil {
			logEntry.Errorf("back filling runID failed. pk[%d], error:%v",
				run.Pk, result.Error)
			return result.Error
		}
		return nil
	})

	return run.ID, err
}

func UpdateRunStatus(logEntry *log.Entry, runID, status string) error {
	logEntry.Debugf("begin update run status. runID:%s, status:%s", runID, status)
	tx := database.DB.Model(&models.Run{}).Where("id = ?", runID).Update("status", status)
	if tx.Error != nil {
		logEntry.Errorf("update run status failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func UpdateRun(logEntry *log.Entry, runID string, run models.Run) error {
	logEntry.Debugf("begin update run. runID:%s", runID)
	tx := database.DB.Model(&models.Run{}).Where("id = ?", runID).Updates(run)
	if tx.Error != nil {
		logEntry.Errorf("update run failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteRun(logEntry *log.Entry, runID string) error {
	logEntry.Debugf("begin delete run. runID:%s", runID)
	err := WithTransaction(database.DB, func(tx *gorm.DB) error {
		result := database.DB.Model(&models.RunJob{}).Where("run_id = ?", runID).Delete(&models.RunJob{})
		if result.Error != nil {
			logEntry.Errorf("delete run_job before deleting run failed. runID:%s, error:%s",
				runID, result.Error.Error())
			return result.Error
		}
		result = database.DB.Model(&models.Run{}).Where("id = ?", runID).Delete(&models.Run{})
		if result.Error != nil {
			logEntry.Errorf("delete run failed. runID:%s, error:%s",
				runID, result.Error.Error())
			return result.Error
		}
		return nil
	})
	return err
}

func GetRunByID(logEntry *log.Entry, runID string) (models.Run, error) {
	logEntry.Debugf("begin get run. runID:%s", runID)
	var run models.Run
	tx := database.DB.Model(&models.Run{}).Where("id = ?", runID).First(&run)
	if tx.Error != nil {
		logEntry.Errorf("get run failed. runID:%s, error:%s",
			runID, tx.Error.Error())
		return models.Run{}, tx.Error
	}
	if err := patchRuntimeAndPostProcess(&run); err != nil {
		logEntry.Errorf("GetRunByID[%s] patchRuntimeAndPostProcess error:%s", runID, err)
		return models.Run{}, err
	}
	return run, nil
}

func ListRun(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, nameFilter []string) ([]models.Run, error) {
	logEntry.Debugf("begin list run. ")
	tx := database.DB.Model(&models.Run{}).Where("pk > ?", pk)
	if len(userFilter) > 0 {
		tx = tx.Where("user_name IN (?)", userFilter)
	}
	if len(fsFilter) > 0 {
		tx = tx.Where("fs_name IN (?)", fsFilter)
	}
	if len(runFilter) > 0 {
		tx = tx.Where("id IN (?)", runFilter)
	}
	if len(nameFilter) > 0 {
		tx = tx.Where("name IN (?)", nameFilter)
	}
	if maxKeys > 0 {
		tx = tx.Limit(maxKeys)
	}
	runList := make([]models.Run, 0)
	tx = tx.Find(&runList)
	if tx.Error != nil {
		logEntry.Errorf("list run failed. Filters: user{%v}, fs{%v}, run{%v}, name{%v}. error:%s",
			userFilter, fsFilter, runFilter, nameFilter, tx.Error.Error())
		return runList, tx.Error
	}
	for _, run := range runList {
		if err := patchRuntimeAndPostProcess(&run); err != nil {
			logEntry.Errorf("ListRun patchRuntimeAndPostProcess[%s] error:%s", run.ID, err)
			return runList, err
		}
	}
	return runList, nil
}

func GetLastRun(logEntry *log.Entry) (models.Run, error) {
	logEntry.Debugf("get last run. ")
	run := models.Run{}
	tx := database.DB.Model(&models.Run{}).Last(&run)
	if tx.Error != nil {
		logEntry.Errorf("get last run failed. error:%s", tx.Error.Error())
		return models.Run{}, tx.Error
	}
	if err := patchRuntimeAndPostProcess(&run); err != nil {
		logEntry.Errorf("GetLastRun[%s] patchRuntimeAndPostProcess error:%s", run.ID, err)
		return models.Run{}, err
	}
	return run, nil
}

func GetRunCount(logEntry *log.Entry) (int64, error) {
	logEntry.Debugf("get run count")
	var count int64
	tx := database.DB.Model(&models.Run{}).Count(&count)
	if tx.Error != nil {
		logEntry.Errorf("get run count failed. error:%s", tx.Error.Error())
		return 0, tx.Error
	}
	return count, nil
}

func ListRunsByStatus(logEntry *log.Entry, statusList []string) ([]models.Run, error) {
	logEntry.Debugf("begin list runs by status [%v]", statusList)
	runList := make([]models.Run, 0)
	tx := database.DB.Model(&models.Run{}).Where("status IN (?)", statusList).Find(&runList)
	if tx.Error != nil {
		logEntry.Errorf("list runs by status [%v] failed. error:%s", statusList, tx.Error.Error())
		return runList, tx.Error
	}
	for _, run := range runList {
		if err := patchRuntimeAndPostProcess(&run); err != nil {
			logEntry.Errorf("ListRunsByStatus[%v] patchRuntimeAndPostProcess error:%s", statusList, err)
			return runList, err
		}
	}
	return runList, nil
}
