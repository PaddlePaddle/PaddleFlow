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
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

func CreateRunJobs(logEntry *log.Entry, jobs map[string]schema.JobView, runID string) error {
	logEntry.Debugf("begin create run_jobs by jobMap: %v", jobs)
	err := WithTransaction(database.DB, func(tx *gorm.DB) error {
		for name, job := range jobs {
			runJob := models.RunJob{
				ID:       job.JobID,
				RunID:    runID,
				Name:     job.JobName,
				StepName: name,
			}
			result := tx.Model(&models.RunJob{}).Create(&runJob)
			if result.Error != nil {
				logEntry.Errorf("create run_job failed. run_job: %v, error: %s",
					runJob, result.Error.Error())
				return result.Error
			}
		}
		return nil
	})
	return err
}

func UpdateRunJob(logEntry *log.Entry, runID string, stepName string, runJob models.RunJob) error {
	logEntry.Debugf("begin update run_job. run_job run_id: %s, step_name: %s", runID, stepName)
	tx := database.DB.Model(&models.RunJob{}).Where("run_id = ?", runID).Where("step_name = ?", stepName).Updates(runJob)
	if tx.Error != nil {
		logEntry.Errorf("update run_job failed. run_id: [%s], step_name: [%s], error: %s",
			runID, stepName, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func GetRunJobsOfRun(logEntry *log.Entry, runID string) ([]models.RunJob, error) {
	logEntry.Debugf("begin to get run_jobs of run with runID[%s].", runID)
	var runJobs []models.RunJob
	tx := database.DB.Model(&models.RunJob{}).Where("run_id = ?", runID).Find(&runJobs)
	if tx.Error != nil {
		logEntry.Errorf("get run_jobs of run with runID[%s] failed. error:%s", runID, tx.Error.Error())
		return []models.RunJob{}, tx.Error
	}
	return runJobs, nil
}
