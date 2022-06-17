package db_service

import (
	"fmt"

	"gorm.io/gorm/clause"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
)

func GetJobTaskByID(id string) (models.JobTask, error) {
	var taskStatus models.JobTask
	tx := database.DB.Model(&models.RunCache{}).Where("id = ?", id).First(&taskStatus)
	if tx.Error != nil {
		logger.LoggerForJob(id).Errorf("get job task status failed, err %v", tx.Error.Error())
		return models.JobTask{}, tx.Error
	}
	return taskStatus, nil
}

func UpdateTask(task *models.JobTask) error {
	if task == nil {
		return fmt.Errorf("JobTask is nil")
	}
	// TODO: change update task logic
	tx := database.DB.Model(&models.RunCache{}).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"status", "message", "ext_runtime_status", "deleted_at"}),
	}).Create(task)
	return tx.Error
}

func ListByJobID(jobID string) ([]models.JobTask, error) {
	var jobList []models.JobTask
	err := database.DB.Model(&models.RunCache{}).Where("job_id = ?", jobID).Find(&jobList).Error
	if err != nil {
		return nil, err
	}
	return jobList, nil
}
