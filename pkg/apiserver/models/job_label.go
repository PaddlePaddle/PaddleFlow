package models

import (
	"time"

	"gorm.io/gorm"

	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
)

type JobLabel struct {
	Pk        int64  `gorm:"primaryKey;autoIncrement"`
	ID        string `gorm:"type:varchar(36);uniqueIndex"`
	Label     string `gorm:"type:varchar(255);NOT NULL"`
	JobID     string `gorm:"type:varchar(60);NOT NULL"`
	CreatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (JobLabel) TableName() string {
	return "job_label"
}

// list job process multi label get and result
func ListJobIDByLabels(ctx *logger.RequestContext, labels map[string]string) ([]string, error) {
	jobIDs := make([]string, 0)
	var jobLabels []JobLabel
	labelsListStr := make([]string, 0)
	for k, v := range labels {
		item := k + "=" + v
		labelsListStr = append(labelsListStr, item)
	}
	err := database.DB.Table("job_label").Where("label IN (?)", labelsListStr).Find(&jobLabels).Error
	if err != nil {
		ctx.Logging().Errorf("list jobID by labels failed, error:[%s]", err.Error())
		return nil, err
	}
	jobLabelsMap := make(map[string]map[string]interface{}, 0)
	for _, v := range jobLabels {
		if _, ok := jobLabelsMap[v.JobID]; !ok {
			jobLabelsMap[v.JobID] = make(map[string]interface{}, 0)
			jobLabelsMap[v.JobID][v.Label] = nil
		} else {
			jobLabelsMap[v.JobID][v.Label] = nil
		}
	}
	for k, v := range jobLabelsMap {
		flag := true
		for _, label := range labelsListStr {
			if _, ok := v[label]; !ok {
				flag = false
				break
			}
		}
		if flag {
			jobIDs = append(jobIDs, k)
		}
	}
	return jobIDs, nil
}
