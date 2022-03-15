/*
Copyright (c) 2022 PaddlePaddle Authors. All Rights Reserve.

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

package api

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/handler"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

type PFJobInterface interface {
	CreateJob() (string, error)
	StopJobByID(id string) error
	GetID() string
}

// getDefaultPath get extra runtime conf default path
func (j *PFJob) getDefaultPath() string {
	baseDir := config.GlobalServerConfig.Job.DefaultJobYamlDir
	if len(j.JobMode) != 0 {
		return fmt.Sprintf("%s/%s_%s.yaml", baseDir, j.JobType, strings.ToLower(j.JobMode))
	} else {
		return fmt.Sprintf("%s/%s.yaml", baseDir, j.JobType)
	}
}

// GetExtRuntimeConf get extra runtime conf from file
func (j *PFJob) GetExtRuntimeConf(fsID, filePath string) ([]byte, error) {
	if len(filePath) == 0 {
		// get extra runtime conf from default path
		filePath = j.getDefaultPath()
		// check file exist
		if exist, err := config.PathExists(filePath); !exist || err != nil {
			log.Errorf("get job from path[%s] failed, file.exsit=[%v], err=[%v]", filePath, exist, err)
			return nil, errors.JobFileNotFound(filePath)
		}

		// read extRuntimeConf as []byte
		extConf, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Errorf("read file [%s] failed! err:[%v]\n", filePath, err)
			return nil, err
		}
		return extConf, nil
	} else {
		conf, err := handler.ReadFileFromFs(fsID, filePath, logger.Logger())
		if err != nil {
			log.Errorf("get job from path[%s] failed, err=[%v]", filePath, err)
			return nil, err
		}

		log.Debugf("reading extra runtime conf[%s]", conf)
		return conf, nil
	}
}

// PFJob will have all info of a Job
type PFJob struct {
	ID        string
	Name      string
	Namespace string
	// JobType of job
	JobType schema.JobType
	JobMode string
	Status  string
	// ClusterID and QueueID of job
	ClusterID ClusterID
	QueueID   QueueID
	FSID      string
	UserName  string
	// resource request resource for job
	Resource     *schema.Resource
	Priority     int32
	MinAvailable int32
	// ExtRuntimeConf define extra runtime conf
	ExtRuntimeConf []byte

	// Conf for job
	Conf schema.Conf

	// extend field
	Tags   []string
	LogUrl string

	WaitingTime *time.Duration
	CreateTime  time.Time
	StartTime   time.Time
	EndTIme     time.Time
}

func NewJobInfo(conf schema.PFJobConf, jobID string) (*PFJob, error) {
	jobConf := conf.(*schema.Conf)

	pfjob := &PFJob{
		ID:        jobID,
		Name:      conf.GetName(),
		Namespace: conf.GetNamespace(),
		JobType:   conf.Type(),
		JobMode:   conf.GetJobMode(),
		ClusterID: ClusterID(conf.GetClusterID()),
		QueueID:   QueueID(conf.GetQueueID()),
		FSID:      conf.GetFS(),
		UserName:  conf.GetUserName(),
		Conf:      *jobConf,
	}
	var err error
	pfjob.ExtRuntimeConf, err = pfjob.GetExtRuntimeConf(conf.GetFS(), conf.GetYamlPath())
	if err != nil {
		return nil, fmt.Errorf("get extra runtime config failed, err: %v", err)
	}
	return pfjob, nil
}
