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

/*
trace_logger is a tool to record trace log.
*/
package trace_logger

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
)

// DeleteFunc delete function for trace log
var DeleteFunc DeleteMethod = func(key string) bool {
	// get run from db
	run, err := models.GetRunByID(log.NewEntry(log.StandardLogger()), key)
	if err != nil {
		// if not found, delete trace log
		return true
	}
	// if run is not finished, keep trace log
	if common.IsRunFinalStatus(run.Status) {
		return true
	}
	return false
}

func Init(config TraceLoggerConfig) error {
	fillDefaultValue(&config)
	return InitTraceLoggerManager(config)
}

func Start(config TraceLoggerConfig) error {
	fillDefaultValue(&config)
	var err error
	// recover local trace log
	err = LoadAll(config.Dir, config.FilePrefix)
	// if error is NotExistErr, omit it
	if err != nil && !os.IsExist(err) {
		errMsg := fmt.Errorf("load local trace log failed. error: %w", err)
		return errMsg
	}
	err = nil

	duration, _ := ParseTimeUnit(config.DeleteInterval)
	// enable auto delete and sync for trace log
	if err = AutoDelete(
		duration,
		DeleteFunc,
	); err != nil {
		errMsg := fmt.Errorf("enable auto delete for trace log failed: %w", err)
		return errMsg
	}

	duration, _ = ParseTimeUnit(config.DeleteInterval)
	if err = AutoSync(
		duration,
	); err != nil {
		_ = CancelAutoDelete()
		errMsg := fmt.Errorf("enable auto sync for trace log failed: %w", err)
		return errMsg
	}
	return nil
}
