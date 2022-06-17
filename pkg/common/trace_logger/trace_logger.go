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

/**
 * @Author: kiritoxkiriko
 * @Date: 2022/6/15
 * @Description: trace logger
 */

package trace_logger

import (
	"fmt"
	file_logger "github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/sirupsen/logrus"
)

// initFileLogger
/**
 * Copy from ../logger/file_logger.go
 * made some modification
 */

var logger *logrus.Logger

func InitTraceLogger(config file_logger.LogConfig) error {
	l := logrus.New()
	// set logger formatter to json
	config.Formatter = "json"
	if err := file_logger.InitFileLogger(l, &config); err != nil {
		return fmt.Errorf("failed to init file logger: %w", err)
	}
	logger = l
	return nil
}
