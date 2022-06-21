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
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
	"strings"
)

// initFileLogger
/**
 * Copy from ../logger/file_logger.go
 * made some modification
 */

const (
	hostNameHolder = "{HOSTNAME}"
)

var logger *logrus.Logger

func InitTraceLogger(config file_logger.LogConfig) error {
	l := logrus.New()
	// set logger formatter to json
	config.Formatter = "json"
	if err := InitLogger(l, &config); err != nil {
		return fmt.Errorf("failed to init file logger: %w", err)
	}
	logger = l
	return nil
}

func InitLogger(logger *logrus.Logger, logConf *file_logger.LogConfig) error {
	hostname, err := os.Hostname()
	if err != nil {
		err = fmt.Errorf("failed to get hostname: %w", err)
		return err
	}

	// init lumberjack logger
	logPath := filepath.Join(logConf.Dir, strings.ReplaceAll(logConf.FilePrefix, hostNameHolder, hostname))
	fmt.Printf("logPath:%s\n", logPath)
	writer := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    logConf.MaxFileSizeInMB,
		MaxAge:     logConf.MaxKeepDays,
		MaxBackups: logConf.MaxFileNum,
		LocalTime:  true,
		Compress:   logConf.IsCompress,
	}
	level, err := logrus.ParseLevel(logConf.Level)
	if err != nil {
		err = fmt.Errorf("failed to parse logger level: %w", err)
		return err
	}

	// init logrus logger
	// don't report caller
	logger.SetLevel(level)
	logger.SetReportCaller(false)
	// set lumberjack logger as logrus logger's output
	// don't log it to stdout
	logger.SetOutput(writer)

	if strings.EqualFold(logConf.Formatter, "json") {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else if strings.EqualFold(logConf.Formatter, "text") {
		logger.SetFormatter(&logrus.TextFormatter{})
	} else {
		return fmt.Errorf("invalid formatter: %s", logConf.Formatter)
	}

	return nil
}
