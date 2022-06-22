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
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// initFileLogger
/**
 * Copy from ../logger/file_logger.go
 * made some modification
 */

const (
	hostNameHolder = "{HOSTNAME}"
)

var (
	logger  *logrus.Logger
	manager TraceLoggerManager
)

type TraceLoggerConfig struct {
	Dir             string        `yaml:"dir"`
	FilePrefix      string        `yaml:"filePrefix"`
	Level           string        `yaml:"level"`
	MaxKeepDays     int           `yaml:"maxKeepDays"`
	MaxFileNum      int           `yaml:"maxFileNum"`
	MaxFileSizeInMB int           `yaml:"maxFileSizeInMB"`
	IsCompress      bool          `yaml:"isCompress"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxCacheSize    int           `yaml:"maxCacheSize"`
}

func InitTraceLogger(config TraceLoggerConfig) error {
	l := logrus.New()
	// set logger formatter to json
	if err := InitFileLogger(l, &config); err != nil {
		return fmt.Errorf("failed to init file logger: %w", err)
	}
	logger = l
	m := NewDefaultTraceLoggerManager()
	if config.Timeout > 0 {
		m.timeout = config.Timeout
	}
	if config.MaxCacheSize > 0 {
		m.maxCacheSize = config.MaxCacheSize
	}

	manager = m
	return nil
}

func InitFileLogger(logger *logrus.Logger, logConf *TraceLoggerConfig) error {
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

	logger.SetFormatter(&logrus.JSONFormatter{})

	return nil
}

// add package wide function

func Key(key string) TraceLogger {
	return manager.Key(key)
}

func UpdateKey(oldKey, newKey string) error {
	return manager.UpdateKey(oldKey, newKey)
}

func SyncAll() error {
	return manager.SyncAll()
}

func LoadAll(path string) error {
	return manager.LoadAll(path)
}

func ClearAll() error {
	return manager.ClearAll()
}

func AutoDelete(duration time.Duration, method ...DeleteMethod) error {
	return manager.AutoDelete(duration, method...)
}

func CancelAutoDelete() error {
	return manager.CancelAutoDelete()
}

func AutoSync(duration time.Duration) error {
	return manager.AutoSync(duration)
}

func CancelAutoSync() error {
	return manager.CancelAutoSync()
}
