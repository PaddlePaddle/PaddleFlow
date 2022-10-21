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

package trace_logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

// initFileLogger
/**
 * Copy from ../fileLogger/file_logger.go
 * made some modification
 */

const (
	hostNameHolder = "{HOSTNAME}"
)

// default configs
const (
	DefaultDir             string = "./"
	DefaultFilePrefix      string = "trace_logger"
	DefaultLevel           string = "INFO"
	DefaultMaxKeepDays     int    = 7
	DefaultMaxFileNum      int    = 3
	DefaultMaxFileSizeInMB int    = 10
	DefaultTimeout         string = "2h"
	DefaultMaxCacheSize    int    = 10000
	DefaultSyncInterval    string = "10m"
	DefaultDeleteInterval  string = "1m"
	LogrusField                   = "trace_logger"
)

var (
	fileLogger *logrus.Logger
	logger     *logrus.Entry
	manager    TraceLoggerManager
)

type TraceLoggerConfig struct {
	Dir             string `yaml:"dir"`             // Dir log file dir
	FilePrefix      string `yaml:"filePrefix"`      // FilePrefix log file prefix
	Level           string `yaml:"level"`           // Level log level
	MaxKeepDays     int    `yaml:"maxKeepDays"`     // MaxKeepDays max keep days for log rotation
	MaxFileNum      int    `yaml:"maxFileNum"`      // MaxFileNum max file num for log rotation
	MaxFileSizeInMB int    `yaml:"maxFileSizeInMB"` // MaxFileSizeInMB max file size in MB for log rotation
	IsCompress      bool   `yaml:"isCompress"`      // IsCompress is compress log file
	Timeout         string `yaml:"timeout"`         // Timeout for local cache
	MaxCacheSize    int    `yaml:"maxCacheSize"`    // MaxCacheSize max local cache size, evict when cache size exceed this value
	SyncInterval    string `yaml:"syncInterval"`    // SyncInterval auto syncs interval
	DeleteInterval  string `yaml:"deleteInterval"`  // DeleteInterval auto delete interval
	Debug           bool   `yaml:"debug"`           // Debug is debug mode, print log to stdout if set true
}

func ParseTimeUnit(timeStr string) (time.Duration, error) {
	timeStr = strings.TrimSpace(timeStr)
	if timeStr == "" {
		return 0, fmt.Errorf("timeStr is empty")
	}
	unit := timeStr[len(timeStr)-1:]
	timeVal, err := strconv.Atoi(timeStr[:len(timeStr)-1])
	if err != nil {
		return 0, fmt.Errorf("failed to parse time: %w", err)
	}
	switch strings.ToLower(unit) {
	case "s":
		return time.Duration(timeVal) * time.Second, nil
	case "m":
		return time.Duration(timeVal) * time.Minute, nil
	case "h":
		return time.Duration(timeVal) * time.Hour, nil
	case "d":
		return time.Duration(timeVal) * time.Hour * 24, nil
	default:
		return 0, fmt.Errorf("unknown time unit: %s", unit)
	}
}

func ParseTimeUnitWithDefault(timeStr string, defaultTime time.Duration) time.Duration {
	res, err := ParseTimeUnit(timeStr)
	if err != nil {
		return defaultTime
	}
	return res
}

func fillDefaultValue(conf *TraceLoggerConfig) {
	if conf.Dir == "" {
		conf.Dir = DefaultDir
	}
	if conf.FilePrefix == "" {
		conf.FilePrefix = DefaultFilePrefix
	}
	if conf.Level == "" {
		conf.Level = DefaultLevel
	}
	if conf.MaxKeepDays == 0 {
		conf.MaxKeepDays = DefaultMaxKeepDays
	}
	if conf.MaxFileNum == 0 {
		conf.MaxFileNum = DefaultMaxFileNum
	}
	if conf.MaxFileSizeInMB == 0 {
		conf.MaxFileSizeInMB = DefaultMaxFileSizeInMB
	}
	if conf.Timeout == "" {
		conf.Timeout = DefaultTimeout
	}
	if conf.MaxCacheSize == 0 {
		conf.MaxCacheSize = DefaultMaxCacheSize
	}
	if conf.SyncInterval == "" {
		conf.SyncInterval = DefaultSyncInterval
	}
	if conf.DeleteInterval == "" {
		conf.DeleteInterval = DefaultDeleteInterval
	}
}

// init default logger
func init() {
	conf := TraceLoggerConfig{}
	fillDefaultValue(&conf)
	_ = InitTraceLoggerManager(conf)
}

func InitTraceLoggerManager(config TraceLoggerConfig) error {
	l := logrus.New()
	// set fileLogger formatter to json
	if err := initFileLogger(l, &config); err != nil {
		return fmt.Errorf("failed to init file fileLogger: %w", err)
	}
	// set file logger
	fileLogger = l
	// set logger entry
	logger = logrus.WithField(LogrusField, nil)
	timeout, err := ParseTimeUnit(config.Timeout)
	m := NewDefaultTraceLoggerManager(config.MaxCacheSize, timeout, config.Debug)
	if err != nil {
		return fmt.Errorf("failed to parse timeout: %w", err)
	}

	manager = m
	return nil
}

func initFileLogger(logger *logrus.Logger, logConf *TraceLoggerConfig) error {
	hostname, err := os.Hostname()
	if err != nil {
		err = fmt.Errorf("failed to get hostname: %w", err)
		return err
	}

	// init lumberjack fileLogger
	logPath := filepath.Join(logConf.Dir, strings.ReplaceAll(logConf.FilePrefix, hostNameHolder, hostname))
	//fmt.Printf("logPath:%s\n", logPath)
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
		err = fmt.Errorf("failed to parse fileLogger level: %w", err)
		return err
	}

	// init logrus fileLogger
	// don't report caller
	logger.SetLevel(level)
	logger.SetReportCaller(false)

	// set lumberjack fileLogger as logrus fileLogger's output
	// don't log it to stdout
	logger.SetOutput(writer)

	logger.SetFormatter(&logrus.JSONFormatter{})

	return nil
}

// add package wide function

// Key this function will create a trace fileLogger for every unique key, the logs will be saved to a same slice when use same trace fileLogger.
// A new key will be treated as temp key, logs will not be saved until the key is updated by UpdateKey.
func Key(key string) TraceLogger {
	return manager.Key(key)
}

// KeyWithUpdate same behavior as Key method, but the key will be updated to newKey after the fileLogger is created.
// i.e. KeyWithUpdate behave same as Key(key1) + UpdateKey(key1, key1)
func KeyWithUpdate(key string) TraceLogger {
	return manager.KeyWithUpdate(key)
}

// UpdateKey this function will update the key of the trace fileLogger.
func UpdateKey(oldKey, newKey string) error {
	return manager.UpdateKey(oldKey, newKey)
}

func SyncAll() error {
	return manager.SyncAll()
}

func LoadAll(path string, prefix ...string) error {
	return manager.LoadAll(path, prefix...)
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

func GetTraceFromCache(key string) (Trace, bool) {
	return manager.GetTraceFromCache(key)
}

// GetTracesByRunIDAndJobID return the traces by run or job id, if not found, return nil, false
// it will return related run and job log for runID, and related job log for jobID,
// it will omit empty IDs
func GetTracesByRunIDAndJobID(runID, jobID string) ([]Trace, bool) {
	traces := make([]Trace, 0)
	// update key
	if runID != "" {
		runTrace, ok := manager.GetTraceFromCache(runID)
		if ok {
			traces = append(traces, runTrace)
		}

		runJobTraces, ok := GetJobTracesByRunID(runID)
		if ok {
			traces = append(traces, runJobTraces...)
		}

	}
	if jobID != "" {
		jobTraces, ok := manager.GetTraceFromCache(runID)
		if ok {
			traces = append(traces, jobTraces)
		}
	}
	if len(traces) == 0 {
		return nil, false
	}
	return traces, true
}

// GetJobTracesByRunID for run and job relation mapping
func GetJobTracesByRunID(runID string) ([]Trace, bool) {
	var jobTraces []Trace
	jobs, err := storage.Job.GetJobsByRunID(runID, "")
	if err != nil {
		logger.Errorf("failed to get jobs by runID: %s, err: %s", runID, err.Error())
	}
	// if empty return
	if len(jobs) == 0 {
		return jobTraces, false
	}
	jobTraces = make([]Trace, 0, len(jobs))

	for _, job := range jobs {
		jobTrace, ok := GetTraceFromCache(job.ID)
		if !ok {
			continue
		}
		jobTraces = append(jobTraces, jobTrace)
	}

	// double check if jobTraces is empty
	if len(jobTraces) == 0 {
		return nil, false
	}
	return jobTraces, true
}
