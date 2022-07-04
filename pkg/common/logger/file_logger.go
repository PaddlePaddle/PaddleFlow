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

package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

const hostNameHolder = "{HOSTNAME}"

/*
 * InitStandardFileLogger - initialize standard logger for file record
 * PARAMS:
 *   - logConf: config of log
 * RETURNS:
 * 	nil, if succeed
 *  error, if fail
 */
func InitStandardFileLogger(logConf *LogConfig) error {
	return InitFileLogger(log.StandardLogger(), logConf)
}

/*
 * InitFileLogger - initialize file logger
 * PARAMS:
 *   - logger: *log.Logger
 *   - logConf: config of log
 * RETURNS:
 * 	nil, if succeed
 *  error, if fail
 */
func InitFileLogger(logger *log.Logger, logConf *LogConfig) error {
	hostname, err := os.Hostname()
	if err != nil {
		err = fmt.Errorf("failed to get hostname: %w", err)
		return err
	}

	logPath := filepath.Join(logConf.Dir, strings.Replace(logConf.FilePrefix, hostNameHolder, hostname, -1))
	fmt.Printf("logPath:%s\n", logPath)
	writer := &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    logConf.MaxFileSizeInMB,
		MaxAge:     logConf.MaxKeepDays,
		MaxBackups: logConf.MaxFileNum,
		LocalTime:  true,
		Compress:   logConf.IsCompress,
	}
	level, err := log.ParseLevel(logConf.Level)
	if err != nil {
		err = fmt.Errorf("failed to parse logger level: %w", err)
		return err
	}
	logger.SetLevel(level)
	logger.SetReportCaller(true)

	if strings.EqualFold(logConf.Formatter, "json") {
		logger.SetFormatter(&log.JSONFormatter{})
	} else if strings.EqualFold(logConf.Formatter, "text") {
		logger.SetFormatter(&log.TextFormatter{})
	} else {
		formatter := Formatter{
			TimestampFormat: time.RFC3339Nano,
		}
		logger.SetFormatter(&formatter)
	}

	lfHook := lfshook.NewHook(lfshook.WriterMap{
		log.ErrorLevel: writer,
		log.FatalLevel: writer,
		log.PanicLevel: writer,
		log.DebugLevel: writer,
		log.InfoLevel:  writer,
		log.WarnLevel:  writer,
	}, logger.Formatter)
	logger.AddHook(lfHook)
	return nil
}
