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

package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rifflock/lfshook"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	hostNameHolder = "{HOSTNAME}"
)

type LogConfig struct {
	Dir             string `yaml:"dir"`
	FilePrefix      string `yaml:"filePrefix"`
	Level           string `yaml:"level"`
	MaxKeepDays     int    `yaml:"maxKeepDays"`
	MaxFileNum      int    `yaml:"maxFileNum"`
	MaxFileSizeInMB int    `yaml:"maxFileSizeInMB"`
	IsCompress      bool   `yaml:"isCompress"`
	Formatter       string `yaml:"formatter"`
}

func AddFlagSet(fs *pflag.FlagSet, logConf *LogConfig) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	fs.StringVar(&logConf.Dir, "log-dir", logConf.Dir, "Directory of log")
	fs.StringVar(&logConf.FilePrefix, "log-file-prefix", logConf.FilePrefix, "Prefix of log file")
	fs.StringVar(&logConf.Level, "log-level", logConf.Level, "Log level")
	fs.IntVar(&logConf.MaxKeepDays, "log-max-keep-days", logConf.MaxKeepDays, "Log max keep days")
	fs.IntVar(&logConf.MaxFileNum, "log-max-file-num", logConf.MaxFileNum, "Log max file number")
	fs.IntVar(&logConf.MaxFileSizeInMB, "log-max-file-size-in-mb", logConf.MaxFileSizeInMB, "Log max file size(M)")
	fs.BoolVar(&logConf.IsCompress, "log-is-compress", logConf.IsCompress, "Use log compress")
	fs.StringVar(&logConf.Formatter, "log-formatter", logConf.Formatter, "Use log compress")
}

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
		fmt.Println(fmt.Sprintf("failed to get hostname: %v", err))
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
		fmt.Errorf("failed to parse logger level: %v\n", err)
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
