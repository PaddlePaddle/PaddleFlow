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
	"github.com/urfave/cli/v2"
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

func LogFlags(logConf *LogConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "log-dir",
			Value:       logConf.Dir,
			Usage:       "directory of log",
			Destination: &logConf.Dir,
		},
		&cli.StringFlag{
			Name:        "log-file-prefix",
			Value:       logConf.FilePrefix,
			Usage:       "prefix of log file",
			Destination: &logConf.FilePrefix,
		},
		&cli.StringFlag{
			Name:        "log-level",
			Value:       logConf.Level,
			Usage:       "log level",
			Destination: &logConf.Level,
		},
		&cli.StringFlag{
			Name:        "log-formatter",
			Value:       logConf.Formatter,
			Usage:       "log formatter",
			Destination: &logConf.Formatter,
		},
		&cli.BoolFlag{
			Name:        "log-is-compress",
			Value:       logConf.IsCompress,
			Usage:       "log compress",
			Destination: &logConf.IsCompress,
		},
		&cli.IntFlag{
			Name:        "log-max-keep-days",
			Value:       logConf.MaxKeepDays,
			Usage:       "log max keep days",
			Destination: &logConf.MaxKeepDays,
		},
		&cli.IntFlag{
			Name:        "log-max-file-num",
			Value:       logConf.MaxFileNum,
			Usage:       "log max file number",
			Destination: &logConf.MaxFileNum,
		},
		&cli.IntFlag{
			Name:        "log-max-file-size-in-mb",
			Value:       logConf.MaxFileSizeInMB,
			Usage:       "log max file size in MiB",
			Destination: &logConf.MaxFileSizeInMB,
		},
	}
}
