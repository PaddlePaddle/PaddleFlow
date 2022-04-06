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
	"github.com/spf13/pflag"
	"github.com/urfave/cli/v2"
)

/**
Currently supports two flag frameworks: fs uses urfave/cli/v2; apiserver uses spf13/pflag
*/

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

func LogFlags(logConf *LogConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "log-dir",
			Value:       "./log",
			Usage:       "directory of log",
			Destination: &logConf.Dir,
		},
		&cli.StringFlag{
			Name:        "log-file-prefix",
			Value:       "./pfs-fuse",
			Usage:       "prefix of log file",
			Destination: &logConf.FilePrefix,
		},
		&cli.StringFlag{
			Name:        "log-level",
			Value:       "INFO",
			Usage:       "log level",
			Destination: &logConf.Level,
		},
		&cli.StringFlag{
			Name:        "log-formatter",
			Value:       "",
			Usage:       "log formatter",
			Destination: &logConf.Formatter,
		},
		&cli.BoolFlag{
			Name:        "log-is-compress",
			Value:       true,
			Usage:       "log compress",
			Destination: &logConf.IsCompress,
		},
		&cli.IntFlag{
			Name:        "log-max-keep-days",
			Value:       90,
			Usage:       "log max keep days",
			Destination: &logConf.MaxKeepDays,
		},
		&cli.IntFlag{
			Name:        "log-max-file-num",
			Value:       100,
			Usage:       "log max file number",
			Destination: &logConf.MaxFileNum,
		},
		&cli.IntFlag{
			Name:        "log-max-file-size-in-mb",
			Value:       200 * 1024 * 1024,
			Usage:       "log max file size in MiB",
			Destination: &logConf.MaxFileSizeInMB,
		},
	}
}
