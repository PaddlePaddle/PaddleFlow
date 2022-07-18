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
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// Default logger format will output [2006-01-02T15:04:05Z07:00] [INFO] (/fileName.go:10) Log message
	defaultLogFormat       = "[%time%][%lvl%][%file%]%customFields% %msg%\n"
	defaultTimestampFormat = time.RFC3339Nano
)

// Formatter implements logrus.Formatter interface.
type Formatter struct {
	// Timestamp format
	TimestampFormat string
	// Available standard keys: time, msg, lvl
	// Also can include custom fields but limited to strings.
	// All of fields need to be wrapped inside %% i.e %time% %msg%
	LogFormat string
}

// Format building logger message.
func (f *Formatter) Format(entry *log.Entry) ([]byte, error) {
	output := f.LogFormat
	if output == "" {
		output = defaultLogFormat
	}

	timestampFormat := f.TimestampFormat
	if timestampFormat == "" {
		timestampFormat = defaultTimestampFormat
	}

	output = strings.Replace(output, "%time%", entry.Time.Format(timestampFormat), 1)
	output = strings.Replace(output, "%msg%", entry.Message, 1)

	level := strings.ToUpper(entry.Level.String())
	output = strings.Replace(output, "%lvl%", level, 1)
	if entry.Caller.File != "" {
		output = strings.Replace(output, "%file%", fmt.Sprintf("%s:%d", entry.Caller.File, entry.Caller.Line), 1)
	}

	customFields := ""
	for k, val := range entry.Data {
		switch v := val.(type) {
		case string:
			customFields += fmt.Sprintf("[%s:%s]", k, v)
		case int:
			s := strconv.Itoa(v)
			customFields += fmt.Sprintf("[%s:%s]", k, s)
		case bool:
			s := strconv.FormatBool(v)
			customFields += fmt.Sprintf("[%s:%s]", k, s)
		case nil:
			customFields += fmt.Sprintf("[%s]", k)
		default:
			customFields += fmt.Sprintf("[%s:%v]", k, v)
		}
	}
	output = strings.Replace(output, "%customFields%", customFields, 1)
	return []byte(output), nil
}
