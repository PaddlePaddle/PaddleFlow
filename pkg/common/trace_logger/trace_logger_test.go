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
 * @Date: 2022/6/17
 * @Description:
 */

package trace_logger

// TODO: file part is not ready

import (
	file_logger "github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	testing "testing"
	"time"
)

func TestInitTraceLogger(t *testing.T) {
	// init logger
	var err error
	conf := file_logger.LogConfig{
		Dir:             "../../../tmp",
		FilePrefix:      "trace_log",
		Level:           "debug",
		MaxKeepDays:     2,
		MaxFileNum:      10,
		MaxFileSizeInMB: 1,
		IsCompress:      false,
	}
	err = InitTraceLogger(conf)
	if err != nil {
		t.Error(err)
		return
	}

	manager := NewDefaultTraceLoggerManager()
	err = manager.AutoDelete(5*time.Second, 2*time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	defer manager.CancelAutoDelete() //nolint:errcheck
	err = testFunc(manager, "key1")
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("cache: %+v\n", manager.cache)

	err = testFunc(manager, "key2")
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("cache: %+v\n", manager.cache)
	<-time.After(6 * time.Second)
	t.Logf("cache: %+v\n", manager.cache)
	return
}

func testFunc(manager TraceLoggerManager, key string) error {
	traceLogger := manager.NewTraceLogger()
	defer traceLogger.RollbackTrace() //nolint:errcheck

	traceLogger.Infof("test1")
	traceLogger.Errorf("test2")
	traceLogger.SetKey(key)
	traceLogger.Panicf("test3")
	<-time.After(1 * time.Second)
	traceLogger.Infof("test4")
	traceLogger.Infof("test5")

	err := traceLogger.CommitTrace()
	if err != nil {
		return err
	}
	return nil
}

func sPrintTrace(trace Trace) string {
	str := ""
	for _, x := range trace.logs {
		str += x.String() + "\n"
	}
	return str
}
