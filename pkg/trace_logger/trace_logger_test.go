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

// TODO: file part is not ready

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
)

const (
	FilePrefix = "trace_log"
)

var (
	FilePath = "/tmp"
)

func TestTraceLogger(t *testing.T) {
	// init fileLogger
	var err error

	err = initTestTraceLogger()
	assert.Equal(t, nil, err)

	// start auto delete
	err = AutoDelete(2 * time.Second)
	assert.Equal(t, nil, err)

	key1 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key1)
	err = traceLoggerTest1(t, key1)
	assert.Equal(t, nil, err)

	assert.Condition(t, func() bool {
		_, ok := manager.GetTraceFromCache(key1)
		return ok
	})

	t.Logf("cache: \n%s\n", manager)

	key2 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key2)
	err = traceLoggerTest1(t, key2)
	assert.Equal(t, nil, err)

	assert.Condition(t, func() bool {
		_, ok := manager.GetTraceFromCache(key1)
		return ok
	})
	assert.Condition(t, func() bool {
		_, ok := manager.GetTraceFromCache(key2)
		return ok
	})

	t.Logf("cache: \n%s\n", manager)

	key3 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key3)
	err = traceLoggerTest1(t, key3)
	assert.Equal(t, nil, err)

	assert.Condition(t, func() bool {
		_, ok := manager.GetTraceFromCache(key3)
		return ok
	})

	// must be deleted one
	assert.Condition(t, func() bool {
		_, ok1 := manager.GetTraceFromCache(key1)
		_, ok2 := manager.GetTraceFromCache(key2)
		return !(ok1 && ok2)
	})

	t.Logf("cache: \n%s\n", manager)

	t.Logf("sync all")
	err = manager.SyncAll()
	assert.Equal(t, nil, err)

	t.Logf("wait for 6s")
	<-time.After(6 * time.Second)
	t.Logf("cache: \n%s\n", manager)

	assert.Condition(t, func() bool {
		_, ok1 := manager.GetTraceFromCache(key1)
		_, ok2 := manager.GetTraceFromCache(key2)
		_, ok3 := manager.GetTraceFromCache(key3)
		return !(ok1 && ok2 && ok3)
	})

	// test clear
	t.Logf("clear all")
	err = manager.ClearAll()
	assert.Equal(t, nil, err)
	t.Logf("cache: \n%s\n", manager)

	// load from disk
	t.Logf("load all")
	err = manager.LoadAll(FilePath, FilePrefix)
	assert.Equal(t, nil, err)

	t.Logf("cache: \n%s\n", manager)
	trace, _ := manager.GetTraceFromCache(key3)
	assert.Condition(t, func() bool {
		_, ok1 := manager.GetTraceFromCache(key1)
		_, ok2 := manager.GetTraceFromCache(key2)
		_, ok3 := manager.GetTraceFromCache(key3)
		return ok1 && ok2 && ok3
	})

	// test traceLoggerTest2
	key4 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key4)
	err = traceLoggerTest2(t, key4)
	assert.Equal(t, nil, err)

	t.Logf("key3 %s: %s\n", key3, trace)
}

func initTestTraceLogger() error {
	if err := createTmpDir(); err != nil {
		return err
	}
	conf := TraceLoggerConfig{
		Dir:             FilePath,
		FilePrefix:      FilePrefix,
		Level:           "debug",
		MaxKeepDays:     2,
		MaxFileNum:      10,
		MaxFileSizeInMB: 1,
		IsCompress:      false,
		Timeout:         "2s",
		MaxCacheSize:    2,
	}

	return InitTraceLoggerManager(conf)

}

func createTmpDir() error {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return err
	}
	FilePath = tmpDir
	return nil
}

func traceLoggerTest1(t *testing.T, key string) error {
	tmpKey := uuid.GenerateIDWithLength("tmp", 4)
	Key(tmpKey).Infof("test1")
	Key(tmpKey).Errorf("test2")
	assert.Condition(t, func() bool {
		_, ok := GetTraceFromCache(tmpKey)
		return !ok
	})
	err := UpdateKey(tmpKey, key)
	assert.Equal(t, nil, err)
	Key(key).Warnf("test3")
	assert.Condition(t, func() bool {
		_, ok := GetTraceFromCache(key)
		return ok
	})

	return nil
}

func traceLoggerTest2(t *testing.T, key string) error {
	KeyWithUpdate(key).Infof("test1")
	KeyWithUpdate(key).Errorf("test2")
	assert.Condition(t, func() bool {
		_, ok := GetTraceFromCache(key)
		return ok
	})

	Key(key).Warnf("test3")
	assert.Condition(t, func() bool {
		_, ok := GetTraceFromCache(key)
		return ok
	})
	return nil
}
