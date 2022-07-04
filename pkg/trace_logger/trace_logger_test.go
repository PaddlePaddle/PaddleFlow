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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
)

func TestTraceLogger(t *testing.T) {
	// init logger
	var err error
	err = initTraceLogger()
	if err != nil {
		t.Error(err)
		return
	}

	// start auto delete
	err = AutoDelete(2 * time.Second)
	if err != nil {
		t.Error(err)
		return
	}

	key1 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key1)
	err = testFunc1(t, key1)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Condition(t, func() bool {
		_, ok := manager.GetTraceFromCache(key1)
		return ok
	})

	t.Logf("cache: \n%s\n", manager)

	key2 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key2)
	err = testFunc1(t, key2)
	if err != nil {
		t.Error(err)
		return
	}

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
	err = testFunc1(t, key3)
	if err != nil {
		t.Error(err)
		return
	}

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
	if err != nil {
		t.Error(err)
		return
	}

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
	_ = manager.ClearAll()
	t.Logf("cache: \n%s\n", manager)

	// load from disk
	t.Logf("load all")
	err = manager.LoadAll("./", "trace_log")
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("cache: \n%s\n", manager)
	trace, _ := manager.GetTraceFromCache(key3)
	assert.Condition(t, func() bool {
		_, ok1 := manager.GetTraceFromCache(key1)
		_, ok2 := manager.GetTraceFromCache(key2)
		_, ok3 := manager.GetTraceFromCache(key3)
		return ok1 && ok2 && ok3
	})

	t.Logf("key3 %s: %s\n", key3, trace)

}

func initTraceLogger() error {
	conf := TraceLoggerConfig{
		Dir:             "./",
		FilePrefix:      "trace_log",
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

func testFunc1(t *testing.T, key string) error {
	tmpKey := uuid.GenerateIDWithLength("tmp", 4)
	Key(tmpKey).Infof("test1")
	Key(tmpKey).Errorf("test2")
	assert.Condition(t, func() bool {
		_, ok := GetTraceFromCache(tmpKey)
		return !ok
	})
	err := UpdateKey(tmpKey, key)
	if err != nil {
		return err
	}
	Key(key).Warnf("test3")
	assert.Condition(t, func() bool {
		_, ok := GetTraceFromCache(key)
		return ok
	})

	return nil
}
