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
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	testing "testing"
	"time"
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
	err = testFunc1(key1)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("cache: %s\n", manager)

	key2 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key2)
	err = testFunc1(key2)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("cache: %s\n", manager)

	key3 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key3)
	err = testFunc1(key3)
	if err != nil {
		t.Error(err)
		return
	}

	key4 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key4)
	err = testFunc1(key4)
	if err != nil {
		t.Error(err)
		return
	}

	key5 := uuid.GenerateIDWithLength("key", 4)
	t.Logf("log key %s", key5)
	err = testFunc1(key5)
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("cache: %s\n", manager)

	t.Logf("sync all")
	err = manager.SyncAll()
	if err != nil {
		t.Error(err)
		return
	}

	t.Logf("wait for 6s")
	<-time.After(6 * time.Second)
	t.Logf("cache: %s\n", manager)

	t.Logf("sync all")
	err = manager.SyncAll()
	if err != nil {
		t.Error(err)
		return
	}

	// test clear
	t.Logf("clear all")
	_ = manager.ClearAll()
	t.Logf("cache: %s\n", manager)

	// load from disk
	t.Logf("load all")
	err = manager.LoadAll("/Users/alex/BAIDU/PaddleFlow/tmp")
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("cache: %s\n", manager)

	/*
		logger.key("key1").logf("test1")

		logger.update("key1", "key2")


		logger.key("key1").logf("test1")
		logger.logf("key1", "test1")

		logger.key("key2").logf("test1")
		logger.key("key2").logf("test1")
		logger.key("key2").logf("test1")
		logger.commit("key1")
	*/

	return
}

func initTraceLogger() error {
	conf := TraceLoggerConfig{
		Dir:             "/Users/alex/BAIDU/PaddleFlow/tmp",
		FilePrefix:      "trace_log",
		Level:           "debug",
		MaxKeepDays:     2,
		MaxFileNum:      10,
		MaxFileSizeInMB: 1,
		IsCompress:      false,
		Timeout:         time.Second * 2,
		MaxCacheSize:    3,
	}

	return InitTraceLogger(conf)

}
func test() {
	Key("key1").Infof("test1")
	UpdateKey("key1", "key2")
	Key("key2").Warnf("test3")

	//cache := gcache.New(10000).LRU().Expiration(time.Second * 2).Build()
}

func testFunc1(key string) error {
	tmpKey := uuid.GenerateIDWithLength("tmp", 4)
	Key(tmpKey).Infof("test1")
	Key(tmpKey).Errorf("test2")
	err := UpdateKey(tmpKey, key)
	if err != nil {
		return err
	}
	Key(key).Warnf("test3")
	<-time.After(2 * time.Second)
	Key(key).Infof("test4")
	Key(key).Infof("test5")
	Key(key).Infof("test6")
	Key(key).Infof("test7")
	Key(key).Infof("test8")
	Key(key).Infof("test9")
	Key(key).Infof("test10")
	Key(key).Infof("test11")
	Key(key).Infof("test12")
	Key(key).Infof("test13")
	Key(key).Infof("test14")
	return nil
}

func testFunc2(key string) error {
	tmpKey := uuid.GenerateIDWithLength("tmp", 4)
	Key(tmpKey).Infof("tmp test1")
	Key(tmpKey).Errorf("tmp test2")
	Key(tmpKey).Errorf("tmp test3")
	return nil
}
