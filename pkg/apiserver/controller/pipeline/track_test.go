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

package pipeline

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func printNum(done chan bool, n int) {
	for i := 0; i < 10; i++ {
		fmt.Printf("===== %d ------ : %d\n", n, i)
	}
	done <- true
}

var f func(done chan bool, n int)

func TestFuncVarPassByValue(t *testing.T) {
	// func with func var arg, no need to worry about multi-threading problem
	var chann = make(chan bool)
	f = printNum
	for i := 30; i < 40; i++ {
		go f(chann, i)
	}
	<-chann
	println("DONE")
}

func TestLogCache(t *testing.T) {
	driver.InitMockDB()
	req := schema.LogRunCacheRequest{
		FirstFp:     "ddddd",
		SecondFp:    "ddddd",
		RunID:       "ddddd",
		Step:        "ddddd",
		FsID:        "ddddd",
		FsName:      "ddddd",
		UserName:    "ddddd",
		Source:      "ddddd",
		ExpiredTime: "ddddd",
		Strategy:    "ddddd",
	}
	cacheID, err := LogCache(req)
	assert.Nil(t, err)
	assert.True(t, strings.Contains(cacheID, "cch-"))
}
