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

package utils

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

func TestReaddirnames(t *testing.T) {
	testErr := fmt.Errorf("open TEST: no such file or directory")

	// case1
	var p1 = gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return nil, testErr
	})
	_, err := Readdirnames("TEST")
	assert.Equal(t, testErr.Error(), err.Error())
	defer p1.Reset()
	// case2
	var p2 = gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	defer p2.Reset()
	file := &os.File{}
	var p3 = gomonkey.ApplyMethod(reflect.TypeOf(file), "Readdirnames",
		func(file *os.File, n int) (names []string, err error) {
			return []string{}, testErr
		})
	defer p3.Reset()
	var p4 = gomonkey.ApplyMethod(reflect.TypeOf(file), "Name",
		func(file *os.File) string {
			return "TEST"
		})
	defer p4.Reset()
	_, err = Readdirnames("TEST")
	assert.Equal(t, testErr.Error(), err.Error())
	// case3
	var p5 = gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	defer p5.Reset()

	var p6 = gomonkey.ApplyFunc(Readdirnames,
		func(path string) (names []string, err error) {
			return []string{"test"}, nil
		})
	defer p6.Reset()
	names, err := Readdirnames("TEST")
	assert.Nil(t, err)
	assert.Equal(t, []string{"test"}, names)
}

func Test_IsEmptyDir(t *testing.T) {
	testErr := fmt.Errorf("test err")
	var p1 = gomonkey.ApplyFunc(Readdirnames, func(path string) ([]string, error) {
		return nil, testErr
	})
	defer p1.Reset()
	empty, err := IsEmptyDir("/test")
	assert.Equal(t, false, empty)
	assert.Equal(t, testErr, err)
	var p2 = gomonkey.ApplyFunc(Readdirnames, func(path string) ([]string, error) {
		return []string{}, nil
	})
	defer p2.Reset()
	empty, err = IsEmptyDir("/test")
	assert.Equal(t, true, empty)
	assert.Nil(t, err)

	var p3 = gomonkey.ApplyFunc(Readdirnames, func(path string) ([]string, error) {
		return []string{"1"}, nil
	})
	defer p3.Reset()
	empty, err = IsEmptyDir("/test")
	assert.Equal(t, false, empty)
	assert.Nil(t, err)
}
