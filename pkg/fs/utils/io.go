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
	"io"
	"os"

	log "github.com/sirupsen/logrus"
)

// Readdirnames reads the contents of the directory associated with file
// and returns a slice of up to n names of files in the directory.
//
// The ioutil.ReadDir function will get the directory name and call lstat to get the directory FileInfo.
// but when mount point is not connected, lstat function will return error
func Readdirnames(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Errorf("open path[%s] failed: %v", path, err)
		return []string{}, err
	}
	defer file.Close()

	names, err := file.Readdirnames(-1)
	if err != nil {
		log.Errorf("read directory names in dir[%s] failed: %v", file.Name(), err)
		return []string{}, err
	}
	return names, nil
}

func IsEmptyDir(path string) (bool, error) {
	names, err := Readdirnames(path)
	if err != nil {
		log.Errorf("read dirs in path[%s] failed: %v", path, err)
		return false, err
	}

	if len(names) == 0 {
		return true, nil
	}
	return false, nil
}

func Exist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func Open(path string) (io.ReadCloser, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}
