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

package common

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	RegPatternQueueName    = "^[a-z0-9][a-z0-9-]{0,8}[a-z0-9]$"
	RegPatternUserName     = "^[A-Za-z0-9-]{4,16}$"
	RegPatternRunName      = "^[A-Za-z0-9_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$"
	RegPatternResource     = "^[1-9][0-9]*([numkMGTPE]|Ki|Mi|Gi|Ti|Pi|Ei)?$"
	RegPatternPipelineName = "^[A-Za-z0-9_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$"
	RegPatternClusterName  = "^[A-Za-z0-9_][A-Za-z0-9-_]{0,253}[A-Za-z0-9_]$"
)

func IsRootUser(userName string) bool {
	return strings.EqualFold(userName, "root")
}

func IsValidQueueStatus(status string) bool {
	if strings.EqualFold(status, StatusQueueCreating) || strings.EqualFold(status, StatusQueueOpen) ||
		strings.EqualFold(status, StatusQueueClosing) || strings.EqualFold(status, StatusQueueClosed) {
		return true
	}
	log.Errorf("Not valid queue status. status:%s", status)
	return false
}

// check string is slice or not
func StringInSlice(s string, strSlice []string) bool {
	for _, str := range strSlice {
		if s == str {
			return true
		}
	}
	return false
}

func RemoveDuplicateStr(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}
	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

func SplitString(str, sep string) []string {
	var result []string
	strList := strings.Split(str, sep)

	for _, s := range strList {
		result = append(result, strings.TrimSpace(s))
	}
	return result
}
