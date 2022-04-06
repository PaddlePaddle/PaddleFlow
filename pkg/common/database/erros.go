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

package database

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

const (
	ErrorKeyIsDuplicated = "DBKeyIsDuplicated" // 数据库key重复
	ErrorUnknown         = "UnknownError"      // 数据库错误
	ErrorRecordNotFound  = "RecordNotFound"    //记录不存在
)

type GormErr struct {
	Number  int    `json:"Number"`
	Message string `json:"Message"`
}

func GetErrorCode(err error) string {
	byteErr, _ := json.Marshal(err)
	var gormErr GormErr
	json.Unmarshal(byteErr, &gormErr)
	switch gormErr.Number {
	case 1062:
		log.Errorf("database key is duplicated. err:%s", gormErr.Message)
		return ErrorKeyIsDuplicated
	case 1032:
		log.Errorf("database record not found. err:%s", gormErr.Message)
		return ErrorRecordNotFound
	}
	return ErrorUnknown
}
