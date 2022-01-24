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

package database

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"gorm.io/gorm"

	"paddleflow/pkg/common/config"
)

type GormErr struct {
	Number  int    `json:"Number"`
	Message string `json:"Message"`
}

var DB *gorm.DB

func AddFlagSet(fs *pflag.FlagSet, databaseConfig *config.DatabaseConfig) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	fs.StringVar(&databaseConfig.Driver, "db-configPath", databaseConfig.Driver, "Driver")
	fs.StringVar(&databaseConfig.Host, "db-host", databaseConfig.Host, "Host")
	fs.StringVar(&databaseConfig.Port, "db-port", databaseConfig.Port, "Port")
	fs.StringVar(&databaseConfig.User, "db-user", databaseConfig.User, "User")
	fs.StringVar(&databaseConfig.Password, "db-password", databaseConfig.Password, "Password")
	fs.StringVar(&databaseConfig.Database, "db-database", databaseConfig.Database, "database")
	fs.IntVar(&databaseConfig.ConnectTimeoutInSeconds, "db-connect-timeout-in-seconds",
		databaseConfig.ConnectTimeoutInSeconds, "ConnectTimeoutInSeconds")
	fs.IntVar(&databaseConfig.LockTimeoutInMilliseconds, "db-lock-timeout-in-milliseconds",
		databaseConfig.LockTimeoutInMilliseconds, "LockTimeoutInMilliseconds")
	fs.IntVar(&databaseConfig.IdleTransactionTimeoutInMilliseconds,
		"db-idle-transaction-timeout-in-milliseconds", databaseConfig.IdleTransactionTimeoutInMilliseconds,
		"IdleTransactionTimeoutInMilliseconds")
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
