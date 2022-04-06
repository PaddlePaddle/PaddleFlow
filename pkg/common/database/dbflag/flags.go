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

package dbflag

import (
	"github.com/spf13/pflag"
	"github.com/urfave/cli/v2"
	"gorm.io/gorm"
)

var DB *gorm.DB

type DatabaseConfig struct {
	Driver                               string `yaml:"driver"`
	Host                                 string `yaml:"host"`
	Port                                 string `yaml:"port"`
	User                                 string `yaml:"user"`
	Password                             string `yaml:"password"`
	Database                             string `yaml:"database"`
	ConnectTimeoutInSeconds              int    `yaml:"connectTimeoutInSeconds,omitempty"`
	LockTimeoutInMilliseconds            int    `yaml:"lockTimeoutInMilliseconds,omitempty"`
	IdleTransactionTimeoutInMilliseconds int    `yaml:"idleTransactionTimeoutInMilliseconds,omitempty"`
	MaxIdleConns                         *int   `yaml:"maxIdleConns,omitempty"`
	MaxOpenConns                         *int   `yaml:"maxOpenConns,omitempty"`
	ConnMaxLifetimeInHours               *int   `yaml:"connMaxLifetimeInHours,omitempty"`
}

func AddFlagSet(fs *pflag.FlagSet, dbConf *DatabaseConfig) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	fs.StringVar(&dbConf.Driver, "db-configPath", dbConf.Driver, "Driver")
	fs.StringVar(&dbConf.Host, "db-host", dbConf.Host, "Host")
	fs.StringVar(&dbConf.Port, "db-port", dbConf.Port, "Port")
	fs.StringVar(&dbConf.User, "db-user", dbConf.User, "User")
	fs.StringVar(&dbConf.Password, "db-password", dbConf.Password, "Password")
	fs.StringVar(&dbConf.Database, "db-database", dbConf.Database, "database")
	fs.IntVar(&dbConf.ConnectTimeoutInSeconds, "db-connect-timeout-in-seconds",
		dbConf.ConnectTimeoutInSeconds, "ConnectTimeoutInSeconds")
	fs.IntVar(&dbConf.LockTimeoutInMilliseconds, "db-lock-timeout-in-milliseconds",
		dbConf.LockTimeoutInMilliseconds, "LockTimeoutInMilliseconds")
	fs.IntVar(&dbConf.IdleTransactionTimeoutInMilliseconds,
		"db-idle-transaction-timeout-in-milliseconds", dbConf.IdleTransactionTimeoutInMilliseconds,
		"IdleTransactionTimeoutInMilliseconds")
}

func DatabaseFlags(dbConf *DatabaseConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "db-configPath",
			Value:       dbConf.Driver,
			Usage:       "driver",
			Destination: &dbConf.Driver,
		},
		&cli.StringFlag{
			Name:        "db-host",
			Value:       dbConf.Host,
			Usage:       "host",
			Destination: &dbConf.Host,
		},
		&cli.StringFlag{
			Name:        "db-port",
			Value:       dbConf.Port,
			Usage:       "port",
			Destination: &dbConf.Port,
		},
		&cli.StringFlag{
			Name:        "db-user",
			Value:       dbConf.User,
			Usage:       "user",
			Destination: &dbConf.User,
		},
		&cli.StringFlag{
			Name:        "db-password",
			Usage:       "password",
			Destination: &dbConf.Password,
		},
		&cli.StringFlag{
			Name:        "db-database",
			Value:       dbConf.Database,
			Usage:       "database",
			Destination: &dbConf.Database,
		},
		&cli.IntFlag{
			Name:        "db-connect-timeout-in-seconds",
			Value:       dbConf.ConnectTimeoutInSeconds,
			Usage:       "db connect timeout in seconds",
			Destination: &dbConf.ConnectTimeoutInSeconds,
		},
		&cli.IntFlag{
			Name:        "db-lock-timeout-in-milliseconds",
			Value:       dbConf.LockTimeoutInMilliseconds,
			Usage:       "db lock timeout in milliseconds",
			Destination: &dbConf.LockTimeoutInMilliseconds,
		},
		&cli.IntFlag{
			Name:        "db-idle-transaction-timeout-in-milliseconds",
			Value:       dbConf.IdleTransactionTimeoutInMilliseconds,
			Usage:       "db idle transaction timeout in milliseconds",
			Destination: &dbConf.IdleTransactionTimeoutInMilliseconds,
		},
	}
}
