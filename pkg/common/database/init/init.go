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

package init

import (
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
)

// data init for sqllite
const (
	dsn              = "file:paddleflow.db?cache=shared&mode=rwc"
	rootUserName     = "root"
	rootUserPassword = "$2a$10$1qdSQN5wMl3FtXoxw7mKpuxBqIuP0eYXTBM9CBn5H4KubM/g5Hrb6%"
)

func InitDatabase(dbConf *config.DatabaseConfig, gormConf *gorm.Config, logLevel string) (*gorm.DB, error) {
	if gormConf == nil {
		gormConf = &gorm.Config{
			NamingStrategy: schema.NamingStrategy{
				TablePrefix:   "",
				SingularTable: true,
			},
		}
	}

	gormLogger := logger.Default
	if level, err := log.ParseLevel(logLevel); err != nil {
		log.Warningf("Parse log level error[%s], using logger.Default as gormLogger.", err.Error())
	} else if level == log.DebugLevel {
		gormLogger = gormLogger.LogMode(logger.Info)
	}
	gormConf.Logger = gormLogger

	var db *gorm.DB
	if strings.EqualFold(dbConf.Driver, "mysql") {
		db = initMysqlDB(dbConf, gormConf)
	} else {
		// 若配置文件没有设置，则默认使用SQLLite
		db = initSQLiteDB(dbConf, gormConf)
	}

	if db == nil {
		panic(fmt.Errorf("Init database db error\n"))
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Get db.DB error[%s]", err.Error())
		return nil, err
	}

	if dbConf.MaxIdleConns == nil {
		dbConf.MaxIdleConns = new(int)
		*dbConf.MaxIdleConns = 5
	}
	sqlDB.SetMaxIdleConns(*dbConf.MaxIdleConns)

	if dbConf.MaxOpenConns == nil {
		dbConf.MaxOpenConns = new(int)
		*dbConf.MaxOpenConns = 10
	}
	sqlDB.SetMaxOpenConns(*dbConf.MaxOpenConns)

	if dbConf.ConnMaxLifetimeInHours == nil {
		dbConf.ConnMaxLifetimeInHours = new(int)
		*dbConf.ConnMaxLifetimeInHours = 1
	}
	sqlDB.SetConnMaxLifetime(time.Hour * time.Duration(*dbConf.ConnMaxLifetimeInHours))
	log.Debugf("InitDatabase success.dbConf:%v", dbConf)
	return db, nil
}

func initSQLiteDB(dbConf *config.DatabaseConfig, gormConf *gorm.Config) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(dsn), gormConf)
	if err != nil {
		log.Fatalf("initSQLiteDB error[%s]", err.Error())
		return nil
	}

	// Create tables
	db.AutoMigrate(
		&models.Pipeline{},
		&models.RunCache{},
		&models.ArtifactEvent{},
		&models.User{},
		&models.Run{},
		&models.Queue{},
		&models.Flavour{},
		&models.Grant{},
		&models.Job{},
		&models.ClusterInfo{},
		&models.Image{},
		&models.FileSystem{},
		&models.Link{},
		&models.RunJob{},
	)
	// init root user to db, can not be modified by config file currently
	rootUser := models.User{
		UserInfo: models.UserInfo{
			Name:     rootUserName,
			Password: rootUserPassword,
		},
	}

	tx := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"password"}),
	}).Create(&rootUser)
	if tx.Error != nil {
		log.Fatalf("init sqllite db error[%s]", tx.Error)
		return nil
	}
	log.Debugf("init sqlite DB success")
	return db
}

func initMysqlDB(dbConf *config.DatabaseConfig, gormConf *gorm.Config) *gorm.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local",
		dbConf.User, dbConf.Password, dbConf.Host, dbConf.Port, dbConf.Database)
	db, err := gorm.Open(mysql.Open(dsn), gormConf)
	if err != nil {
		log.Fatalf("initMysqlDB error[%s]", err.Error())
		return nil
	}
	log.Debugf("init mysql DB success")
	return db
}
