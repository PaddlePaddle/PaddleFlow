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

package models

import (
	"time"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

const (
	ROOT = "root"
)

type UserInfo struct {
	Name     string `gorm:"uniqueIndex" json:"name"`
	Password string `json:"-"`
}

type User struct {
	Pk        int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	CreatedAt time.Time      `json:"createTime"`
	UpdatedAt time.Time      `json:"-"`
	DeletedAt gorm.DeletedAt `json:"-"`
	UserInfo  `gorm:"embedded"`
}

func (User) TableName() string {
	return "user"
}

func CreateUser(ctx *logger.RequestContext, user *User) error {
	ctx.Logging().Debugf("model begin create user. username:%s ", user.Name)
	tx := database.DB.Table("user").Create(user)
	if tx.Error != nil {
		ctx.Logging().Errorf("model create user failed. user:%v, error:%s ",
			&user, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func UpdateUser(ctx *logger.RequestContext, userName, password string) error {
	ctx.Logging().Debugf("model update user's password, userName:%v.", userName)
	err := database.DB.Table("user").Where("name = ?", userName).UpdateColumn("password", password).Error
	if err != nil {
		ctx.Logging().Errorf("model update password failed . userName:%v, error:%s ",
			userName, err)
	}
	return err
}

func ListUser(ctx *logger.RequestContext, pk int64, maxKey int) ([]User, error) {
	ctx.Logging().Debugf("model begin list user.")
	var userList []User
	query := database.DB.Where(&User{})
	query.Where("name != ?", ROOT)
	query.Where("pk > ?", pk)
	if maxKey > 0 {
		query.Limit(maxKey)
	}
	err := query.Find(&userList).Error
	if err != nil {
		ctx.Logging().Errorf("list user failed. error : %s ", err.Error())
		return nil, err
	}
	return userList, nil
}

func DeleteUser(ctx *logger.RequestContext, userName string) error {
	ctx.Logging().Debugf("model begin delete user. userName:%s", userName)
	tx := database.DB.Table("user").Unscoped().Where("name = ?", userName).Delete(&User{})
	if tx.Error != nil {
		ctx.Logging().Errorf("model delete user failed. userName:%s, error:%s",
			userName, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func GetUserByName(ctx *logger.RequestContext, userName string) (User, error) {
	ctx.Logging().Debugf("model begin get user by name. userName:%s", userName)
	var user User
	tx := database.DB.Table("user").Where("name = ?", userName).First(&user)
	if tx.Error != nil {
		ctx.Logging().Errorf("get user failed. userName:%s, error:%s", userName, tx.Error.Error())
		return User{}, tx.Error
	}
	return user, nil
}

func GetLastUser(ctx *logger.RequestContext) (User, error) {
	ctx.Logging().Debugf("model get last user. ")
	queue := User{}
	tx := database.DB.Table("user").Last(&queue)
	if tx.Error != nil {
		ctx.Logging().Errorf("get last user failed. error:%s", tx.Error.Error())
		return User{}, tx.Error
	}
	return queue, nil
}
