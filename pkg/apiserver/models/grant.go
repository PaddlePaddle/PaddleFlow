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

package models

import (
	"time"

	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
)

type Grant struct {
	Pk           int64          `json:"-" gorm:"primaryKey;autoIncrement"`
	ID           string         `json:"grantID" gorm:"uniqueIndex"`
	UserName     string         `json:"userName"`
	ResourceType string         `json:"resourceType"`
	ResourceID   string         `json:"resourceID"`
	CreatedAt    time.Time      `json:"createTime"`
	UpdatedAt    time.Time      `json:"updateTime,omitempty"`
	DeletedAt    gorm.DeletedAt `json:"-" gorm:"index"`
}

func (Grant) TableName() string {
	return "grant"
}

func CreateGrant(ctx *logger.RequestContext, grant *Grant) error {
	ctx.Logging().Debugf("model begin create grant: %v", grant)
	grant.ID = uuid.GenerateID(common.PrefixGrant)
	tx := database.DB.Table("grant").Create(grant)
	if tx.Error != nil {
		ctx.Logging().Errorf("create grant failed. grant:%v, error:%s",
			grant, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func DeleteGrant(ctx *logger.RequestContext, userName, resourceType, resourceID string) error {
	ctx.Logging().Debugf("model begin delete grant. userName:%s, resourceID:%s ", userName, resourceID)
	tx := database.DB.Unscoped().Table("grant").Where("user_name = ? and resource_type = ? and resource_id = ?", userName, resourceType, resourceID).Delete(&Grant{})
	if tx.Error != nil {
		ctx.Logging().Errorf("delete grant failed. userName:%v, resourceID:%s. error:%s",
			userName, resourceID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func GetGrant(ctx *logger.RequestContext, userName, resourceType, resourceID string) (*Grant, error) {
	ctx.Logging().Debugf("model begin get grant. userName:%s, resourceID:%s ", userName, resourceID)
	var grant Grant
	tx := database.DB.Table("grant").Where("user_name = ? and resource_id = ? and resource_type = ?", userName, resourceID, resourceType).First(&grant)
	if tx.Error != nil {
		ctx.Logging().Errorf("model get grant failed. userName:%v, resourceID:%s. error:%s.",
			userName, resourceID, tx.Error.Error())
		return nil, tx.Error
	}
	return &grant, nil
}

func HasAccessToResource(ctx *logger.RequestContext, resourceType string, resourceID string) bool {
	if common.IsRootUser(ctx.UserName) {
		return true
	}

	var num int64
	tx := database.DB.Table("grant").Where("user_name = ? and resource_type = ? and resource_id = ?",
		ctx.UserName, resourceType, resourceID).Count(&num)
	if tx.Error != nil {
		ctx.Logging().Errorf("deny access to resourceID[%s] resourceType[%s].", resourceID, resourceType)
		return false
	}
	if num > 0 {
		return true
	}
	return false
}

func DeleteGrantByUserName(ctx *logger.RequestContext, userName string) error {
	ctx.Logging().Debugf("model begin delete grant by userName. userName:%s. ", userName)
	err := database.DB.Unscoped().Table("grant").Where("user_name = ?", userName).Delete(&Grant{}).Error
	if err != nil {
		ctx.Logging().Debugf("model delete grant by userName failed. userName:%s, error: %s. ", userName, err.Error())
		return err
	}
	return nil
}

func DeleteGrantByResourceID(ctx *logger.RequestContext, resourceID string) error {
	ctx.Logging().Debugf("model begin delete grant by resourceID. resourceID:%s. ", resourceID)
	err := database.DB.Table("grant").Unscoped().Where("resource_id = ?", resourceID).Delete(&Grant{}).Error
	if err != nil {
		ctx.Logging().Debugf("model delete grant by resourceID failed. resourceID:%s, error: %s. ", resourceID, err.Error())
		return err
	}
	return nil
}

func ListGrant(ctx *logger.RequestContext, pk int64, maxKeys int, userName string) ([]Grant, error) {
	ctx.Logging().Debugf("model begin list grants by userName. userName:%s. ", userName)
	query := database.DB.Table("grant")
	query.Where("pk > ?", pk)
	if maxKeys > 0 {
		query.Limit(maxKeys)
	}
	if userName != "" {
		query.Where("user_name = ?", userName)
	}
	var grants []Grant

	if err := query.Find(&grants).Error; err != nil {
		ctx.Logging().Errorf("model list grant failed. userName:[%s]. error:%s.",
			userName, err.Error())
		return nil, err
	}
	return grants, nil
}

func GetLastGrant(ctx *logger.RequestContext) (Grant, error) {
	ctx.Logging().Debugf("get last grant.")
	grant := Grant{}
	tx := database.DB.Table("grant").Last(&grant)
	if tx.Error != nil {
		ctx.Logging().Errorf("get last grant failed. error:%s", tx.Error.Error())
		return Grant{}, tx.Error
	}
	return grant, nil
}
