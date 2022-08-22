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

package storage

import (
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type AuthStore struct {
	db *gorm.DB
}

func newAuthStore(db *gorm.DB) *AuthStore {
	return &AuthStore{db: db}
}

// ============================================================= table user ============================================================= //

func (as *AuthStore) CreateUser(ctx *logger.RequestContext, user *model.User) error {
	ctx.Logging().Debugf("model begin create user. username:%s ", user.Name)
	tx := as.db.Model(&model.User{}).Create(user)
	if tx.Error != nil {
		ctx.Logging().Errorf("model create user failed. user:%v, error:%s ",
			&user, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (as *AuthStore) UpdateUser(ctx *logger.RequestContext, userName, password string) error {
	ctx.Logging().Debugf("model update user's password, userName:%v.", userName)
	err := as.db.Model(&model.User{}).Where("name = ?", userName).UpdateColumn("password", password).Error
	if err != nil {
		ctx.Logging().Errorf("model update password failed . userName:%v, error:%s ",
			userName, err)
	}
	return err
}

func (as *AuthStore) ListUser(ctx *logger.RequestContext, pk int64, maxKey int) ([]model.User, error) {
	ctx.Logging().Debugf("model begin list user.")
	var userList []model.User
	query := DB.Where(&model.User{})
	query.Where("name != ?", UserROOT)
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

func (as *AuthStore) DeleteUser(ctx *logger.RequestContext, userName string) error {
	ctx.Logging().Debugf("model begin delete user. userName:%s", userName)
	tx := as.db.Model(&model.User{}).Unscoped().Where("name = ?", userName).Delete(&model.User{})
	if tx.Error != nil {
		ctx.Logging().Errorf("model delete user failed. userName:%s, error:%s",
			userName, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (as *AuthStore) GetUserByName(ctx *logger.RequestContext, userName string) (model.User, error) {
	ctx.Logging().Debugf("model begin get user by name. userName:%s", userName)
	var user model.User
	tx := as.db.Model(&model.User{}).Where("name = ?", userName).First(&user)
	if tx.Error != nil {
		ctx.Logging().Errorf("get user failed. userName:%s, error:%s", userName, tx.Error.Error())
		return model.User{}, tx.Error
	}
	return user, nil
}

func (as *AuthStore) GetLastUser(ctx *logger.RequestContext) (model.User, error) {
	ctx.Logging().Debugf("model get last user. ")
	queue := model.User{}
	tx := as.db.Model(&model.User{}).Last(&queue)
	if tx.Error != nil {
		ctx.Logging().Errorf("get last user failed. error:%s", tx.Error.Error())
		return model.User{}, tx.Error
	}
	return queue, nil
}

// ============================================================= table grant ============================================================= //

func (as *AuthStore) CreateGrant(ctx *logger.RequestContext, grant *model.Grant) error {
	ctx.Logging().Debugf("model begin create grant: %v", grant)
	grant.ID = uuid.GenerateID(common.PrefixGrant)
	tx := as.db.Model(&model.Grant{}).Create(grant)
	if tx.Error != nil {
		ctx.Logging().Errorf("create grant failed. grant:%v, error:%s",
			grant, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (as *AuthStore) DeleteGrant(ctx *logger.RequestContext, userName, resourceType, resourceID string) error {
	ctx.Logging().Debugf("model begin delete grant. userName:%s, resourceID:%s ", userName, resourceID)
	tx := as.db.Unscoped().Table("grant").Where("user_name = ? and resource_type = ? and resource_id = ?", userName, resourceType, resourceID).Delete(&model.Grant{})
	if tx.Error != nil {
		ctx.Logging().Errorf("delete grant failed. userName:%v, resourceID:%s. error:%s",
			userName, resourceID, tx.Error.Error())
		return tx.Error
	}
	return nil
}

func (as *AuthStore) GetGrant(ctx *logger.RequestContext, userName, resourceType, resourceID string) (*model.Grant, error) {
	ctx.Logging().Debugf("model begin get grant. userName:%s, resourceID:%s ", userName, resourceID)
	var grant model.Grant
	tx := as.db.Model(&model.Grant{}).Where("user_name = ? and resource_id = ? and resource_type = ?", userName, resourceID, resourceType).First(&grant)
	if tx.Error != nil {
		ctx.Logging().Errorf("model get grant failed. userName:%v, resourceID:%s. error:%s.",
			userName, resourceID, tx.Error.Error())
		return nil, tx.Error
	}
	return &grant, nil
}

func (as *AuthStore) HasAccessToResource(ctx *logger.RequestContext, resourceType string, resourceID string) bool {
	if common.IsRootUser(ctx.UserName) {
		return true
	}

	var num int64
	tx := as.db.Model(&model.Grant{}).Where("user_name = ? and resource_type = ? and resource_id = ?",
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

func (as *AuthStore) DeleteGrantByUserName(ctx *logger.RequestContext, userName string) error {
	ctx.Logging().Debugf("model begin delete grant by userName. userName:%s. ", userName)
	err := as.db.Unscoped().Table("grant").Where("user_name = ?", userName).Delete(&model.Grant{}).Error
	if err != nil {
		ctx.Logging().Debugf("model delete grant by userName failed. userName:%s, error: %s. ", userName, err.Error())
		return err
	}
	return nil
}

func (as *AuthStore) DeleteGrantByResourceID(ctx *logger.RequestContext, resourceID string) error {
	ctx.Logging().Debugf("model begin delete grant by resourceID. resourceID:%s. ", resourceID)
	err := as.db.Model(&model.Grant{}).Unscoped().Where("resource_id = ?", resourceID).Delete(&model.Grant{}).Error
	if err != nil {
		ctx.Logging().Debugf("model delete grant by resourceID failed. resourceID:%s, error: %s. ", resourceID, err.Error())
		return err
	}
	return nil
}

func (as *AuthStore) ListGrant(ctx *logger.RequestContext, pk int64, maxKeys int, userName string) ([]model.Grant, error) {
	ctx.Logging().Debugf("model begin list grants by userName. userName:%s. ", userName)
	query := as.db.Model(&model.Grant{})
	query.Where("pk > ?", pk)
	if maxKeys > 0 {
		query.Limit(maxKeys)
	}
	if userName != "" {
		query.Where("user_name = ?", userName)
	}
	var grants []model.Grant

	if err := query.Find(&grants).Error; err != nil {
		ctx.Logging().Errorf("model list grant failed. userName:[%s]. error:%s.",
			userName, err.Error())
		return nil, err
	}
	return grants, nil
}

func (as *AuthStore) GetLastGrant(ctx *logger.RequestContext) (model.Grant, error) {
	ctx.Logging().Debugf("get last grant.")
	grant := model.Grant{}
	tx := as.db.Model(&model.Grant{}).Last(&grant)
	if tx.Error != nil {
		ctx.Logging().Errorf("get last grant failed. error:%s", tx.Error.Error())
		return model.Grant{}, tx.Error
	}
	return grant, nil
}
