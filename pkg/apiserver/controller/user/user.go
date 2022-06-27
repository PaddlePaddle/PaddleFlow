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

package user

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/crypto/bcrypt"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	gormErrors "github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type LoginInfo struct {
	UserName string `json:"username"`
	Password string `json:"password"`
}

type UpdateUserArgs struct {
	Password string `json:"password"`
}

type CreateUserResponse struct {
	UserName string `json:"username"`
}

type LoginResponse struct {
	Authorization string `json:"authorization"`
}

type ListUserResponse struct {
	common.MarkerInfo
	Users []model.User `json:"userList"`
}

var ErrMismatchedPassword = errors.New("password mismatched")

func Login(ctx *logger.RequestContext, userName string, password string, passwordEncoded bool) (*model.User, error) {
	ctx.Logging().Debugf("begin verify user. userName:%s ", userName)
	user, err := storage.Auth.GetUserByName(ctx, userName)
	if err != nil {
		ctx.Logging().Errorf("user verify failed. userName: error:%s", err.Error())
		ctx.ErrorCode = common.UserNotExist
		ctx.Logging().Errorf("user verify failed because user not found. userName:%s, error:%s",
			userName, err.Error())
		return nil, errors.New("verify user failed")
	}
	if passwordEncoded {
		if user.UserInfo.Password != password {
			err = ErrMismatchedPassword
		}
	} else {
		err = bcrypt.CompareHashAndPassword([]byte(user.UserInfo.Password), []byte(password))
	}
	if err != nil {
		ctx.ErrorCode = common.AuthFailed
		ctx.Logging().Errorf("user verify failed. error:%s",
			err.Error())
		return nil, errors.New(common.AuthFailed)
	}
	return &user, nil
}

func CreateUser(ctx *logger.RequestContext, userName, password string) (*CreateUserResponse, error) {

	if !schema.CheckReg(userName, common.RegPatternUserName) {
		ctx.Logging().Errorf("create user failed. username not allowed. userName:%v", userName)
		ctx.ErrorCode = common.InvalidNamePattern
		err := common.InvalidNamePatternError(userName, common.ResourceTypeUser, common.RegPatternUserName)
		return nil, err
	}
	ctx.Logging().Debugf("begin create user. new user name:%v ", userName)
	if err := CheckPasswordLever(password); err != nil {
		ctx.Logging().Errorf("create user failed. password is weak. userName:%v", userName)
		ctx.ErrorCode = common.UserPasswordWeak
		return nil, err

	}
	if !common.IsRootUser(ctx.UserName) {
		ctx.Logging().Errorln("create user failed. root is needed.")
		ctx.ErrorCode = common.OnlyRootAllowed
		return nil, errors.New("create user failed")
	}
	var err error
	pd, err := EncodePassWord(password)
	if err != nil {
		ctx.Logging().Errorf("create user failed because encode password failed. new user name:%v ",
			userName)
		ctx.ErrorCode = common.InternalError
		return nil, err
	}
	userInfo := model.UserInfo{
		Name:     userName,
		Password: pd,
	}
	user := model.User{
		UserInfo: userInfo,
	}
	if err := storage.Auth.CreateUser(ctx, &user); err != nil {
		ctx.Logging().Errorln("models create user failed.")
		if gormErrors.GetErrorCode(err) == gormErrors.ErrorKeyIsDuplicated {
			ctx.ErrorCode = common.UserNameDuplicated
		} else {
			ctx.ErrorCode = common.InternalError
		}
		return nil, err
	}
	ctx.Logging().Debugf("create user success. new userName:%s ", user.Name)
	response := &CreateUserResponse{
		UserName: user.Name,
	}

	return response, nil
}

func UpdateUser(ctx *logger.RequestContext, userName, password string) error {
	ctx.Logging().Debugf("begin update user. userName:%s ", userName)
	if err := CheckPasswordLever(password); err != nil {
		ctx.Logging().Errorf("update user failed. password is weak. userName:%v", userName)
		ctx.ErrorCode = common.UserPasswordWeak
		return err

	}
	newPassword, err := EncodePassWord(password)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("update user's password failed. error:%s",
			err.Error())
		return errors.New("update user failed")
	}
	// check user exist
	if _, err := storage.Auth.GetUserByName(ctx, userName); err != nil {
		ctx.ErrorCode = common.UserNotExist
		ctx.Logging().Errorf("update user's password failed. user not exist. userName:%s", ctx.UserName)
		return errors.New("update user failed")
	}
	// regular user can only update his own password
	if !common.IsRootUser(ctx.UserName) && !strings.EqualFold(ctx.UserName, userName) {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorf("update user's password failed. regular user can noly update himselft. userName:%s", ctx.UserName)
		return errors.New("update user failed")
	}

	err = storage.Auth.UpdateUser(ctx, userName, newPassword)
	if err != nil {
		ctx.Logging().Errorf("models update user's password failed. error:%s",
			err.Error())

		ctx.ErrorCode = common.UserNotExist
		return err
	}
	return nil
}

func DeleteUser(ctx *logger.RequestContext, userName string) error {
	ctx.Logging().Debugf("begin delete user. userName:%s ", userName)

	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln("delete user failed, root is needed.")
		return errors.New("delete user failed")
	}

	user, err := storage.Auth.GetUserByName(ctx, userName)
	if err != nil {
		ctx.ErrorCode = common.UserNotExist
		ctx.Logging().Errorf("delete user failed. user:%s not exist", userName)
		return errors.New("delete user failed")
	}

	if common.IsRootUser(user.Name) {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln("delete user failed, delete root.")
		return errors.New("delete user failed")
	}

	if err := storage.Auth.DeleteUser(ctx, userName); err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("models delete user failed. error:%s", err.Error())
		return err
	}
	if err := storage.Auth.DeleteGrantByUserName(ctx, userName); err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("models delete user failed. delete user's grant  error:%s", err.Error())
		return err
	}
	return nil
}

func ListUser(ctx *logger.RequestContext, marker string, maxKeys int) (*ListUserResponse, error) {
	ctx.Logging().Debug("begin list user.")
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln("list user failed. root is needed")
		return nil, errors.New("list user failed")
	}
	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return nil, err
		}
	}
	userList, err := storage.Auth.ListUser(ctx, pk, maxKeys)
	if err != nil {
		ctx.Logging().Errorf("models list user failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
		return nil, err
	}
	listUserResponse := ListUserResponse{}
	listUserResponse.IsTruncated = false
	if len(userList) > 0 {
		queue := userList[len(userList)-1]
		if !IsLastUserPk(ctx, queue.Pk) {
			nextMarker, err := common.EncryptPk(queue.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s] ",
					queue.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return nil, err
			}
			listUserResponse.NextMarker = nextMarker
			listUserResponse.IsTruncated = true
		}
	}
	for _, user := range userList {
		listUserResponse.Users = append(listUserResponse.Users, user)
	}
	return &listUserResponse, nil
}

func EncodePassWord(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

func IsLastUserPk(ctx *logger.RequestContext, pk int64) bool {
	lastQueue, err := storage.Auth.GetLastUser(ctx)
	if err != nil {
		ctx.Logging().Errorf("models get last user failed. error:[%s]",
			err.Error())
	}
	if lastQueue.Pk == pk {
		return true
	}
	return false
}

func GetUserByName(ctx *logger.RequestContext, userName string) (*model.User, error) {
	ctx.Logging().Debug("begin get user.")
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.AccessDenied
		ctx.Logging().Errorln("get user failed. root is needed.")
		return nil, errors.New("get user failed")
	}
	user, err := storage.Auth.GetUserByName(ctx, userName)
	if err != nil {
		ctx.Logging().Errorf("models get user failed. userName: error:%s",
			err.Error())
		ctx.ErrorCode = common.UserNotExist
		return nil, err
	}
	return &user, nil
}

func CheckPasswordLever(ps string) error {
	if len(ps) < 6 {
		return fmt.Errorf("password len is < 6")
	}
	num := `[0-9]{1}`
	az := `[a-z]{1}`
	if b, err := regexp.MatchString(num, ps); !b || err != nil {
		return fmt.Errorf("password need num :%v", err)
	}
	if b, err := regexp.MatchString(az, ps); !b || err != nil {
		return fmt.Errorf("password need a_z :%v", err)
	}
	return nil
}
