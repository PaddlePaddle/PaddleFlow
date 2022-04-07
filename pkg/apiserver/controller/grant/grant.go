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

package grant

import (
	"errors"
	"fmt"

	"gorm.io/gorm"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/uuid"
)

var checkFuncs map[string]func(ctx *logger.RequestContext, ID string) error

type ListGrantResponse struct {
	common.MarkerInfo
	GrantList []models.Grant `json:"grantList"`
}

func checkQueue(ctx *logger.RequestContext, queueName string) error {
	_, err := models.GetQueueByName(database.DB, ctx, queueName)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		return fmt.Errorf("queueName:%s not found", queueName)
	}
	return nil
}

func checkUser(ctx *logger.RequestContext, userName string) error {
	_, err := models.GetUserByName(database.DB, ctx, userName)
	if err != nil {
		ctx.ErrorCode = common.UserNotExist
		return fmt.Errorf("userName:%s not found", userName)
	}
	return nil
}

func init() {
	checkFuncs = make(map[string]func(ctx *logger.RequestContext, resourceID string) error)
	checkFuncs[common.ResourceTypeQueue] = checkQueue
	checkFuncs[common.ResourceTypeUser] = checkUser
}

type CreateGrantResponse struct {
	GrantID string `json:"grantID"`
}

func CreateGrant(ctx *logger.RequestContext, grant *models.Grant) (*CreateGrantResponse, error) {
	ctx.Logging().Debugf("begin create grant. grantInfo: %v.", &grant)
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("create grant failed. root is needed.")
		return nil, errors.New("create grant failed")
	}
	//grant to root is not allowed
	if common.IsRootUser(grant.UserName) {
		ctx.ErrorCode = common.GrantRootActionNotSupport
		ctx.Logging().Errorln("can't grant to admin, root has garnts of all resource.")
		return nil, errors.New("create grant failed")
	}

	//check resouce type
	checkResourceFunc, ok := checkFuncs[grant.ResourceType]
	if !ok {
		ctx.ErrorCode = common.GrantResourceTypeNotFound
		ctx.Logging().Errorln("create grant failed. reourceType not exist.")
		return nil, errors.New("create grant failed")
	}
	//check resource
	if err := checkResourceFunc(ctx, grant.ResourceID); err != nil {
		ctx.Logging().Errorf("create grant failed.%v:%s not exist.", grant.ResourceType, grant.ResourceID)
		return nil, err
	}
	//check user
	if err := checkFuncs[common.ResourceTypeUser](ctx, grant.UserName); err != nil {
		ctx.Logging().Errorf("create grant failed.user:%v not exist.", grant.UserName)
		return nil, err
	}

	//can't grant repeatedlly

	if existgrant, _ := models.GetGrant(database.DB, ctx, grant.UserName, grant.ResourceType, grant.ResourceID); existgrant != nil {
		ctx.ErrorCode = common.GrantAlreadyExist
		ctx.Logging().Errorf("create grant failed.user:[%s] already has the grant of resource[%s].", grant.UserName, grant.ResourceID)
		return nil, errors.New("create grant failed")
	}
	grant.ID = uuid.GenerateID(common.PrefixGrant)
	err := models.CreateGrant(database.DB, ctx, grant)
	if err != nil {
		ctx.Logging().Errorf("create grant failed. error:%s", err.Error())
		if database.GetErrorCode(err) == database.ErrorKeyIsDuplicated {
			ctx.ErrorCode = common.QueueNameDuplicated
		} else {
			ctx.ErrorCode = common.InternalError
		}
		return nil, err
	}
	response := &CreateGrantResponse{
		GrantID: grant.ID,
	}
	return response, nil
}

func DeleteGrant(ctx *logger.RequestContext, userName, resourceID, resourceType string) error {
	ctx.Logging().Debugf("begin delete grant. userName:%v, resourceID:%v.", userName, resourceID)
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("delete grant failed. admin is needed.")
		return errors.New("delete grant failed")
	}
	//delete root's grant is not allowed
	if common.IsRootUser(userName) {
		ctx.ErrorCode = common.GrantRootActionNotSupport
		ctx.Logging().Errorln("can's delete root's grants, root has garnts of all resource.")
		return errors.New("delete grant failed")
	}
	checkResourceFunc, ok := checkFuncs[resourceType]
	if !ok {
		ctx.ErrorCode = common.GrantResourceTypeNotFound
		ctx.Logging().Errorln("delete grant failed. reourceType not exist.")
		return errors.New("delete grant failed")
	}
	//check resource
	if err := checkResourceFunc(ctx, resourceID); err != nil {
		ctx.Logging().Errorf("delete grant failed.%v:%s not exist.", resourceType, resourceID)
		return err
	}
	//check user
	if err := checkFuncs[common.ResourceTypeUser](ctx, userName); err != nil {
		ctx.Logging().Errorf("delete grant failed. user:%v not exist.", userName)
		return err
	}
	//check if grant exist
	if _, err := models.GetGrant(database.DB, ctx, userName, resourceType, resourceID); err != nil {
		ctx.ErrorCode = common.GrantNotFound
		ctx.Logging().Errorf("delete grant failed. grant with userName:%v and resourceID:%v not exist.", userName, resourceID)
		return err
	}

	if err := models.DeleteGrant(database.DB, ctx, userName, resourceType, resourceID); err != nil {
		ctx.ErrorCode = common.GrantNotFound
		ctx.Logging().Errorf("delete grant failed. userName:%v, resourceID:%v",
			userName, resourceID)
		return err
	}
	return nil
}

func ListGrant(ctx *logger.RequestContext, marker string, maxKeys int, userName string) (ListGrantResponse, error) {

	ctx.Logging().Debugf("begin list grants. user:[%s].", userName)

	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorf("list user[%s]'s grants failed. root is needed.", userName)
		return ListGrantResponse{}, errors.New("list grants failed")
	}
	listGrantResponse := ListGrantResponse{}
	listGrantResponse.IsTruncated = false
	listGrantResponse.GrantList = []models.Grant{}

	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return listGrantResponse, err
		}
	}

	grantList, err := models.ListGrant(database.DB, ctx, pk, maxKeys, userName)
	if err != nil {
		ctx.Logging().Errorf("models list grant failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
	}

	// get next marker
	if len(grantList) > 0 {
		grant := grantList[len(grantList)-1]
		if !IsLastGrantPk(database.DB, ctx, grant.Pk) {
			nextMarker, err := common.EncryptPk(grant.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					grant.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return listGrantResponse, err
			}
			listGrantResponse.NextMarker = nextMarker
			listGrantResponse.IsTruncated = true
		}
	}
	listGrantResponse.MaxKeys = maxKeys
	for _, grant := range grantList {
		listGrantResponse.GrantList = append(listGrantResponse.GrantList, grant)
	}
	return listGrantResponse, nil
}

func IsLastGrantPk(db *gorm.DB, ctx *logger.RequestContext, pk int64) bool {
	lastGrant, err := models.GetLastGrant(db, ctx)
	if err != nil {
		ctx.Logging().Errorf("get last grant failed. error:[%s]", err.Error())
	}
	if lastGrant.Pk == pk {
		return true
	}
	return false
}
