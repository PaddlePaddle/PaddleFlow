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

package queue

import (
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	vcclientset "volcano.sh/apis/pkg/client/clientset/versioned"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

const defaultQueueName = "default"

type CreateQueueResponse struct {
	QueueName string `json:"name"`
}

type ListQueueResponse struct {
	common.MarkerInfo
	QueueList []models.Queue `json:"queueList"`
}

var GlobalVCQueue *VCQueue

func Init(client *vcclientset.Clientset) {
	GlobalVCQueue = NewVCQueue(client)
}

func ListQueue(ctx *logger.RequestContext, marker string, maxKeys int, name string) (ListQueueResponse, error) {
	ctx.Logging().Debugf("begin list queue.")
	listQueueResponse := ListQueueResponse{}
	listQueueResponse.IsTruncated = false
	listQueueResponse.QueueList = []models.Queue{}

	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return listQueueResponse, err
		}
	}

	queueList, err := models.ListQueue(ctx, pk, maxKeys, name)
	if err != nil {
		ctx.Logging().Errorf("models list queue failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
	}

	// get next marker
	if len(queueList) > 0 {
		queue := queueList[len(queueList)-1]
		if !IsLastQueuePk(ctx, queue.Pk) {
			nextMarker, err := common.EncryptPk(queue.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s]",
					queue.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return listQueueResponse, err
			}
			listQueueResponse.NextMarker = nextMarker
			listQueueResponse.IsTruncated = true
		}
	}

	listQueueResponse.MaxKeys = maxKeys
	for _, queue := range queueList {
		listQueueResponse.QueueList = append(listQueueResponse.QueueList, queue)
	}
	return listQueueResponse, nil
}

func IsLastQueuePk(ctx *logger.RequestContext, pk int64) bool {
	lastQueue, err := models.GetLastQueue(ctx)
	if err != nil {
		ctx.Logging().Errorf("get last queue failed. error:[%s]", err.Error())
	}
	if lastQueue.Pk == pk {
		return true
	}
	return false
}

func CreateQueue(ctx *logger.RequestContext, queueInfo *models.QueueInfo) (CreateQueueResponse, error) {
	ctx.Logging().Debugf("begin create queue. queueInfo:%s", config.PrettyFormat(queueInfo))
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("create queue failed. error: admin is needed.")
		return CreateQueueResponse{}, errors.New("create queue failed")
	}

	if queueInfo.Name == "" {
		ctx.ErrorCode = common.QueueNameNotFound
		ctx.Logging().Errorln("create queue failed. error: queueName is not found.")
		return CreateQueueResponse{}, errors.New("queueName is not found.")
	}

	// TODO(liuxiaopeng04)：这里的逻辑先注释掉，后续这块儿需要再改下
	// if queueInfo.ClusterName == "" {
	// 	ctx.ErrorCode = common.ClusterNameNotFound
	// 	ctx.Logging().Errorln("create queue failed. error: clusterName not found.")
	// 	return CreateQueueResponse{}, errors.New("clusterName is not found")
	// }

	if queueInfo.Namespace == "" {
		ctx.ErrorCode = common.NamespaceNotFound
		ctx.Logging().Errorln("create queue failed. error: namespace is not found.")
		return CreateQueueResponse{}, errors.New("namespace is not found")
	}

	if !schema.CheckReg(queueInfo.Name, common.RegPatternQueueName) {
		ctx.ErrorCode = common.InvalidNamePattern
		err := common.InvalidNamePatternError(queueInfo.Name, common.ResourceTypeQueue, common.RegPatternQueueName)
		log.Errorf("CreateQueue failed. err: %v.", err)
		return CreateQueueResponse{}, err
	}

	exist := strings.EqualFold(queueInfo.Name, defaultQueueName) || models.IsQueueExist(ctx, queueInfo.Name)
	if exist {
		ctx.Logging().Errorf("GlobalVCQueue create queue failed. queueName[%s] exist.", queueInfo.Name)
		ctx.ErrorCode = common.QueueNameDuplicated
		return CreateQueueResponse{}, errors.New("queue name duplicated")
	}

	// check queue resource
	resourceInfo := schema.ResourceInfo{
		Cpu:             queueInfo.Cpu,
		Mem:             queueInfo.Mem,
		ScalarResources: make(map[v1.ResourceName]string),
	}
	for k, v := range queueInfo.ScalarResources {
		resourceInfo.ScalarResources[k] = v
	}
	if err := schema.ValidateResourceInfo(resourceInfo, config.GlobalServerConfig.Job.ScalarResourceArray); err != nil {
		ctx.Logging().Errorf("create queue failed. error: %s", err.Error())
		return CreateQueueResponse{}, err
	}

	queue := models.Queue{QueueInfo: *queueInfo, Status: common.StatusQueueCreating}
	err := models.CreateQueue(ctx, &queue)
	if err != nil {
		ctx.Logging().Errorf("create queue failed. error:%s", err.Error())
		if database.GetErrorCode(err) == database.ErrorKeyIsDuplicated {
			ctx.ErrorCode = common.QueueNameDuplicated
		} else {
			ctx.ErrorCode = common.InternalError
		}
		return CreateQueueResponse{}, err
	}

	err = GlobalVCQueue.CreateQueue(&queue)
	if err != nil {
		ctx.Logging().Errorf("GlobalVCQueue create queue failed. error:%s", err.Error())
		ctx.ErrorCode = common.QueueResourceNotMatch
		ctx.ErrorMessage = err.Error()
		deleteErr := models.DeleteQueue(ctx, queue.Name)
		if deleteErr != nil {
			ctx.Logging().Errorf("delete queue roll back db failed. error:%s", deleteErr.Error())
		}
		return CreateQueueResponse{}, err
	}

	err = models.UpdateQueueStatus(queue.Name, common.StatusQueueOpen)
	if err != nil {
		fmt.Errorf("update queue status to open failed")
	}

	ctx.Logging().Debugf("create queue success. queueName:%s", queueInfo.Name)
	response := CreateQueueResponse{
		QueueName: queueInfo.Name,
	}
	return response, nil
}

func GetQueueByName(ctx *logger.RequestContext, queueName string) (models.Queue, error) {
	ctx.Logging().Debugf("begin get queue by name. queueName:%s", queueName)

	if !models.HasAccessToResource(ctx, common.ResourceTypeQueue, queueName) {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorf("get queueName[%s] failed. error: access denied.", queueName)
		return models.Queue{}, fmt.Errorf("get queueName[%s] failed.\n", queueName)
	}

	queue, err := models.GetQueueByName(ctx, queueName)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		return models.Queue{}, fmt.Errorf("queueName[%s] is not found.\n", queueName)
	}
	return queue, nil
}

func CloseQueue(ctx *logger.RequestContext, queueName string) error {
	ctx.Logging().Debugf("begin stop queue. queueName:%s", queueName)
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("close queue failed. error: admin is needed.")
		return errors.New("close queue failed")
	}

	queue, err := models.GetQueueByName(ctx, queueName)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		return fmt.Errorf("queueName[%s] is not found.\n", queueName)
	}

	err = GlobalVCQueue.CloseQueue(&queue)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		log.Errorf("close queue failed. queueName:[%s] error:[%s]", queueName, err.Error())
		return errors.New("close queue failed")
	}

	err = models.CloseQueue(ctx, queueName)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		log.Errorf("close queue update db failed. queueName:[%s] error:[%s]", queueName, err.Error())
		return errors.New("close queue failed")
	}
	ctx.Logging().Debugf("close queue succeed. queueName:%s", queueName)
	return nil
}

func DeleteQueue(ctx *logger.RequestContext, queueName string) error {
	ctx.Logging().Debugf("begin delete queue. queueName:%s", queueName)
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("delete queue failed. error: admin is needed.")
		return errors.New("delete queue failed")
	}

	queue, err := models.GetQueueByName(ctx, queueName)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		return fmt.Errorf("queueName[%s] is not found.\n", queueName)
	}

	if queue.Status != common.StatusQueueClosed {
		ctx.ErrorCode = common.QueueIsNotClosed
		return fmt.Errorf("queueName[%s] is not closed.\n", queueName)
	}

	err = GlobalVCQueue.DeleteQueue(&queue)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		log.Errorf("delete queue failed. queueName:[%s] error:[%s]", queueName, err.Error())
		return errors.New("delete queue failed")
	}
	err = models.DeleteQueue(ctx, queueName)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("delete queue update db failed. queueName:[%s]", queueName)
		return err
	}

	ctx.Logging().Debugf("queue is deleting. queueName:%s", queueName)
	return nil
}
