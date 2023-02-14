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
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	gormErrors "github.com/PaddlePaddle/PaddleFlow/pkg/common/errors"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/api"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const defaultQueueName = "default"
const defaultRootEQuotaName = "root"

type CreateQueueRequest struct {
	Name         string              `json:"name"`
	Namespace    string              `json:"namespace"`
	ClusterName  string              `json:"clusterName"`
	QuotaType    string              `json:"quotaType"`
	MaxResources schema.ResourceInfo `json:"maxResources"`
	MinResources schema.ResourceInfo `json:"minResources"`
	Location     map[string]string   `json:"location"`
	// 任务调度策略
	SchedulingPolicy []string `json:"schedulingPolicy,omitempty"`
	Status           string   `json:"-"`
}

type UpdateQueueRequest struct {
	Name         string              `json:"-"`
	Namespace    string              `json:"-"`
	ClusterName  string              `json:"-"`
	QuotaType    string              `json:"-"`
	MaxResources schema.ResourceInfo `json:"maxResources,omitempty"`
	MinResources schema.ResourceInfo `json:"minResources,omitempty"`
	Location     map[string]string   `json:"location,omitempty"`
	// 任务调度策略
	SchedulingPolicy []string `json:"schedulingPolicy,omitempty"`
	Status           string   `json:"-"`
}

type CreateQueueResponse struct {
	QueueName string `json:"name"`
}

type UpdateQueueResponse struct {
	model.Queue
}

type GetQueueResponse struct {
	model.Queue
}

type ListQueueRequest struct {
	Marker    string
	MaxKeys   int
	QueueName string
}

type ListQueueResponse struct {
	common.MarkerInfo
	QueueList []model.Queue `json:"queueList"`
}

func ListQueue(ctx *logger.RequestContext, marker string, maxKeys int, name string) (ListQueueResponse, error) {
	ctx.Logging().Debugf("begin list queue.")
	listQueueResponse := ListQueueResponse{}
	listQueueResponse.IsTruncated = false
	listQueueResponse.QueueList = []model.Queue{}

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

	queueList, err := storage.Queue.ListQueue(pk, maxKeys, name, ctx.UserName)
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
	listQueueResponse.QueueList = append(listQueueResponse.QueueList, queueList...)
	return listQueueResponse, nil
}

func IsLastQueuePk(ctx *logger.RequestContext, pk int64) bool {
	lastQueue, err := storage.Queue.GetLastQueue()
	if err != nil {
		ctx.Logging().Errorf("get last queue failed. error:[%s]", err.Error())
	}
	if lastQueue.Pk == pk {
		return true
	}
	return false
}

func CreateQueue(ctx *logger.RequestContext, request *CreateQueueRequest) (CreateQueueResponse, error) {
	ctx.Logging().Debugf("begin create request. request:%s", config.PrettyFormat(request))
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("create request failed. error: admin is needed.")
		return CreateQueueResponse{}, errors.New("create request failed")
	}

	if request.Name == "" {
		ctx.ErrorCode = common.QueueNameNotFound
		ctx.Logging().Errorln("create request failed. error: queueName is not found.")
		return CreateQueueResponse{}, errors.New("queueName is not found.")
	}

	if request.ClusterName == "" {
		if config.GlobalServerConfig.Job.IsSingleCluster {
			request.ClusterName = config.DefaultClusterName
		} else {
			ctx.ErrorCode = common.ClusterNameNotFound
			ctx.Logging().Errorln("create request failed. error: clusterName is not found.")
			return CreateQueueResponse{}, errors.New("clusterName is not found")
		}
	}
	clusterInfo, err := storage.Cluster.GetClusterByName(request.ClusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln("create request failed. error: cluster not found by Name.")
		return CreateQueueResponse{}, errors.New("cluster not found by Name")
	}
	if clusterInfo.Status != model.ClusterStatusOnLine {
		ctx.ErrorCode = common.InvalidClusterStatus
		errMsg := fmt.Sprintf("cluster[%s] not in online status, operator not permit", clusterInfo.Name)
		ctx.Logging().Errorln(errMsg)
		return CreateQueueResponse{}, errors.New(errMsg)
	}
	// validate namespace
	if err = validateNamespace(request.Namespace, clusterInfo); err != nil {
		ctx.ErrorCode = common.NamespaceNotFound
		ctx.Logging().Errorf("create request failed. error: namespace[%s] is invalid, err: %v", request.Namespace, err)
		return CreateQueueResponse{}, err
	}

	if errStr := common.IsDNS1123Label(request.Name); len(errStr) != 0 {
		ctx.ErrorCode = common.InvalidNamePattern
		log.Errorf("CreateQueue failed when check name[%s] isDNS1123Label. err: %s.", request.Name, errStr)
		return CreateQueueResponse{}, fmt.Errorf("name[%s] of queue is invalid, err: %s",
			request.Name, strings.Join(errStr, ","))
	}

	exist := strings.EqualFold(request.Name, defaultQueueName) || storage.Queue.IsQueueExist(request.Name)
	if exist {
		ctx.Logging().Errorf("create queue failed. queueName[%s] exist.", request.Name)
		ctx.ErrorCode = common.QueueNameDuplicated
		return CreateQueueResponse{}, errors.New("request name duplicated")
	}

	// check quota type of queue
	if len(request.QuotaType) == 0 {
		// TODO: get quota type from cluster info
		request.QuotaType = schema.TypeElasticQuota
	}
	if request.QuotaType != schema.TypeElasticQuota && request.QuotaType != schema.TypeVolcanoCapabilityQuota {
		ctx.Logging().Errorf("create queue failed. the type %s of quota is not supported.", request.QuotaType)
		ctx.ErrorCode = common.QueueQuotaTypeIsNotSupported
		return CreateQueueResponse{}, errors.New("quota type is not supported")
	}

	// check request max resources and min resources
	maxResources, err := resources.NewResourceFromMap(request.MaxResources.ToMap())
	if err != nil {
		ctx.Logging().Errorf("create queue failed. error: %s", err.Error())
		ctx.ErrorCode = common.InvalidComputeResource
		return CreateQueueResponse{}, err
	}
	if maxResources.IsNegative() {
		err = fmt.Errorf("queue maxResources[%v] has negative value", request.MaxResources)
		ctx.Logging().Errorf("create queue failed. error: %s", err.Error())
		ctx.ErrorCode = common.InvalidComputeResource
		return CreateQueueResponse{}, err
	}
	minResources := resources.EmptyResource()
	if request.QuotaType == schema.TypeElasticQuota {
		// check min resources for elastic queue
		minResources, err = resources.NewResourceFromMap(request.MinResources.ToMap())
		if err != nil {
			ctx.Logging().Errorf("create queue failed. error: %s", err.Error())
			ctx.ErrorCode = common.InvalidComputeResource
			return CreateQueueResponse{}, err
		}
		if minResources.IsNegative() {
			err = fmt.Errorf("queue minResources[%v] has negative value", request.MinResources)
			ctx.Logging().Errorf("create queue failed. error: %s", err.Error())
			ctx.ErrorCode = common.InvalidComputeResource
			return CreateQueueResponse{}, err
		}
		if !minResources.LessEqual(maxResources) {
			ctx.Logging().Errorf("create queue failed. error: maxResources less than minResources")
			ctx.ErrorCode = common.InvalidComputeResource
			return CreateQueueResponse{}, fmt.Errorf("maxResources less than minResources")
		}
	}

	if request.Location == nil {
		request.Location = make(map[string]string)
	}
	if request.QuotaType == schema.TypeElasticQuota {
		// check the hierarchy of elastic quota
		eQuotaType := request.Location[v1beta1.QuotaTypeKey]
		switch eQuotaType {
		case "", v1beta1.QuotaTypeLogical:
			request.Location[v1beta1.QuotaTypeKey] = v1beta1.QuotaTypeLogical
			// set parent elastic quota
			if _, exist := request.Location[v1beta1.ElasticQuotaParentKey]; !exist {
				request.Location[v1beta1.ElasticQuotaParentKey] = defaultRootEQuotaName
			}
		case v1beta1.QuotaTypePhysical:
			// delete parent for physical elastic quota
			delete(request.Location, v1beta1.ElasticQuotaParentKey)
		default:
			return CreateQueueResponse{}, fmt.Errorf("the type of elastic quota %s is not suppported", eQuotaType)
		}
	}

	request.Status = schema.StatusQueueCreating
	queueInfo := model.Queue{
		Model: model.Model{
			ID: uuid.GenerateID(common.PrefixQueue),
		},
		Name:             request.Name,
		Namespace:        request.Namespace,
		QuotaType:        request.QuotaType,
		ClusterId:        clusterInfo.ID,
		MaxResources:     maxResources,
		MinResources:     minResources,
		Location:         request.Location,
		SchedulingPolicy: request.SchedulingPolicy,
		Status:           schema.StatusQueueCreating,
	}
	err = storage.Queue.CreateQueue(&queueInfo)
	if err != nil {
		ctx.Logging().Errorf("create request failed. error:%s", err.Error())
		if gormErrors.GetErrorCode(err) == gormErrors.ErrorKeyIsDuplicated {
			ctx.ErrorCode = common.QueueNameDuplicated
		} else {
			ctx.ErrorCode = common.InternalError
		}
		return CreateQueueResponse{}, err
	}

	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		ctx.Logging().Errorf("GlobalVCQueue create request failed. error:%s", err.Error())
		ctx.ErrorCode = common.QueueResourceNotMatch
		ctx.ErrorMessage = err.Error()
		deleteErr := storage.Queue.DeleteQueue(request.Name)
		if deleteErr != nil {
			ctx.Logging().Errorf("delete request roll back db failed. error:%s", deleteErr.Error())
		}
		return CreateQueueResponse{}, err
	}
	// create namespace if not exist in cluster
	k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
	coreNs := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: request.Namespace,
		},
	}
	if _, err = k8sRuntime.CreateNamespace(coreNs, metav1.CreateOptions{}); err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("create namespace [%s] resource on cluster %s failed, err: %v",
			request.Namespace, request.ClusterName, err)
		return CreateQueueResponse{}, err
	}
	// create queue in cluster
	rQ := api.NewQueueInfo(queueInfo)
	err = runtimeSvc.CreateQueue(rQ)
	if err != nil && k8serrors.IsAlreadyExists(err) {
		_, err = UpdateQueue(ctx, &UpdateQueueRequest{
			Name:         request.Name,
			Namespace:    request.Namespace,
			MaxResources: request.MaxResources,
			MinResources: request.MinResources,
			QuotaType:    request.QuotaType,
		})
	}
	if err != nil {
		ctx.Logging().Errorf("GlobalVCQueue create request failed. error:%s", err.Error())
		ctx.ErrorCode = common.QueueResourceNotMatch
		ctx.ErrorMessage = err.Error()
		deleteErr := storage.Queue.DeleteQueue(request.Name)
		if deleteErr != nil {
			ctx.Logging().Errorf("delete request roll back db failed. error:%s", deleteErr.Error())
		}
		return CreateQueueResponse{}, err
	}

	err = storage.Queue.UpdateQueueStatus(request.Name, schema.StatusQueueOpen)
	if err != nil {
		fmt.Errorf("update request status to open failed")
	}

	ctx.Logging().Debugf("create request success. queueName:%s", request.Name)
	response := CreateQueueResponse{
		QueueName: request.Name,
	}
	return response, nil
}

func validateNamespace(namespace string, clusterInfo model.ClusterInfo) error {
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	// check namespace format
	if errStr := common.IsDNS1123Label(namespace); len(errStr) != 0 {
		return fmt.Errorf("namespace[%s] of queue is invalid, err: %s",
			namespace, strings.Join(errStr, ","))
	}
	// check NamespaceList rules
	if len(clusterInfo.NamespaceList) != 0 {
		isExist := false
		for _, ns := range clusterInfo.NamespaceList {
			if namespace == ns {
				isExist = true
				break
			}
		}
		if !isExist {
			return fmt.Errorf(
				"namespace[%s] of queue not in the specified values [%s] by cluster[%s]",
				namespace, clusterInfo.RawNamespaceList, clusterInfo.Name)
		}
	}
	return nil
}

func UpdateQueue(ctx *logger.RequestContext, request *UpdateQueueRequest) (UpdateQueueResponse, error) {
	ctx.Logging().Debugf("begin update request. request:%s", config.PrettyFormat(request))
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("update request failed. error: admin is needed.")
		return UpdateQueueResponse{}, errors.New("update request failed")
	}
	// check queue name
	if request.Name == "" {
		ctx.ErrorCode = common.QueueNameNotFound
		ctx.Logging().Errorln("update request failed. error: queueName is not found.")
		return UpdateQueueResponse{}, errors.New("queueName is not found")
	}
	queueInfo, err := storage.Queue.GetQueueByName(request.Name)
	if err != nil {
		ctx.ErrorCode = common.RecordNotFound
		ctx.Logging().Errorf("get queue failed. error:%s", err.Error())
		return UpdateQueueResponse{}, err
	}
	// record a snapshot of queue
	var queueSnapshot model.Queue
	storage.Queue.DeepCopyQueue(queueInfo, &queueSnapshot)
	// get cluster, if closed, refuse to update queue
	clusterInfo, err := storage.Cluster.GetClusterById(queueInfo.ClusterId)
	if err != nil {
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln("update request failed. error: cluster not found by Name.")
		return UpdateQueueResponse{}, errors.New("cluster not found by Name")
	}
	if clusterInfo.Status != model.ClusterStatusOnLine {
		ctx.ErrorCode = common.InvalidClusterStatus
		errMsg := fmt.Sprintf("cluster[%s] not in online status, operator not permit", clusterInfo.Name)
		ctx.Logging().Errorln(errMsg)
		return UpdateQueueResponse{}, errors.New(errMsg)
	}

	// validate fields if not nil, validate namespace at first
	var updateClusterRequired, resourceUpdated bool

	// validate MaxResource or MinResource
	if resourceUpdated, err = validateQueueResource(request.MaxResources, queueInfo.MaxResources); err != nil {
		ctx.Logging().Errorf("update queue maxResources failed. error: %s", err.Error())
		ctx.ErrorCode = common.InvalidComputeResource
		return UpdateQueueResponse{}, err
	}
	if queueInfo.QuotaType == schema.TypeElasticQuota {
		minResUpdated, err := validateQueueResource(request.MinResources, queueInfo.MinResources)
		if err != nil {
			ctx.Logging().Errorf("update queue minResources failed. error: %s", err.Error())
			ctx.ErrorCode = common.InvalidComputeResource
			return UpdateQueueResponse{}, err
		}
		resourceUpdated = resourceUpdated || minResUpdated
		if resourceUpdated && !queueInfo.MinResources.LessEqual(queueInfo.MaxResources) {
			err = fmt.Errorf("minResource cannot be larger than maxResource")
			ctx.Logging().Errorf("update queue failed. error: %s", err.Error())
			ctx.ErrorCode = common.InvalidComputeResource
			return UpdateQueueResponse{}, err
		}
	}
	if resourceUpdated {
		updateClusterRequired = true
	}

	// validate Location
	if len(request.Location) != 0 {
		updateClusterRequired = true
		// check the hierarchy of elastic quota
		if queueInfo.QuotaType == schema.TypeElasticQuota {
			_, exist := request.Location[v1beta1.QuotaTypeKey]
			if exist {
				err = fmt.Errorf("the isolaction type of elastic quota cannot be changed")
				ctx.Logging().Errorf("update queue failed. error: %s", err.Error())
				ctx.ErrorCode = common.InvalidArguments
				return UpdateQueueResponse{}, err
			}
			// remove parent for physical elastic quota
			if queueInfo.Location[v1beta1.QuotaTypeKey] == v1beta1.QuotaTypePhysical {
				delete(request.Location, v1beta1.ElasticQuotaParentKey)
				delete(queueInfo.Location, v1beta1.ElasticQuotaParentKey)
			}
		}
		for key, value := range request.Location {
			if len(value) == 0 {
				// remove location when value is empty
				delete(queueInfo.Location, key)
			} else {
				queueInfo.Location[key] = value
			}
		}
	}

	// validate scheduling policy
	if len(request.SchedulingPolicy) != 0 {
		log.Debug("update queue scheduling policy")
		// TODO: change the data type of schedulingPolicy to map[string]interface{}
		schedulingPolicy := make(map[string]struct{})
		for _, policy := range queueInfo.SchedulingPolicy {
			schedulingPolicy[policy] = struct{}{}
		}
		for _, policy := range request.SchedulingPolicy {
			if strings.HasSuffix(policy, "-") {
				// remove old scheduling policy
				sp := strings.TrimRight(policy, "-")
				delete(schedulingPolicy, sp)
			} else {
				schedulingPolicy[policy] = struct{}{}
			}
		}
		sp := []string{}
		for policy, _ := range schedulingPolicy {
			sp = append(sp, policy)
		}
		queueInfo.SchedulingPolicy = sp
	}

	// init runtimeSvc if updateCluster is necessary
	var runtimeSvc runtime.RuntimeService
	if updateClusterRequired {
		runtimeSvc, err = runtime.GetOrCreateRuntime(clusterInfo)
		if err != nil {
			ctx.Logging().Errorf("GlobalVCQueue update request failed. error:%s", err.Error())
			ctx.ErrorCode = common.QueueResourceNotMatch
			ctx.ErrorMessage = err.Error()
			return UpdateQueueResponse{}, err
		}
	}

	// update queue in db
	if err = storage.Queue.UpdateQueue(&queueInfo); err != nil {
		ctx.Logging().Errorf("update queue failed. error:%s", err.Error())
		ctx.ErrorCode = common.QueueUpdateFailed
		return UpdateQueueResponse{}, err
	}

	// update queue in cluster, which will roll back changes in db if failed
	if updateClusterRequired {
		log.Debugf("required to update queue in cluster. queueName:[%s]", queueInfo.Name)
		rQ := api.NewQueueInfo(queueInfo)
		if err = runtimeSvc.UpdateQueue(rQ); err != nil {
			ctx.Logging().Errorf("GlobalVCQueue create request failed. error:%s", err.Error())
			ctx.ErrorCode = common.QueueResourceNotMatch
			ctx.ErrorMessage = err.Error()
			if rollbackErr := storage.Queue.UpdateQueue(&queueSnapshot); rollbackErr != nil {
				ctx.Logging().Errorf("update request roll back db failed.queue:%s error:%v",
					queueSnapshot.Name, rollbackErr)
				err = rollbackErr
			}
			return UpdateQueueResponse{}, err
		}
	}

	ctx.Logging().Debugf("update request success. queueName:%s", queueInfo.Name)
	response := UpdateQueueResponse{
		queueInfo,
	}
	return response, nil
}

func validateQueueResource(rResource schema.ResourceInfo, qResource *resources.Resource) (bool, error) {
	needUpdate := false
	if qResource == nil {
		return needUpdate, fmt.Errorf("queue resource is null")
	}
	if rResource.CPU != "" {
		needUpdate = true
		cpu, err := resources.ParseMilliQuantity(rResource.CPU)
		if err != nil {
			log.Errorf("parse cpu resource failed, err=%v", err)
			return needUpdate, err
		}
		qResource.SetResources(resources.ResCPU, int64(cpu))

	}
	if rResource.Mem != "" {
		needUpdate = true
		mem, err := resources.ParseQuantity(rResource.Mem)
		if err != nil {
			log.Errorf("parse memory resource failed, err=%v", err)
			return needUpdate, err
		}
		qResource.SetResources(resources.ResMemory, int64(mem))
	}
	if len(rResource.ScalarResources) != 0 {
		needUpdate = true
		for rName, rValue := range rResource.ScalarResources {
			if rValue == "" {
				// remove empty resource
				qResource.DelResources(string(rName))
			} else {
				rQuantity, err := resources.ParseQuantity(rValue)
				if err != nil {
					log.Errorf("parse resource failed, err=%v", err)
					return needUpdate, err
				}
				qResource.SetResources(string(rName), int64(rQuantity))

			}
		}
	}
	if qResource.IsNegative() {
		err := fmt.Errorf("queue resource[%v] has negative value", qResource)
		log.Errorf("validate resourceInfo failed, err=%v", err)
		return needUpdate, err
	}
	return needUpdate, nil
}

func GetQueueByName(ctx *logger.RequestContext, queueName string) (GetQueueResponse, error) {
	ctx.Logging().Debugf("begin get queue by name. queueName:%s", queueName)

	if !storage.Auth.HasAccessToResource(ctx, common.ResourceTypeQueue, queueName) {
		ctx.ErrorCode = common.ActionNotAllowed
		ctx.Logging().Errorf("get queueName[%s] failed. error: access denied.", queueName)
		return GetQueueResponse{}, fmt.Errorf("get queueName[%s] failed.\n", queueName)
	}

	queue, err := storage.Queue.GetQueueByName(queueName)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		return GetQueueResponse{}, fmt.Errorf("queueName[%s] is not found.\n", queueName)
	}

	clusterInfo, err := storage.Cluster.GetClusterById(queue.ClusterId)
	if err != nil {
		ctx.Logging().Errorf("get clusterInfo by ClusterId %s failed. error: %s",
			queue.ClusterId, err.Error())
		return GetQueueResponse{}, err
	}

	// calculate the idle resource of queue
	usedResource := resources.EmptyResource()
	if clusterInfo.Status == model.ClusterStatusOnLine {
		runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
		if err != nil {
			ctx.ErrorCode = common.InternalError
			ctx.Logging().Errorf("get queue used quota failed. queueName:[%s] error:[%s]", queueName, err.Error())
			return GetQueueResponse{}, fmt.Errorf("get queue used quota failed, error: %v", err)
		}
		switch clusterInfo.ClusterType {
		case schema.KubernetesType:
			kubeRuntime := runtimeSvc.(*runtime.KubeRuntime)
			rQ := api.NewQueueInfo(queue)
			usedResource, err = kubeRuntime.GetQueueUsedQuota(rQ)
			if err != nil {
				ctx.ErrorCode = common.InternalError
				ctx.Logging().Errorf("get queue used quota failed. queueName:[%s] error:[%s]", queueName, err.Error())
				return GetQueueResponse{}, fmt.Errorf("get queue used quota failed, error: %v", err)
			}
		default:
			ctx.Logging().Warnf("cannot get queue used quota for cluster type %s", clusterInfo.ClusterType)
		}
	}
	idleResource := queue.MaxResources.Clone()
	idleResource.Sub(usedResource)
	queue.IdleResources = idleResource
	queue.UsedResources = usedResource

	getQueueResponse := GetQueueResponse{
		Queue: queue,
	}
	return getQueueResponse, nil
}

func DeleteQueue(ctx *logger.RequestContext, queueName string) error {
	ctx.Logging().Debugf("begin delete queue. queueName:%s", queueName)
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("delete queue failed. error: admin is needed.")
		return errors.New("delete queue failed")
	}

	queue, err := storage.Queue.GetQueueByName(queueName)
	if err != nil {
		ctx.ErrorCode = common.QueueNameNotFound
		return fmt.Errorf("queueName[%s] is not found.\n", queueName)
	}

	isInUse, jobsInfo := storage.Queue.IsQueueInUse(queue.ID)
	if isInUse {
		ctx.ErrorCode = common.QueueIsInUse
		ctx.ErrorMessage = fmt.Sprintf("queue[%s] is inuse, and jobs on queue: %v", queueName, jobsInfo)
		ctx.Logging().Errorf(ctx.ErrorMessage)
		return fmt.Errorf(ctx.ErrorMessage)
	}
	clusterInfo, err := storage.Cluster.GetClusterById(queue.ClusterId)
	if err != nil {
		ctx.Logging().Errorf("get clusterInfo by ClusterId %s failed. error: %s",
			queue.ClusterId, err.Error())
		return err
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(clusterInfo)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("delete queue failed. queueName:[%s] error:[%s]", queueName, err.Error())
		return errors.New("delete queue failed")
	}
	rQ := api.NewQueueInfo(queue)
	err = runtimeSvc.DeleteQueue(rQ)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.Logging().Errorf("delete queue failed. queueName:[%s] error:[%s]", queueName, err.Error())
		return errors.New("delete queue failed")
	}
	err = storage.Queue.DeleteQueue(queueName)
	if err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("delete queue update db failed. queueName:[%s]", queueName)
		return err
	}

	ctx.Logging().Debugf("queue is deleting. queueName:%s", queueName)
	return nil
}

// InitDefaultQueue init default queue for single cluster environment
func InitDefaultQueue() error {
	log.Info("starting init data for single cluster: initDefaultQueue")
	if defaultQueue, err := storage.Queue.GetQueueByName(config.DefaultQueueName); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Errorf("GetQueueByName %s failed, err: %v", config.DefaultQueueName, err)
		return err
	} else if err == nil {
		log.Infof("default queue[%+v] has been created", defaultQueue)
		return nil
	}
	ctx := &logger.RequestContext{UserName: common.UserRoot}
	// create default cluster
	defaultQueue := &CreateQueueRequest{
		Name:        config.DefaultQueueName,
		Namespace:   config.DefaultNamespace,
		ClusterName: config.DefaultClusterName,
		QuotaType:   schema.TypeVolcanoCapabilityQuota,
		MaxResources: schema.ResourceInfo{
			CPU: "20",
			Mem: "20Gi",
		},
	}
	_, err := CreateQueue(ctx, defaultQueue)
	if err != nil {
		log.Errorf("create default queue[%+v] failed, err: %v", defaultQueue, err)
		return err
	}
	return nil
}
