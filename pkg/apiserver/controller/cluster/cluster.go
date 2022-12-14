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

package cluster

import (
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/queue"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/uuid"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type ClusterCommonInfo struct {
	ID            string   `json:"clusterId"`     // 集群id
	Description   string   `json:"description"`   // 集群描述
	Endpoint      string   `json:"endpoint"`      // 集群endpoint, 比如 http://10.11.11.47:8080
	Source        string   `json:"source"`        // 来源, 比如 OnPremise （内部部署）、AWS、CCE
	ClusterType   string   `json:"clusterType"`   // 集群类型，比如kubernetes/local/yarn
	Version       string   `json:"version"`       // 集群版本v1.16
	Status        string   `json:"status"`        // 集群状态，可选值为online, offline
	Credential    string   `json:"credential"`    // 用于存储集群的凭证信息，比如k8s的kube_config配置
	Setting       string   `json:"setting"`       // 存储额外配置信息
	NamespaceList []string `json:"namespaceList"` // 命名空间列表，json类型，如["ns1", "ns2"]
}
type CreateClusterRequest struct {
	ClusterCommonInfo
	Name string `json:"clusterName"` // 集群名字
}

type CreateClusterResponse struct {
	model.ClusterInfo
}

type GetClusterResponse struct {
	model.ClusterInfo
}

type ListClusterRequest struct {
	Marker          string   `json:"marker"`
	MaxKeys         int      `json:"maxKeys"`
	ClusterNameList []string `json:"clusterNameList"`
	ClusterStatus   string   `json:"clusterStatus"`
}

type ListClusterResponse struct {
	common.MarkerInfo
	ClusterList []model.ClusterInfo `json:"clusterList"`
}

type UpdateClusterRequest struct {
	ClusterCommonInfo
}

type UpdateClusterReponse struct {
	model.ClusterInfo
}

type ClusterQuotaReponse struct {
	NodeQuotaInfoList []schema.NodeQuotaInfo `json:"nodeList"`
	Summary           schema.QuotaSummary    `json:"summary"`
	ErrMessage        string                 `json:"errMsg"`
}

func validateClusterStatus(clusterStatus string) error {
	validClusterStatusList := []string{
		model.ClusterStatusOnLine,
		model.ClusterStatusOffLine,
	}

	if !common.StringInSlice(clusterStatus, validClusterStatusList) {
		return errors.New("clusterStatus can only be 'online' or 'offline'")
	}
	return nil
}

func validateCreateClusterRequest(ctx *logger.RequestContext, request *CreateClusterRequest) error {
	request.Name = strings.TrimSpace(request.Name)

	request.Endpoint = strings.TrimSpace(request.Endpoint)
	if request.Endpoint == "" {
		return errors.New("Endpoint should not be empty")
	}

	request.ClusterType = strings.TrimSpace(request.ClusterType)
	switch request.ClusterType {
	case schema.KubernetesType, schema.LocalType:
	case "":
		return fmt.Errorf("ClusterType should not be empty")
	default:
		return fmt.Errorf("ClusterType [%s] is invalid", request.ClusterType)
	}

	request.Version = strings.TrimSpace(request.Version)
	if request.ClusterType == schema.KubernetesType && request.Version == "" {
		return fmt.Errorf("version of cluster %s should not be empty", request.ClusterType)
	}

	request.Status = strings.TrimSpace(request.Status)
	if request.Status == "" {
		request.Status = model.DefaultClusterStatus
	} else {
		if err := validateClusterStatus(request.Status); err != nil {
			return err
		}
	}

	// check namespaceList
	for _, ns := range request.NamespaceList {
		errStr := common.IsDNS1123Label(ns)
		if len(errStr) != 0 {
			return fmt.Errorf("namespace[%s] is invalid, err: %s", ns, strings.Join(errStr, ","))
		}
	}

	request.Source = strings.TrimSpace(request.Source)
	if request.Source == "" {
		request.Source = model.DefaultClusterSource
	}

	request.Credential = strings.TrimSpace(request.Credential)
	request.Description = strings.TrimSpace(request.Description)
	request.Setting = strings.TrimSpace(request.Setting)

	return nil
}

func validateUpdateClusterRequest(ctx *logger.RequestContext, request *UpdateClusterRequest, clusterInfo *model.ClusterInfo) error {
	request.Endpoint = strings.TrimSpace(request.Endpoint)
	if request.Endpoint != "" {
		clusterInfo.Endpoint = request.Endpoint
	}

	request.ClusterType = strings.TrimSpace(request.ClusterType)
	switch request.ClusterType {
	case schema.KubernetesType, schema.LocalType:
		clusterInfo.ClusterType = strings.TrimSpace(request.ClusterType)
	case "":
	default:
		return fmt.Errorf("ClusterType [%s] is invalid", request.ClusterType)
	}

	request.Version = strings.TrimSpace(request.Version)
	if request.Version != "" {
		clusterInfo.Version = request.Version
	} else if request.ClusterType == schema.KubernetesType {
		return fmt.Errorf("version of cluster %s should not be empty", request.ClusterType)
	}

	request.Status = strings.TrimSpace(request.Status)
	if request.Status != "" {
		if err := validateClusterStatus(request.Status); err != nil {
			return err
		}
		clusterInfo.Status = request.Status
	}

	request.Source = strings.TrimSpace(request.Source)
	if request.Source != "" {
		clusterInfo.Source = strings.TrimSpace(request.Source)
	}
	request.Credential = strings.TrimSpace(request.Credential)
	if request.Description != "" {
		clusterInfo.Description = strings.TrimSpace(request.Description)
	}
	request.Setting = strings.TrimSpace(request.Setting)
	if request.Setting != "" {
		clusterInfo.Setting = strings.TrimSpace(request.Setting)
	}
	if len(request.NamespaceList) > 0 {
		if err := validateNamespace(request.NamespaceList, clusterInfo.NamespaceList, clusterInfo.ID); err != nil {
			ctx.Logging().Errorf("update namespace for cluster[%s] failed. errorMsg:[%s]", clusterInfo.Name, err.Error())
			return err
		}
		clusterInfo.NamespaceList = request.NamespaceList
	}
	if request.Credential != "" {
		clusterInfo.Credential = strings.TrimSpace(request.Credential)
		if err := validateConnectivity(*clusterInfo); err != nil {
			ctx.Logging().Errorf("update runtime failed, cluster[%s]. errorMsg:[%s]", clusterInfo.Name, err.Error())
			ctx.ErrorCode = common.InvalidCredential
			return err
		}
	}

	return nil
}

// validateNamespace if any namespace bound queue, it must not be deleted
func validateNamespace(new, old []string, clusterID string) error {
	// if deleted any, must check queue unbound
	errNs := make([]string, 0)
	newSet := make(map[string]bool)
	for _, ns := range new {
		newSet[ns] = true
	}
	// get queues
	queues := storage.Queue.ListQueuesByCluster(clusterID)
	relatedSet := make(map[string]bool)
	for _, queue := range queues {
		relatedSet[queue.Namespace] = true
	}
	for _, ns := range old {
		if !newSet[ns] && relatedSet[ns] {
			errNs = append(errNs, ns)
		}
	}

	if len(errNs) != 0 {
		return fmt.Errorf("cluster still bound to queue by namespace%v", errNs)
	}
	return nil
}

func CreateCluster(ctx *logger.RequestContext, request *CreateClusterRequest) (*CreateClusterResponse, error) {
	clusterName := strings.TrimSpace(request.Name)

	if !schema.CheckReg(clusterName, common.RegPatternClusterName) {
		ctx.Logging().Errorf("create cluster failed. clusterName not allowed. clusterName:%v", clusterName)
		ctx.ErrorCode = common.InvalidNamePattern
		err := common.InvalidNamePatternError(clusterName, common.ResourceTypeCluster, common.RegPatternClusterName)
		return nil, err
	}

	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("create cluster failed. error: admin is needed.")
		return nil, errors.New("create cluster failed")
	}

	if err := validateCreateClusterRequest(ctx, request); err != nil {
		ctx.Logging().Errorf("validateCreateClusterRequest failed, ClusterName: %s", clusterName)
		ctx.ErrorCode = common.InternalError
		return nil, err
	}

	clusterId := uuid.GenerateID(common.PrefixCluster)
	clusterInfo := model.ClusterInfo{
		Model: model.Model{
			ID:        clusterId,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Name:          clusterName,
		Description:   request.Description,
		Endpoint:      request.Endpoint,
		Source:        request.Source,
		ClusterType:   request.ClusterType,
		Version:       request.Version,
		Status:        request.Status,
		Credential:    request.Credential,
		Setting:       request.Setting,
		NamespaceList: request.NamespaceList,
	}

	if err := validateConnectivity(clusterInfo); err != nil {
		ctx.Logging().Errorf("get cluster [%s] client failed. %s", clusterInfo.Name, err.Error())
		ctx.ErrorCode = common.InvalidCredential
		return nil, err
	}

	err := storage.Cluster.CreateCluster(&clusterInfo)
	response := CreateClusterResponse{clusterInfo}
	return &response, err
}

// validateConnectivity validate connectivity of cluster
func validateConnectivity(clusterInfo model.ClusterInfo) error {
	if _, err := runtime.CreateRuntime(clusterInfo); err != nil {
		log.Errorf("create cluster failed. clusterName not allowed. clusterName:%v", clusterInfo.Name)
		return err
	}
	return nil
}

func IsLastClusterPk(ctx *logger.RequestContext, pk int64) bool {
	lastCluster, err := storage.Cluster.GetLastCluster()
	if err != nil {
		ctx.Logging().Errorf("get last cluster failed. error:[%s]", err.Error())
	}
	if lastCluster.Pk == pk {
		return true
	}
	return false
}

func ListCluster(ctx *logger.RequestContext, marker string, maxKeys int,
	clusterNameList []string, clusterStatus string) (*ListClusterResponse, error) {
	ctx.Logging().Debug("begin list cluster.")
	response := ListClusterResponse{}
	response.IsTruncated = false

	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("list cluster failed. error: admin is needed.")
		return &response, errors.New("list cluster failed")
	}

	var pk int64
	var err error
	if marker != "" {
		pk, err = common.DecryptPk(marker)
		if err != nil {
			ctx.Logging().Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			ctx.ErrorCode = common.InvalidMarker
			return &response, err
		}
	}

	clusterList, err := storage.Cluster.ListCluster(pk, maxKeys, clusterNameList, clusterStatus)
	if err != nil {
		ctx.Logging().Errorf("list cluster failed. err:[%s]", err.Error())
		ctx.ErrorCode = common.InternalError
		return &response, err
	}

	if len(clusterList) > 0 {
		cluster := clusterList[len(clusterList)-1]
		if !IsLastClusterPk(ctx, cluster.Pk) {
			nextMarker, err := common.EncryptPk(cluster.Pk)
			if err != nil {
				ctx.Logging().Errorf("EncryptPk error. pk:[%d] error:[%s] ",
					cluster.Pk, err.Error())
				ctx.ErrorCode = common.InternalError
				return &response, err
			}
			response.NextMarker = nextMarker
			response.IsTruncated = true
		}
	}
	response.MaxKeys = int(maxKeys)
	for _, cluster := range clusterList {
		response.ClusterList = append(response.ClusterList, cluster)
	}

	return &response, nil
}

func GetCluster(ctx *logger.RequestContext, clusterName string) (*GetClusterResponse, error) {
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("get cluster failed. error: admin is needed.")
		return nil, errors.New("get cluster failed")
	}

	clusterInfo, err := storage.Cluster.GetClusterByName(clusterName)
	if err != nil {
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("get cluster failed. clusterName:[%s]", clusterName)
		return nil, err
	}
	return &GetClusterResponse{clusterInfo}, nil
}

func DeleteCluster(ctx *logger.RequestContext, clusterName string) error {
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("delete cluster failed. error: admin is needed.")
		return errors.New("delete cluster failed")
	}
	// 检查clusterName是否存在
	clusterInfo, err := storage.Cluster.GetClusterByName(clusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNotFound
		ctx.Logging().Errorln("delete cluster failed. error: cluster not found.")
		return err
	}
	queues := storage.Queue.ListQueuesByCluster(clusterInfo.ID)
	var inUsedQueue []string
	for _, q := range queues {
		isInUse, _ := storage.Queue.IsQueueInUse(q.ID)
		if isInUse {
			inUsedQueue = append(inUsedQueue, q.Name)
		}
	}
	if len(inUsedQueue) > 0 {
		ctx.ErrorCode = common.QueueIsInUse
		ctx.ErrorMessage = fmt.Sprintf("cluster %s has inuse queues: %s", clusterName, inUsedQueue)
		ctx.Logging().Errorf(ctx.ErrorMessage)
		return fmt.Errorf(ctx.ErrorMessage)
	}
	// delete queues
	for _, q := range queues {
		if err = queue.DeleteQueue(ctx, q.Name); err != nil {
			ctx.ErrorCode = common.InternalError
			ctx.ErrorMessage = err.Error()
			ctx.Logging().Errorf("delete cluster failed. clusterName:[%s]", clusterName)
			return err
		}
	}
	if err := storage.Cluster.DeleteCluster(clusterName); err != nil {
		ctx.ErrorCode = common.InternalError
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("delete cluster failed. clusterName:[%s]", clusterName)
		return err
	}

	return nil
}

func UpdateCluster(ctx *logger.RequestContext,
	clusterName string, request *UpdateClusterRequest) (*UpdateClusterReponse, error) {
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("update cluster failed. error: admin is needed.")
		return nil, errors.New("update cluster failed")
	}

	clusterInfo, err := storage.Cluster.GetClusterByName(clusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNameNotFound
		ctx.Logging().Errorf("get cluster failed. clusterName:[%s]", clusterName)
		return nil, err
	}

	if err := validateUpdateClusterRequest(ctx, request, &clusterInfo); err != nil {
		ctx.Logging().Errorf("validateCreateClusterRequest failed, ClusterName: %s", clusterName)
		ctx.ErrorCode = common.InvalidClusterProperties
		return nil, err
	}

	if err := storage.Cluster.UpdateCluster(clusterInfo.ID, &clusterInfo); err != nil {
		ctx.ErrorMessage = err.Error()
		ctx.Logging().Errorf("delete cluster failed. clusterName:[%s]", clusterName)
		return nil, err
	}
	response := UpdateClusterReponse{clusterInfo}
	return &response, nil
}

// 根据clusterNameList列出其对应的cluster quota信息
// 如果clusterNameList为空，则返回所有集群的cluster quota信息
func ListClusterQuota(ctx *logger.RequestContext, clusterNameList []string) (map[string]ClusterQuotaReponse, error) {
	response := map[string]ClusterQuotaReponse{}

	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("get cluster quota failed. error: admin is needed.")
		return response, errors.New("get cluster failed")
	}

	// 获取状态为online的集群列表
	clusterList, err := storage.Cluster.ListCluster(0, -1, clusterNameList, model.ClusterStatusOnLine)
	if err != nil {
		ctx.Logging().Errorf("listCluster failed. error: %s", err.Error())
		return response, err
	}

	// 如果用户传了集群名，但是在DB中没找到集群信息，则报错
	if len(clusterNameList) != 0 && len(clusterList) == 0 {
		return response, fmt.Errorf("can't get cluster quota by clusterNames: %s",
			strings.Join(clusterNameList, ","))
	}

	failedClusterErrorMsgList := []string{}
	for _, c := range clusterList {
		runtimeSvc, err := runtime.GetOrCreateRuntime(c)

		if err != nil {
			errMsg := fmt.Sprintf("clusterName: %s, errorMsg: %s", c.Name, err.Error())
			ctx.Logging().Errorf("get k8s client failed. %s", errMsg)
			response[c.Name] = ClusterQuotaReponse{
				ErrMessage:        errMsg,
				NodeQuotaInfoList: []schema.NodeQuotaInfo{},
			}
			failedClusterErrorMsgList = append(failedClusterErrorMsgList, errMsg)
		} else {
			summary, nodeQuotaList, err := runtimeSvc.ListNodeQuota()
			if err != nil {
				errMsg := fmt.Sprintf("clusterName: %s, errorMsg: %s", c.Name, err.Error())
				ctx.Logging().Errorf("get cluster resource quota failed. %s", errMsg)
				failedClusterErrorMsgList = append(failedClusterErrorMsgList, errMsg)
			} else {
				response[c.Name] = ClusterQuotaReponse{
					Summary:           summary,
					NodeQuotaInfoList: nodeQuotaList,
					ErrMessage:        "",
				}
			}
		}
	}
	// 如果所有的集群都报错，则报错
	if len(failedClusterErrorMsgList) > 0 && len(failedClusterErrorMsgList) == len(clusterList) {
		return response, fmt.Errorf("%s", strings.Join(failedClusterErrorMsgList, "\n"))
	}
	return response, nil
}

// InitDefaultCluster init default cluster for single cluster environment
func InitDefaultCluster() error {
	log.Info("starting init data for single cluster: initDefaultCluster")
	if clusterInfo, err := storage.Cluster.GetClusterByName(config.DefaultClusterName); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Errorf("GetClusterByName %s failed, err: %v", config.DefaultClusterName, err)
		return err
	} else if err == nil {
		log.Infof("default cluster[%+v] has been created", clusterInfo)
		return nil
	}
	// create default cluster
	clusterInfo := &model.ClusterInfo{
		Name:        config.DefaultClusterName,
		Description: "default cluster",
		Endpoint:    "127.0.0.1",
		Source:      "",
		ClusterType: schema.KubernetesType,
		Version:     "1.16+",
		Status:      model.ClusterStatusOnLine,
	}
	if err := storage.Cluster.CreateCluster(clusterInfo); err != nil {
		log.Errorf("create default cluster failed, err: %v", err)
		return err
	}
	return nil
}
