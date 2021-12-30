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
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/common/uuid"
)

type ClusterCommonInfo struct {
	ID            string   `json:"clusterId"`     // 集群id
	Description   string   `json:"description"`   // 集群描述
	Endpoint      string   `json:"endpoint"`      // 集群endpoint, 比如 http://10.11.11.47:8080
	Source        string   `json:"source"`        // 来源, 比如 OnPremise （内部部署）、AWS、CCE
	ClusterType   string   `json:"clusterType"`   // 集群类型，比如kubernetes-v1.16
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
	models.ClusterInfo
}

type GetClusterResponse struct {
	models.ClusterInfo
}

type ListClusterResponse struct {
	common.MarkerInfo
	ClusterList []models.ClusterInfo `json:"clusterList"`
}

type UpdateClusterRequest struct {
	ClusterCommonInfo
}

type UpdateClusterReponse struct {
	models.ClusterInfo
}

type ClusterQuotaReponse struct {
	NodeQuotaInfoList []NodeQuotaInfo     `json:"nodeList"`
	Summary           ClusterQuotaSummary `json:"summary"`
	ErrMessage        string              `json:"errMsg"`
}

var (
	K8sClientMap           = map[string]*kubernetes.Clientset{}
	ClusterName2Credential = map[string]string{}
)

func GetK8sClientFromKubeConfigBytes(kubeConfigBytes []byte) (*kubernetes.Clientset, error) {
	c, err := config.InitKubeConfigFromBytes(kubeConfigBytes)
	if err != nil {
		return nil, err
	}
	clientSet, err := kubernetes.NewForConfig(c)

	if err != nil {
		log.Errorf("init config from kubeConfigBytes failed, error: %s", err.Error())
		return nil, err
	}
	return clientSet, nil
}

func GetK8sClient(clusterName, credential string) (*kubernetes.Clientset, error) {
	clientSet, clientSetFound := K8sClientMap[clusterName]
	c, credentialFound := ClusterName2Credential[clusterName]
	// 如果缓存里的credential和db中的credential一致，说明credential未改变，则直接返回缓存中的k8s client
	if clientSetFound && credentialFound && c == credential {
		return clientSet, nil
	}

	// credential base64 string 解码成 []byte
	credentialBytes, decodeErr := base64.StdEncoding.DecodeString(credential)
	if decodeErr != nil {
		errMsg := fmt.Sprintf("decode cluster[%s] credential base64 string error! msg: %s",
			clusterName, decodeErr.Error())
		return nil, errors.New(errMsg)
	}

	clientSet, err := GetK8sClientFromKubeConfigBytes(credentialBytes)
	if err != nil {
		return nil, err
	}
	K8sClientMap[clusterName] = clientSet
	// 更新缓存中的credential
	ClusterName2Credential[clusterName] = credential
	return clientSet, nil
}

func validateClusterStatus(clusterStatus string) error {
	validClusterStatusList := []string{
		models.ClusterStatusOnLine,
		models.ClusterStatusOffLine,
	}

	if !common.StringInSlice(clusterStatus, validClusterStatusList) {
		return errors.New("clusterStatus can only be 'online' or 'offline'")
	}
	return nil
}

func validateCreateClusterRequest(request *CreateClusterRequest) error {
	request.ID = strings.TrimSpace(request.ID)
	if request.ID == "" {
		request.ID = uuid.GenerateIDWithLength(common.PrefixCluster, 24)
	}

	request.Name = strings.TrimSpace(request.Name)
	if request.Name == "" {
		return errors.New("ClusterName should not be empty")
	}
	if len(request.Name) > util.ClusterNameMaxLength {
		return errors.New("The length of ClusterName should not exceed 255")
	}

	request.Endpoint = strings.TrimSpace(request.Endpoint)
	if request.Endpoint == "" {
		return errors.New("Endpoint should not be empty")
	}

	request.ClusterType = strings.TrimSpace(request.ClusterType)
	if request.ClusterType == "" {
		return errors.New("ClusterType should not be empty")
	}

	request.Status = strings.TrimSpace(request.Status)
	if request.Status == "" {
		request.Status = models.DefaultClusterStatus
	} else {
		if err := validateClusterStatus(request.Status); err != nil {
			return err
		}
	}

	request.Source = strings.TrimSpace(request.Source)
	if request.Source == "" {
		request.Source = models.DefaultClusterSource
	}

	request.Credential = strings.TrimSpace(request.Credential)
	request.Description = strings.TrimSpace(request.Description)
	request.Setting = strings.TrimSpace(request.Setting)

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

	if err := validateCreateClusterRequest(request); err != nil {
		ctx.Logging().Errorf("validateCreateClusterRequest failed, ClusterId: %s, ClusterName: %s",
			request.ID, clusterName)
		ctx.ErrorCode = common.InternalError
		return nil, err
	}

	clusterInfo := &models.ClusterInfo{
		ID:            request.ID,
		Name:          clusterName,
		Description:   request.Description,
		Endpoint:      request.Endpoint,
		Source:        request.Source,
		ClusterType:   request.ClusterType,
		Status:        request.Status,
		Credential:    request.Credential,
		Setting:       request.Setting,
		NamespaceList: request.NamespaceList,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	err := models.CreateCluster(ctx, clusterInfo)
	response := CreateClusterResponse{*clusterInfo}
	return &response, err
}

func IsLastClusterPk(ctx *logger.RequestContext, pk int64) bool {
	lastCluster, err := models.GetLastCluster(ctx)
	if err != nil {
		ctx.Logging().Errorf("get last cluster failed. error:[%s]", err.Error())
	}
	if lastCluster.Pk == pk {
		return true
	}
	return false
}

func ListCluster(ctx *logger.RequestContext, marker string, maxKeys int64,
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

	clusterList, err := models.ListCluster(ctx, pk, maxKeys, clusterNameList, clusterStatus)
	if err != nil {
		ctx.Logging().Errorf("models list cluster failed. err:[%s]", err.Error())
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

	clusterInfo, err := models.GetClusterByName(ctx, clusterName)
	if err != nil {
		ctx.ErrorCode = common.InternalError
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

	if err := models.DeleteCluster(ctx, clusterName); err != nil {
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

	clusterInfo, err := models.GetClusterByName(ctx, clusterName)
	if err != nil {
		ctx.ErrorCode = common.ClusterNameNotFound
		ctx.Logging().Errorf("get cluster failed. clusterName:[%s]", clusterName)
		return nil, err
	}

	clusterId := clusterInfo.ID
	if request.ID != "" {
		clusterInfo.ID = strings.TrimSpace(request.ID)
	}
	if request.Endpoint != "" {
		clusterInfo.Endpoint = strings.TrimSpace(request.Endpoint)
	}
	if request.ClusterType != "" {
		clusterInfo.ClusterType = strings.TrimSpace(request.ClusterType)
	}
	if request.Credential != "" {
		clusterInfo.Credential = strings.TrimSpace(request.Credential)
	}
	if request.Description != "" {
		clusterInfo.Description = strings.TrimSpace(request.Description)
	}
	if request.Source != "" {
		clusterInfo.Source = strings.TrimSpace(request.Source)
	}
	if request.Setting != "" {
		clusterInfo.Setting = strings.TrimSpace(request.Setting)
	}
	if request.Status != "" {
		request.Status = strings.TrimSpace(request.Status)
		if err := validateClusterStatus(request.Status); err != nil {
			return nil, err
		}
		clusterInfo.Status = request.Status
	}

	if len(request.NamespaceList) > 0 {
		clusterInfo.NamespaceList = request.NamespaceList
	}

	if err := models.UpdateCluster(ctx, clusterId, &clusterInfo); err != nil {
		ctx.ErrorCode = common.InternalError
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
	clusterList, err := models.ListCluster(ctx, 0, -1, clusterNameList, models.ClusterStatusOnLine)
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
		clientSet, err := GetK8sClient(c.Name, c.Credential)
		if err != nil {
			errMsg := fmt.Sprintf("clusterName: %s, errorMsg: %s", c.Name, err.Error())
			ctx.Logging().Errorf("get k8s client failed. %s", errMsg)
			response[c.Name] = ClusterQuotaReponse{
				ErrMessage:        errMsg,
				NodeQuotaInfoList: []NodeQuotaInfo{},
			}
			failedClusterErrorMsgList = append(failedClusterErrorMsgList, errMsg)
		} else {
			summary, nodeQuotaList := GetNodeQuotaList(clientSet)
			response[c.Name] = ClusterQuotaReponse{
				Summary:           summary,
				NodeQuotaInfoList: nodeQuotaList,
				ErrMessage:        "",
			}

		}
	}
	// 如果所有的集群都报错，则报错
	if len(failedClusterErrorMsgList) == len(clusterList) {
		return response, fmt.Errorf("%s", strings.Join(failedClusterErrorMsgList, "\n"))
	}
	return response, nil
}
