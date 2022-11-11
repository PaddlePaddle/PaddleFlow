package cluster

import (
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type ListClusterResourcesRequest struct {
	ClusterNameList []string `json:"clusterNames"`
	Labels          string   `json:"labels"`
	LabelType       string   `json:"labelType"`
	PageNo          int      `json:"pageNo"`
	PageSize        int      `json:"pageSize"`
}

type ListClusterByLabelResponse struct {
	Data []ClusterQuotaResponse `json:"data"`
}

type ClusterQuotaResponse struct {
	Allocatable map[string]map[string]interface{} `json:"allocatable"`
	Capacity    map[string]interface{}            `json:"capacity"`
	IsIsolation int                               `json:"isIsolation"`
	ClusterName string                            `json:"clusterName"`
}

// ListClusterQuotaByLabels return the node resources in clusters, lists can be filtered by labels in pods or nodes
func ListClusterQuotaByLabels(ctx *logger.RequestContext, req ListClusterResourcesRequest) (ListClusterByLabelResponse, error) {
	log.Infof("clusterName list req: %v", req)
	var response ListClusterByLabelResponse
	var dataList []ClusterQuotaResponse

	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("get cluster quota failed. error: admin is needed.")
		return response, errors.New("get cluster failed")
	}

	// 1. group by nodes to sum resource
	var err error
	var result []model.ResourceInfoResponse
	if result, err = storage.ResourceCache.ListResouces(req.ClusterNameList, req.Labels, req.LabelType, req.PageSize, req.PageNo); err != nil {
		err = fmt.Errorf("list resources from cache failed, err: %v", err.Error())
		log.Errorln(err)
		return response, err
	}
	// 2. build resources
	clusterMap, err := buildClusterResponse(result)
	if err != nil {
		log.Errorln(err)
		return response, err
	}
	if len(clusterMap) == 0 {
		return ListClusterByLabelResponse{Data: dataList}, nil
	}
	// 3. convert map to list
	for _, v := range clusterMap {
		dataList = append(dataList, v)
	}
	maxIndex := len(dataList)
	maxPageSize := (maxIndex + req.PageSize - 1) / req.PageSize
	// req.PageNo ranges in [1, maxPageSize]
	req.PageNo = req.PageNo - 1
	if req.PageNo > maxPageSize || req.PageNo < 0 {
		err = fmt.Errorf("list out of range when pageNo %d and pageSize %d", req.PageNo, req.PageSize)
		log.Errorln(err)
		return response, err
	}
	startIndex := req.PageNo * req.PageSize
	endIndex := (req.PageNo + 1) * req.PageSize
	if endIndex > maxIndex {
		endIndex = maxIndex
	}
	log.Infof("len(dataList): %d, startIndex: %d, endIndex: %d, maxIndex: %d, maxPageSize: %d", len(dataList), startIndex, endIndex, maxIndex, maxPageSize)
	dataList = dataList[startIndex:endIndex]
	log.Infof("len(dataList): %d, startIndex: %d, endIndex: %d, maxIndex: %d, maxPageSize: %d", len(dataList), startIndex, endIndex, maxIndex, maxPageSize)
	return ListClusterByLabelResponse{Data: dataList}, nil
}

func buildClusterResponse(result []model.ResourceInfoResponse) (map[string]ClusterQuotaResponse, error) {
	log.Infof("cluster list queried from db, result: %v", result)
	var err error
	clusterMap := map[string]ClusterQuotaResponse{}
	for _, resInfo := range result {
		cName := resInfo.ClusterName
		if _, exist := clusterMap[cName]; !exist {
			clusterMap[cName] = ClusterQuotaResponse{
				ClusterName: cName,
				Allocatable: make(map[string]map[string]interface{}),
				Capacity:    make(map[string]interface{}),
			}
		}
		cluster := clusterMap[cName]

		// todo  capacity结构未最终确定
		var capacity interface{}
		err = json.Unmarshal([]byte(resInfo.CapacityJSON), &capacity)
		if err != nil {
			log.Errorf("list cluster resources by label failed, unmarshal failed, err: %v", err)
			return nil, err
		}
		cluster.Capacity[resInfo.NodeName] = capacity
		// todo allocatable 需要用capacity去做减法, 由于结构未最终确定,暂时展示为已使用资源量
		// todo 单位换算,加上m或者kb
		if _, exist := cluster.Allocatable[resInfo.NodeName]; !exist {
			cluster.Allocatable[resInfo.NodeName] = make(map[string]interface{})
		}
		nodeAllocatableRes := cluster.Allocatable[resInfo.NodeName]
		nodeAllocatableRes[resInfo.ResourceName] = resInfo.Value
	}
	return clusterMap, nil
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
