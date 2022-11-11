package cluster

import (
	"encoding/json"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
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

type ListClusterResourceResponse struct {
	Data []ClusterQuotaResponse `json:"data"`
}

type ClusterQuotaResponse struct {
	Allocatable map[string]map[string]int64  `json:"allocatable"`
	Capacity    map[string]map[string]string `json:"capacity"`
	IsIsolation int                          `json:"isIsolation"`
	ClusterName string                       `json:"clusterName"`
}

// ListClusterQuotaByLabels return the node resources in clusters, lists can be filtered by labels in pods or nodes
func ListClusterQuotaByLabels(ctx *logger.RequestContext, req ListClusterResourcesRequest) (ListClusterResourceResponse, error) {
	log.Infof("clusterName list req: %v", req)
	var response ListClusterResourceResponse
	var dataList []ClusterQuotaResponse

	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("get cluster quota failed. error: admin is needed.")
		return response, errors.New("get cluster failed")
	}

	// 1. group by nodes to sum resource
	var err error
	var result []model.ResourceInfoResponse
	if result, err = storage.ResourceCache.ListResouces(req.ClusterNameList, req.Labels, req.LabelType); err != nil {
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
		return ListClusterResourceResponse{Data: dataList}, nil
	}
	// 3. convert map to list
	for _, v := range clusterMap {
		dataList = append(dataList, v)
	}
	// 4. page limit
	dataList, err = slicePages(dataList, req.PageSize, req.PageNo)
	if err != nil {
		log.Errorln(err)
		return response, err
	}
	// calculate allocatable resource by capacity sub used
	dataList, err = updateAllocatable(dataList)
	if err != nil {
		log.Errorf("get allocatable resource failed, err: %v", err)
		return response, err
	}

	return ListClusterResourceResponse{Data: dataList}, nil
}

// buildClusterResponse convert sql results to map
func buildClusterResponse(result []model.ResourceInfoResponse) (map[string]ClusterQuotaResponse, error) {
	log.Infof("cluster list queried from db, result: %v", result)
	var err error
	clusterMap := map[string]ClusterQuotaResponse{}
	for _, resInfo := range result {
		cName := resInfo.ClusterName
		if _, exist := clusterMap[cName]; !exist {
			clusterMap[cName] = ClusterQuotaResponse{
				ClusterName: cName,
				Allocatable: make(map[string]map[string]int64),
				Capacity:    make(map[string]map[string]string),
			}
		}
		cluster := clusterMap[cName]

		// todo  capacity结构未最终确定,暂时使用map
		if _, exist := cluster.Capacity[resInfo.NodeName]; !exist {
			var capacity map[string]string
			err = json.Unmarshal([]byte(resInfo.CapacityJSON), &capacity)
			if err != nil {
				log.Errorf("list cluster resources by label failed, unmarshal failed, err: %v", err)
				return nil, err
			}
			cluster.Capacity[resInfo.NodeName] = capacity
		}
		// todo allocatable 需要用capacity去做减法, 由于结构未最终确定,暂时展示为已使用资源量
		// todo 单位换算,加上m或者kb
		if _, exist := cluster.Allocatable[resInfo.NodeName]; !exist {
			cluster.Allocatable[resInfo.NodeName] = make(map[string]int64)
		}
		//allocatableValue, err := getAllocatableRes(capacity, resInfo)
		nodeAllocatableRes := cluster.Allocatable[resInfo.NodeName]
		nodeAllocatableRes[resInfo.ResourceName] = resInfo.Value
		cluster.Allocatable[resInfo.NodeName] = nodeAllocatableRes
		// save back single cluster data
		clusterMap[cName] = cluster
	}
	return clusterMap, nil
}

func slicePages(dataList []ClusterQuotaResponse, pageSize, pageNo int) ([]ClusterQuotaResponse, error) {
	maxIndex := len(dataList)
	maxPageSize := (maxIndex + pageSize - 1) / pageSize
	// pageNo ranges in [1, maxPageSize]
	pageNo = pageNo - 1
	if pageNo > maxPageSize || pageNo < 0 {
		err := fmt.Errorf("list out of range when pageNo %d and pageSize %d", pageNo, pageSize)
		log.Errorln(err)
		return dataList, err
	}
	startIndex := pageNo * pageSize
	endIndex := (pageNo + 1) * pageSize
	if endIndex > maxIndex {
		endIndex = maxIndex
	}
	log.Debugf("dataList number: %d, startIndex: %d, endIndex: %d, maxIndex: %d, maxPageSize: %d",
		len(dataList), startIndex, endIndex, maxIndex, maxPageSize)
	dataList = dataList[startIndex:endIndex]
	return dataList, nil
}

func updateAllocatable(dataList []ClusterQuotaResponse) ([]ClusterQuotaResponse, error) {
	cpu := "cpu"
	memory := "memory"
	for i, cluster := range dataList {
		for nodeName, used := range cluster.Allocatable {
			capacity := cluster.Capacity[nodeName]
			allocatable := cluster.Allocatable[nodeName]
			cpuCapacity, err := resource.ParseQuantity(capacity[cpu])
			if err != nil {
				log.Errorln(err)
				return dataList, err
			}
			memoryCapacity, err := resource.ParseQuantity(capacity[memory])
			if err != nil {
				log.Errorln(err)
				return dataList, err
			}
			// todo GPU
			cpuAllocatable := cpuCapacity.MilliValue() - used[cpu]
			memoryAllocatable := memoryCapacity.Value() - used[cpu]
			// save back
			allocatable[cpu] = cpuAllocatable
			allocatable[memory] = memoryAllocatable
			cluster.Allocatable[nodeName] = allocatable
		}
		dataList[i] = cluster
	}
	return dataList, nil
}
