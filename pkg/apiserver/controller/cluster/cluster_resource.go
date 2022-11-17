package cluster

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/k8s"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

type ListClusterResourcesRequest struct {
	ClusterNameList []string `json:"clusterNames"`
	Labels          string   `json:"labels"`
	LabelType       string   `json:"-"`
	PageNo          int      `json:"pageNo"`
	PageSize        int      `json:"pageSize"`
}

type NodeResourcesResponse struct {
	Allocatable map[string]map[string]interface{} `json:"allocatable"`
	Capacity    map[string]map[string]string      `json:"capacity"`
	Labels      map[string]map[string]string      `json:"labels"`
	ClusterName string                            `json:"clusterName"`
}

// ListClusterResources return the node resources in clusters, lists can be filtered by labels in pods or nodes
func ListClusterResources(ctx *logger.RequestContext, req ListClusterResourcesRequest) (map[string]NodeResourcesResponse, error) {
	log.Infof("list cluster resources request: %v", req)
	if !common.IsRootUser(ctx.UserName) {
		ctx.ErrorCode = common.OnlyRootAllowed
		ctx.Logging().Errorln("list cluster resources failed. error: admin is needed.")
		return nil, errors.New("list cluster resources failed")
	}

	// 1. list nodes
	offset := (req.PageNo - 1) * req.PageSize
	nodes, err := storage.NodeCache.ListNode(req.ClusterNameList, req.Labels, req.PageSize, offset)
	if err != nil {
		err = fmt.Errorf("list node from cache failed, err: %v", err.Error())
		ctx.Logging().Errorln(err)
		return nil, err
	}
	ctx.Logging().Debugf("list nodes: %+v", nodes)
	var nodeLists []string
	for i := range nodes {
		nodeLists = append(nodeLists, nodes[i].ID)
	}
	// 2. list node resources
	result, err := storage.ResourceCache.ListNodeResources(nodeLists)
	if err != nil {
		err = fmt.Errorf("list node resources from cache failed, err: %v", err.Error())
		ctx.Logging().Errorln(err)
		return nil, err
	}
	ctx.Logging().Debugf("list node resources: %v", result)
	// 3. construct response
	return ConstructClusterResources(nodes, result)
}

func ConstructClusterResources(nodes []model.NodeInfo, nodeResources []model.ResourceInfo) (map[string]NodeResourcesResponse, error) {
	var nodeUsed = map[string]map[string]int64{}
	for _, rInfo := range nodeResources {
		nodeUsedResources, find := nodeUsed[rInfo.NodeID]
		if !find {
			nodeUsedResources = make(map[string]int64)
			nodeUsed[rInfo.NodeID] = nodeUsedResources
		}
		nodeUsedResources[rInfo.Name] = rInfo.Value
	}

	var err error
	var clusterResources = map[string]NodeResourcesResponse{}
	for _, node := range nodes {
		cQuotaResponse, find := clusterResources[node.ClusterName]
		if !find {
			cQuotaResponse = NodeResourcesResponse{
				Allocatable: make(map[string]map[string]interface{}),
				Capacity:    make(map[string]map[string]string),
				Labels:      make(map[string]map[string]string),
				ClusterName: node.ClusterName,
			}
		}
		cQuotaResponse.Capacity[node.Name] = node.Capacity
		cQuotaResponse.Labels[node.Name] = node.Labels
		// set node allocatable
		used, ok := nodeUsed[node.ID]
		if !ok {
			used = map[string]int64{}
		}
		log.Debugf("node %s used resources: %+v", node.Name, used)
		allocatable, err := getAllocatable(node.Capacity, used)
		if err != nil {
			break
		}
		cQuotaResponse.Allocatable[node.Name] = allocatable

		clusterResources[node.ClusterName] = cQuotaResponse
	}
	log.Debugf("cluster resources: %+v", clusterResources)
	return clusterResources, err
}

func getAllocatable(nodeCapacity map[string]string, usedResources map[string]int64) (map[string]interface{}, error) {
	capacity, err := resources.NewResourceFromMap(nodeCapacity)
	if err != nil {
		log.Errorf("new resources from node capacity %v failed, err: %v", nodeCapacity, err)
		return nil, err
	}
	isGPUX := false
	for rName := range nodeCapacity {
		if k8s.IsGPUX(rName) {
			isGPUX = true
		}
	}
	used := resources.EmptyResource()
	for rName, rValue := range usedResources {
		if k8s.IsGPUX(rName) {
			isGPUX = true
		}
		used.SetResources(rName, rValue)
	}

	if isGPUX {
		return k8s.SubWithGPUX(capacity, usedResources), nil
	} else {
		capacity.Sub(used)
		return capacity.ToMap(), nil
	}
}
