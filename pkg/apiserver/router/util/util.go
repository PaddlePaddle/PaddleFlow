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

package util

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

const (
	PaddleflowRouterPrefix    = "/api/paddleflow"
	PaddleflowRouterVersionV1 = "/v1"

	DefaultMaxKeys = 50
	ListPageMax    = 1000
	MaxDescLength  = 1024

	ParamKeyQueueName        = "queueName"
	ParamKeyRunID            = "runID"
	ParamKeyCheckCache       = "checkCache"
	ParamKeyRunCacheID       = "runCacheID"
	ParamKeyPipelineID       = "pipelineID"
	ParamKeyPipelineDetailID = "pipelineDetailID"
	ParamKeyScheduleID       = "scheduleID"

	QueryKeyAction    = "action"
	QueryActionStop   = "stop"
	QueryActionRetry  = "retry"
	QueryActionDelete = "delete"
	QueryActionCreate = "create"
	QueryActionModify = "modify"

	QueryKeyMarker  = "marker"
	QueryKeyMaxKeys = "maxKeys"

	QueryKeyPipelineID      = "pipelineID"
	QueryKeyScheduleFilter  = "scheduleFilter"
	QueryKeyStatusFilter    = "statusFilter"
	QueryKeyUserFilter      = "userFilter"
	QueryKeyFsFilter        = "fsFilter"
	QueryKeyPplFilter       = "pplFilter"
	QueryKeyPplDetailFilter = "pplDetailFilter"
	QueryKeyNameFilter      = "nameFilter"
	QueryKeyRunFilter       = "runFilter"
	QueryKeyTypeFilter      = "typeFilter"
	QueryKeyPathFilter      = "pathFilter"
	QueryKeyUser            = "user"
	QueryKeyName            = "name"
	QueryKeyUserName        = "username"
	QueryResourceType       = "resourceType"
	QueryResourceID         = "resourceID"
	QueryKeyStatus          = "status"
	QueryKeyTimestamp       = "timestamp"
	QueryKeyStartTime       = "startTime"
	QueryKeyQueue           = "queue"
	QueryKeyLabels          = "labels"

	ParamKeyClusterName   = "clusterName"
	ParamKeyClusterNames  = "clusterNames"
	ParamKeyClusterStatus = "clusterStatus"

	QueryFsPath     = "fsPath"
	QueryFsName     = "fsName"
	QueryFsname     = "fsname"
	QueryPath       = "path"
	QueryClusterID  = "clusterID"
	QueryNodeName   = "nodename"
	QueryMountPoint = "mountpoint"

	ParamFlavourName = "flavourName"

	// cluster name最大长度
	ClusterNameMaxLength = 255

	ParamKeyName            = "name"
	ParamKeyNamespace       = "namespace"
	ParamKeyKind            = "kind"
	ParamKeyAPIVersion      = "apiVersion"
	ParamKeyJobID           = "jobID"
	ParamKeyPageNo          = "pageNo"
	ParamKeyPageSize        = "pageSize"
	ParamKeyLogFilePosition = "logFilePosition"
)

func GetQueryMaxKeys(ctx *logger.RequestContext, r *http.Request) (int, error) {
	maxKeys := DefaultMaxKeys
	queryMaxKeys := r.URL.Query().Get(QueryKeyMaxKeys)
	if queryMaxKeys != "" {
		var err error
		maxKeys, err = strconv.Atoi(queryMaxKeys)
		if err != nil || maxKeys <= 0 || maxKeys > ListPageMax {
			ctx.ErrorCode = common.InvalidURI
			return 0, common.InvalidMaxKeysError(queryMaxKeys)
		}
	}
	return maxKeys, nil
}

func SplitFilter(strFilter string, splitter string, toStrip bool) (filterList []string) {
	splitRes := strings.Split(strFilter, splitter)
	if toStrip {
		for _, item := range splitRes {
			trimItem := strings.TrimSpace(item)
			if trimItem != "" {
				filterList = append(filterList, trimItem)
			}
		}
	}

	return filterList
}
