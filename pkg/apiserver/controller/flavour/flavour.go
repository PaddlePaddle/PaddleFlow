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

package flavour

import (
	"reflect"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

// CreateFlavourRequest convey request for create flavour
type CreateFlavourRequest struct {
	Name            string                     `json:"name"`
	ClusterName     string                     `json:"clusterName"`
	ClusterID       string                     `json:"-"`
	CPU             string                     `json:"cpu"`
	Mem             string                     `json:"mem"`
	ScalarResources schema.ScalarResourcesType `json:"scalarResources,omitempty"`
	UserName        string                     `json:"-"`
}

// UpdateFlavourRequest convey request for update flavour
type UpdateFlavourRequest struct {
	Name            string                     `json:"-"`
	ClusterName     string                     `json:"clusterName,omitempty"`
	ClusterID       string                     `json:"-"`
	CPU             string                     `json:"cpu,omitempty"`
	Mem             string                     `json:"mem,omitempty"`
	ScalarResources schema.ScalarResourcesType `json:"scalarResources,omitempty"`
	UserName        string                     `json:"-"`
}

// CreateFlavourResponse convey response for create flavour
type CreateFlavourResponse struct {
	models.Flavour
}

// UpdateFlavourResponse convey response for update flavour
type UpdateFlavourResponse struct {
	models.Flavour
}

// ListFlavourResponse convey response for list flavour
type ListFlavourResponse struct {
	common.MarkerInfo
	FlavourList []models.Flavour `json:"flavourList"`
}

// CreateFlavour handler for creating flavour
func CreateFlavour(request *CreateFlavourRequest) (*CreateFlavourResponse, error) {
	flavour := models.Flavour{
		Name:            request.Name,
		CPU:             request.CPU,
		Mem:             request.Mem,
		ScalarResources: request.ScalarResources,
		ClusterID:       request.ClusterID,
		ClusterName:     request.ClusterName,
		UserName:        request.UserName,
	}
	if err := models.CreateFlavour(&flavour); err != nil {
		return nil, err
	}

	response := &CreateFlavourResponse{
		Flavour: flavour,
	}
	return response, nil
}

// UpdateFlavour handler for updating flavour
func UpdateFlavour(request *UpdateFlavourRequest) (*UpdateFlavourResponse, error) {
	flavour, err := GetFlavour(request.Name)
	if err != nil {
		log.Errorf("get flavour %s failed when update", request.Name)
		return nil, err
	}
	isChanged := false
	if request.CPU != flavour.CPU {
		isChanged = true
		flavour.CPU = request.CPU
	}
	if request.Mem != flavour.Mem {
		isChanged = true
		flavour.Mem = request.Mem
	}
	if !reflect.DeepEqual(request.ScalarResources, flavour.ScalarResources) {
		isChanged = true
		flavour.ScalarResources = request.ScalarResources
	}

	if request.ClusterName != flavour.ClusterName {
		isChanged = true
		flavour.ClusterName = request.ClusterName
	}

	if isChanged {
		log.Debugf("field changed, update flavour %s to %v", flavour.Name, flavour)
		if err := models.UpdateFlavour(&flavour); err != nil {
			log.Errorf("update flavour in db failed, err=%v", err)
			return nil, err
		}
	}

	return &UpdateFlavourResponse{flavour}, nil
}

// GetFlavour handler for getting flavour
func GetFlavour(name string) (models.Flavour, error) {
	return models.GetFlavour(name)
}

// ListFlavour handler for listing flavour
func ListFlavour(maxKeys int, marker, clusterName, queryKey string) (*ListFlavourResponse, error) {
	log.Debug("begin list flavour.")
	response := ListFlavourResponse{}
	response.IsTruncated = false

	var pk int64
	if marker != "" {
		var err error
		pk, err = common.DecryptPk(marker)
		if err != nil {
			log.Errorf("DecryptPk marker[%s] failed. err:[%s]",
				marker, err.Error())
			return &response, err
		}
	}
	var clusterID string
	if clusterName != "" {
		ctx := &logger.RequestContext{}
		cluster, err := models.GetClusterByName(ctx, clusterName)
		if err != nil {
			log.Errorf("cluster %s not found, err=%v", clusterName, err)
			return nil, err
		}
		clusterID = cluster.ID
	}

	flavours, err := models.ListFlavour(pk, maxKeys, clusterID, queryKey)
	if err != nil {
		log.Errorf("models list flavour failed. err:[%s]", err.Error())
		return &response, err
	}

	if len(flavours) > 0 {
		cluster := flavours[len(flavours)-1]
		if !IsLastFlavourPk(cluster.Pk) {
			nextMarker, err := common.EncryptPk(cluster.Pk)
			if err != nil {
				log.Errorf("EncryptPk error. pk:[%d] error:[%s] ", cluster.Pk, err.Error())
				return &response, err
			}
			response.NextMarker = nextMarker
			response.IsTruncated = true
		}
	}
	response.MaxKeys = maxKeys
	for _, cluster := range flavours {
		response.FlavourList = append(response.FlavourList, cluster)
	}

	return &response, nil
}

// DeleteFlavour handler for deleting flavour
func DeleteFlavour(flavourName string, userID int64) error {
	if err := models.DeleteFlavour(flavourName); err != nil {
		return err
	}

	return nil
}

// IsLastFlavourPk get last flavour that usually be used for indicating last page
func IsLastFlavourPk(pk int64) bool {
	lastQueue, err := models.GetLastFlavour()
	if err != nil {
		log.Errorf("get last flavour failed. error:[%s]", err.Error())
	}
	if lastQueue.Pk == pk {
		return true
	}
	return false
}
