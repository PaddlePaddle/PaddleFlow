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
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/schema"
)

const customFlavour = "customFlavour"

// CreateFlavourRequest convey request for create flavour
type CreateFlavourRequest struct {
	Name            string                     `json:"name"`
	ClusterName     string                     `json:"clusterName,omitempty"`
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
	FlavourName string `json:"name"`
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
		FlavourName: flavour.Name,
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

	if request.ClusterName != flavour.ClusterName {
		errMsg := fmt.Sprintf("not support operate to update flavour[%s]'s cluster", request.Name)
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	isChanged := false
	if request.CPU != "" && request.CPU != flavour.CPU {
		isChanged = true
		flavour.CPU = request.CPU
	}
	if request.Mem != "" && request.Mem != flavour.Mem {
		isChanged = true
		flavour.Mem = request.Mem
	}
	if flavour.ScalarResources == nil {
		flavour.ScalarResources = make(schema.ScalarResourcesType)
	}
	if len(request.ScalarResources) != 0 || request.ScalarResources == nil {
		for resourceName, resource := range request.ScalarResources {
			isChanged = true
			flavour.ScalarResources[resourceName] = resource
		}
	} else {
		log.Debugf("flavour %s scalarResources is set nil", flavour.Name)
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
		cluster, err := models.GetClusterByName(clusterName)
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
	response.FlavourList = append(response.FlavourList, flavours...)

	return &response, nil
}

// DeleteFlavour handler for deleting flavour
func DeleteFlavour(flavourName string, userID int64) error {
	if err := models.DeleteFlavour(flavourName); err != nil {
		log.Errorf("delete flavour %s failed, err: %v", flavourName, err)
		return err
	}

	return nil
}

// IsLastFlavourPk get last flavour that usually be used for indicating last page
func IsLastFlavourPk(pk int64) bool {
	lastFlavour, err := models.GetLastFlavour()
	if err != nil {
		log.Errorf("get last flavour failed. error:[%s]", err.Error())
	}
	if lastFlavour.Pk == pk {
		return true
	}
	return false
}

// GetFlavourWithCheck get req.Flavour and check if it is valid, if exists in db, return it
func GetFlavourWithCheck(reqFlavour schema.Flavour) (schema.Flavour, error) {
	if reqFlavour.Name == "" || reqFlavour.Name == customFlavour {
		if err := schema.ValidateResourceInfo(reqFlavour.ResourceInfo, config.GlobalServerConfig.Job.ScalarResourceArray); err != nil {
			log.Errorf("validate resource info failed, err:%v", err)
			return schema.Flavour{}, err
		}
		return reqFlavour, nil
	}
	flavour, err := models.GetFlavour(reqFlavour.Name)
	if err != nil {
		log.Errorf("Get flavour by name %s failed when creating job, err:%v", reqFlavour.Name, err)
		return schema.Flavour{}, fmt.Errorf("get flavour[%s] failed, err:%v", reqFlavour.Name, err)
	}
	return schema.Flavour{
		Name: flavour.Name,
		ResourceInfo: schema.ResourceInfo{
			CPU:             flavour.CPU,
			Mem:             flavour.Mem,
			ScalarResources: flavour.ScalarResources,
		},
	}, nil
}
