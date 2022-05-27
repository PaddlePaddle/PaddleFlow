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

package fs

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	apiv1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/k8s"
)

const (
	TimeFormat = "2006-01-02 15:04:05"
)

// FileSystemService the service which contains the operation of file system
type FileSystemService struct{}

type CreateFileSystemRequest struct {
	Name       string            `json:"name"`
	Url        string            `json:"url"`
	Properties map[string]string `json:"properties"`
	Username   string            `json:"username"`
}

type ListFileSystemRequest struct {
	Marker   string `json:"marker"`
	MaxKeys  int32  `json:"maxKeys"`
	Username string `json:"username"`
	FsName   string `json:"fsName"`
}

type GetFileSystemRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
}

type DeleteFileSystemRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
}

type GetFileSystemResponse struct {
	Id            string            `json:"id"`
	Name          string            `json:"name"`
	ServerAddress string            `json:"serverAddress"`
	Type          string            `json:"type"`
	SubPath       string            `json:"subPath"`
	Username      string            `json:"username"`
	Properties    map[string]string `json:"properties"`
}

type CreateFileSystemClaimsRequest struct {
	Namespaces []string `json:"namespaces"`
	FsIDs      []string `json:"fsIDs"`
}

type CreateFileSystemResponse struct {
	FsName string `json:"fsName"`
	FsID   string `json:"fsID"`
}

type ListFileSystemResponse struct {
	Marker     string                `json:"marker"`
	Truncated  bool                  `json:"truncated"`
	NextMarker string                `json:"nextMarker"`
	FsList     []*FileSystemResponse `json:"fsList"`
}

type FileSystemResponse struct {
	Id            string            `json:"id"`
	Name          string            `json:"name"`
	ServerAddress string            `json:"serverAddress"`
	Type          string            `json:"type"`
	SubPath       string            `json:"subPath"`
	Username      string            `json:"username"`
	Properties    map[string]string `json:"properties"`
}

type CreateFileSystemClaimsResponse struct {
	Message string `json:"message"`
}

var fileSystemService *FileSystemService

// GetFileSystemService returns the instance of file system service
func GetFileSystemService() *FileSystemService {
	if fileSystemService == nil {
		fileSystemService = &FileSystemService{}
	}
	return fileSystemService
}

// CreateFileSystem the function which performs the operation of creating FileSystem
func (s *FileSystemService) CreateFileSystem(ctx *logger.RequestContext, req *CreateFileSystemRequest) (models.FileSystem, error) {
	fsType, serverAddress, subPath := common.InformationFromURL(req.Url, req.Properties)
	fs := models.FileSystem{
		Name:          req.Name,
		PropertiesMap: req.Properties,
		ServerAddress: serverAddress,
		Type:          fsType,
		SubPath:       subPath,
		UserName:      req.Username,
	}
	fs.ID = common.ID(req.Username, req.Name)

	err := models.CreatFileSystem(&fs)
	if err != nil {
		log.Errorf("create file system[%v] in db failed: %v", fs, err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return models.FileSystem{}, err
	}
	return fs, nil
}

// GetFileSystem the function which performs the operation of getting file system detail
func (s *FileSystemService) GetFileSystem(fsID string) (models.FileSystem, error) {
	modelsFs, err := models.GetFileSystemWithFsID(fsID)
	if err != nil {
		log.Errorf("get file system err[%v]", err)
		return models.FileSystem{}, err
	}
	return modelsFs, err
}

// DeleteFileSystem the function which performs the operation of delete file system
func (s *FileSystemService) DeleteFileSystem(ctx *logger.RequestContext, fsID string) error {
	err := models.DeleteFileSystem(fsID)
	if err != nil {
		ctx.Logging().Errorf("delete fs[%s] failed error[%v]", fsID, err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}
	// delete cache config if exist
	err = models.DeleteFSCacheConfig(ctx.Logging(), fsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		ctx.Logging().Errorf("delete fs[%s] cache config failed error[%v]", fsID, err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}
	return nil
}

// ListFileSystem the function which performs the operation of list file systems
func (s *FileSystemService) ListFileSystem(ctx *logger.RequestContext, req *ListFileSystemRequest) ([]models.FileSystem, string, error) {
	limit := req.MaxKeys + 1
	marker := req.Marker
	if req.Marker == "" {
		marker = time.Now().Format(TimeFormat)
	}
	listUserName := req.Username
	if req.Username == common.UserRoot {
		listUserName = ""
	}

	items, err := models.ListFileSystem(int(limit), listUserName, marker, req.FsName)
	if err != nil {
		ctx.Logging().Errorf("list file systems err[%v]", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return nil, "", err
	}

	itemsLen := len(items)
	if itemsLen == 0 {
		return []models.FileSystem{}, "", err
	}
	if itemsLen > int(req.MaxKeys) {
		return items[:len(items)-1], items[len(items)-1].UpdatedAt.Format(TimeFormat), err
	}

	return items, "", err
}

// CreateFileSystemClaims the function which performs the operation of creating FileSystem claims
func (s *FileSystemService) CreateFileSystemClaims(ctx *logger.RequestContext, req *CreateFileSystemClaimsRequest) error {
	if len(req.Namespaces) == 0 || len(req.FsIDs) == 0 {
		return nil
	}
	fsModel, err := models.GetFsWithIDs(req.FsIDs)
	if err != nil {
		ctx.Logging().Errorf("get fs modelss failed: %v", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}
	if len(req.FsIDs) != len(fsModel) {
		ctx.Logging().Errorf("get fs modelss failed: %v, modelss: %v", req.FsIDs, fsModel)
		ctx.ErrorCode = common.FileSystemDataBaseError

		var notExistFsIDs []string
		count := make(map[string]int)
		for _, fsmodels := range fsModel {
			count[fsmodels.ID]++
		}
		for _, fsID := range req.FsIDs {
			if count[fsID] == 0 {
				notExistFsIDs = append(notExistFsIDs, fsID)
			}
		}
		return common.InvalidField("fsIDs", fmt.Sprintf("fs %v is not exist", notExistFsIDs))
	}

	for k, fsID := range req.FsIDs {
		for _, ns := range req.Namespaces {
			if fsModel[k].Type == fsCommon.MockType {
				continue
			}
			var pv string
			userName := fsModel[k].UserName
			if pv, err = createPV(ns, fsID, userName); err != nil {
				ctx.Logging().Errorf("create PV with file system[%v] in namespace[%v] failed: %v",
					fsID, ns, err)
				ctx.ErrorCode = common.K8sOperatorError
				return err
			}
			if err = createPVC(ns, fsID, pv); err != nil {
				ctx.Logging().Errorf("create PVC with file system[%v] in namespace[%v] failed: %v",
					fsID, ns, err)
				ctx.ErrorCode = common.K8sOperatorError
				return err
			}
		}
	}
	return nil
}

// TODO: remove to kubernetes runtime
func deletePVC(fsID string) error {
	k8sOperator := k8s.GetK8sOperator()
	nsList, err := k8sOperator.ListNamespaces(metav1.ListOptions{})
	if err != nil {
		log.Errorf("list namespaces when clean pvc failed: %v", err)
		return err
	}
	log.Debugf("namespace list %v", nsList)
	pvc := config.DefaultPVC
	pvcName := strings.Replace(pvc.Name, schema.FSIDFormat, fsID, -1)
	log.Debugf("delete pvc name:%s", pvcName)
	propagationPolicy := metav1.DeletePropagationBackground
	deleteOptions := &metav1.DeleteOptions{PropagationPolicy: &propagationPolicy}
	for _, item := range nsList.Items {
		ns := item.Name

		if _, errK8sOperator := k8sOperator.GetPersistentVolumeClaim(ns, pvcName, metav1.GetOptions{}); k8serrors.IsNotFound(errK8sOperator) {
			continue
		} else if errK8sOperator != nil && !k8serrors.IsNotFound(errK8sOperator) {
			log.Errorf("k8sOperator GetPersistentVolumeClaim err[%v]", errK8sOperator)
			return errK8sOperator
		}

		if err := k8sOperator.DeletePersistentVolumeClaim(ns, pvcName, deleteOptions); err != nil {
			log.Errorf("delete pvc[%s/%s] failed: %v", ns, pvc, err)
			return err
		}
	}
	return nil
}

// TODO: remove to kubernetes runtime
func createPV(namespace, fsId, userName string) (string, error) {
	k8sOperator := k8s.GetK8sOperator()
	pv := config.DefaultPV
	// format pvname to fsid
	pvName := strings.Replace(pv.Name, schema.FSIDFormat, fsId, -1)
	pvName = strings.Replace(pvName, schema.NameSpaceFormat, namespace, -1)
	// check pv existence
	if _, err := k8sOperator.GetPersistentVolume(pvName, metav1.GetOptions{}); err == nil {
		return "", nil
	} else if !k8serrors.IsNotFound(err) {
		return "", err
	}
	// construct a new pv
	newPV := &apiv1.PersistentVolume{}
	if err := copier.Copy(newPV, pv); err != nil {
		return "", err
	}
	newPV.Name = pvName
	csi := newPV.Spec.CSI
	if csi != nil && csi.VolumeAttributes != nil {
		if _, ok := csi.VolumeAttributes[schema.FSID]; ok {
			newPV.Spec.CSI.VolumeAttributes[schema.FSID] = fsId
			newPV.Spec.CSI.VolumeHandle = pvName
		}
		if _, ok := csi.VolumeAttributes[schema.PFSUserName]; ok {
			newPV.Spec.CSI.VolumeAttributes[schema.PFSUserName] = userName
		}
		if _, ok := csi.VolumeAttributes[schema.PFSServer]; ok {
			newPV.Spec.CSI.VolumeAttributes[schema.PFSServer] = fmt.Sprintf("%s:%d", config.GlobalServerConfig.Fs.K8sServiceName, config.GlobalServerConfig.Fs.K8sServicePort)
		}
	}
	// create pv in k8s
	if _, err := k8sOperator.CreatePersistentVolume(newPV); err != nil {
		return "", err
	}
	return pvName, nil
}

// TODO: remove to kubernetes runtime
func createPVC(namespace, fsId, pv string) error {
	k8sOperator := k8s.GetK8sOperator()
	pvc := config.DefaultPVC
	pvcName := strings.Replace(pvc.Name, schema.FSIDFormat, fsId, -1)
	// check pvc existence
	if _, err := k8sOperator.GetPersistentVolumeClaim(namespace, pvcName, metav1.GetOptions{}); err == nil {
		return nil
	} else if !k8serrors.IsNotFound(err) {
		return err
	}
	// construct a new pvc
	newPVC := &apiv1.PersistentVolumeClaim{}
	if err := copier.Copy(newPVC, pvc); err != nil {
		return err
	}
	newPVC.Namespace = namespace
	newPVC.Name = pvcName
	newPVC.Spec.VolumeName = pv
	// create pvc in k8s
	if _, err := k8sOperator.CreatePersistentVolumeClaim(namespace, newPVC); err != nil {
		return err
	}
	return nil
}
