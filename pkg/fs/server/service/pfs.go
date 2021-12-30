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

package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/jinzhu/copier"
	log "github.com/sirupsen/logrus"
	apiv1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/server/api/common"
	"paddleflow/pkg/fs/server/api/request"
	utils "paddleflow/pkg/fs/server/utils/fs"
	"paddleflow/pkg/fs/utils/k8s"
)

const (
	TimeFormat      = "2006-01-02 15:04:05"
	FSIDFormat      = "$(pfs.fs.id)"
	NameSpaceFormat = "$(namespace)"
	FSID            = "pfs.fs.id"
	PFSServer       = "pfs.server"
	PFSUserName     = "pfs.user.name"
)

// FileSystemService the service which contains the operation of file system
type FileSystemService struct{}

var fileSystemService *FileSystemService

// GetFileSystemService returns the instance of file system service
func GetFileSystemService() *FileSystemService {
	if fileSystemService == nil {
		fileSystemService = &FileSystemService{}
	}
	return fileSystemService
}

// CreateStorage the function which performs the operation of creating FileSystem
func (s *FileSystemService) CreateFileSystem(ctx *logger.RequestContext, req *request.CreateFileSystemRequest) (models.FileSystem, error) {
	fsType, serverAddress, subPath := utils.InformationFromURL(req.Url, req.Properties)
	fs := models.FileSystem{
		Name:          req.Name,
		PropertiesMap: req.Properties,
		ServerAddress: serverAddress,
		Type:          fsType,
		SubPath:       subPath,
		UserName:      req.Username,
	}
	fs.ID = utils.ID(req.Username, req.Name)

	err := models.CreatFileSystem(&fs)
	if err != nil {
		log.Errorf("create file system[%v] in db failed: %v", fs, err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return models.FileSystem{}, err
	}
	return fs, nil
}

// GetFileSystem the function which performs the operation of getting file system detail
func (s *FileSystemService) GetFileSystem(req *request.GetFileSystemRequest, fsID string) (models.FileSystem, error) {
	if req.Username == utils.UserRoot {
		req.Username = ""
	}
	modelsFs, err := models.GetFileSystemWithFsIDAndUserName(fsID, req.Username)
	if err != nil {
		log.Errorf("get file system err[%v]", err)
		return models.FileSystem{}, err
	}
	if modelsFs.ID == "" {
		log.Errorf("get file system empty with username[%s] fsid[%s]", req.Username, fsID)
		return models.FileSystem{}, common.New("Get file system is empty")
	}
	return modelsFs, err
}

// DeleteFileSystem the function which performs the operation of delete file system
func (s *FileSystemService) DeleteFileSystem(ctx *logger.RequestContext, fsID string) error {
	err := deletePVC(fsID)
	if err != nil {
		ctx.Logging().Errorf("delete pvc error[%v]", err)
		ctx.ErrorCode = common.K8sOperatorError
		return err
	}
	err = models.DeleteFileSystem(fsID)
	if err != nil {
		ctx.Logging().Errorf("delete failed error[%v]", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}
	return err
}

// ListFileSystem the function which performs the operation of list file systems
func (s *FileSystemService) ListFileSystem(ctx *logger.RequestContext, req *request.ListFileSystemRequest, isRoot bool) ([]models.FileSystem, string, error) {
	limit := req.MaxKeys + 1
	marker := req.Marker
	if req.Marker == "" {
		marker = time.Now().Format(TimeFormat)
	}
	if isRoot == true {
		req.Username = ""
	}

	items, err := models.ListFileSystem(int(limit), req.Username, marker, req.FsName)
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
func (s *FileSystemService) CreateFileSystemClaims(ctx *logger.RequestContext, req *request.CreateFileSystemClaimsRequest) error {
	if len(req.Namespaces) == 0 || len(req.FsIDs) == 0 {
		return nil
	}
	fsmodelss, err := models.GetFsWithIDs(req.FsIDs)
	if err != nil {
		ctx.Logging().Errorf("get fs modelss failed: %v", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}
	if len(req.FsIDs) != len(fsmodelss) {
		ctx.Logging().Errorf("get fs modelss failed: %v, modelss: %v", req.FsIDs, fsmodelss)
		ctx.ErrorCode = common.FileSystemDataBaseError

		var notExistFsIDs []string
		count := make(map[string]int)
		for _, fsmodels := range fsmodelss {
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
			if fsmodelss[k].Type == base.MockType {
				continue
			}
			var pv string
			userName := fsmodelss[k].UserName
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

func deletePVC(fsID string) error {
	k8sOperator := k8s.GetK8sOperator()
	nsList, err := k8sOperator.ListNamespaces(metav1.ListOptions{})
	if err != nil {
		log.Errorf("list namespaces when clean pvc failed: %v", err)
		return err
	}
	log.Debugf("namespace list %v", nsList)
	pvc := config.DefaultPVC
	pvcName := strings.Replace(pvc.Name, FSIDFormat, fsID, -1)
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

func createPV(namespace, fsId, userName string) (string, error) {
	k8sOperator := k8s.GetK8sOperator()
	pv := config.DefaultPV
	// format pvname to fsid
	pvName := strings.Replace(pv.Name, FSIDFormat, fsId, -1)
	pvName = strings.Replace(pvName, NameSpaceFormat, namespace, -1)
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
		if _, ok := csi.VolumeAttributes[FSID]; ok {
			newPV.Spec.CSI.VolumeAttributes[FSID] = fsId
			newPV.Spec.CSI.VolumeHandle = pvName
		}
		if _, ok := csi.VolumeAttributes[PFSUserName]; ok {
			newPV.Spec.CSI.VolumeAttributes[PFSUserName] = userName
		}
		if _, ok := csi.VolumeAttributes[PFSServer]; ok {
			newPV.Spec.CSI.VolumeAttributes[PFSServer] = fmt.Sprintf("%s:%d", config.GlobalServerConfig.Fs.K8sServiceName, config.GlobalServerConfig.Fs.K8sServicePort)
		}
	}
	// create pv in k8s
	if _, err := k8sOperator.CreatePersistentVolume(newPV); err != nil {
		return "", err
	}
	return pvName, nil
}
func createPVC(namespace, fsId, pv string) error {
	k8sOperator := k8s.GetK8sOperator()
	pvc := config.DefaultPVC
	pvcName := strings.Replace(pvc.Name, FSIDFormat, fsId, -1)
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
