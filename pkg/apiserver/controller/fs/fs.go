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
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	k8score "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	k8smeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/service/db_service"
)

const (
	TimeFormat = "2006-01-02 15:04:05"
)

// obsoleted funcs: create/delete PVC/PVC code can be found in commit 23e7038cecd7bfa9acdc80bbe1d62d904dbe1568

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

	err := db_service.CreatFileSystem(&fs)
	if err != nil {
		log.Errorf("create file system[%v] in db failed: %v", fs, err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return models.FileSystem{}, err
	}
	return fs, nil
}

// GetFileSystem the function which performs the operation of getting file system detail
func (s *FileSystemService) GetFileSystem(fsID string) (models.FileSystem, error) {
	modelsFs, err := db_service.GetFileSystemWithFsID(fsID)
	if err != nil {
		log.Errorf("get file system err[%v]", err)
		return models.FileSystem{}, err
	}
	return modelsFs, err
}

// DeleteFileSystem the function which performs the operation of delete file system
func (s *FileSystemService) DeleteFileSystem(ctx *logger.RequestContext, fsID string) error {
	return db_service.WithTransaction(database.DB, func(tx *gorm.DB) error {
		if err := db_service.DeleteFileSystem(tx, fsID); err != nil {
			ctx.Logging().Errorf("delete fs[%s] failed error[%v]", fsID, err)
			ctx.ErrorCode = common.FileSystemDataBaseError
			return err
		}
		// delete cache config if exist
		if err := db_service.DeleteFSCacheConfig(tx, fsID); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			ctx.Logging().Errorf("delete fs[%s] cache config failed error[%v]", fsID, err)
			ctx.ErrorCode = common.FileSystemDataBaseError
			return err
		}
		if err := DeletePvPvc(fsID); err != nil {
			ctx.Logging().Errorf("delete deletePvPvc for fs[%s] err: %v", fsID, err)
			return err
		}
		return nil
	})
}

func DeletePvPvc(fsID string) error {
	clusters, err := db_service.ListCluster(0, 0, nil, "")
	if err != nil {
		return fmt.Errorf("list clusters failed")
	}
	for _, cluster := range clusters {
		if cluster.ClusterType != schema.KubernetesType {
			log.Debugf("cluster[%s] type: %s, no need to delete pv pvc", cluster.Name, cluster.ClusterType)
			continue
		}
		runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
		if err != nil {
			log.Errorf("DeletePvPvc: cluster[%s] GetOrCreateRuntime err: %v", cluster.Name, err)
			return err
		}
		k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
		namespaces := cluster.NamespaceList
		if len(namespaces) == 0 { // cluster has no namespace restrictions. iterate all namespaces
			nsList, err := k8sRuntime.ListNamespaces(k8smeta.ListOptions{})
			if err != nil {
				log.Errorf("DeletePvPvc: cluster[%s] ListNamespaces err: %v", cluster.Name, err)
				return err
			}
			if nsList == nil {
				log.Errorf("DeletePvPvc: cluster[%s] ListNamespaces nil", cluster.Name)
				return fmt.Errorf("clust[%s] namespace list nil", cluster.Name)
			}
			for _, ns := range nsList.Items {
				if ns.Status.Phase == k8score.NamespaceActive {
					namespaces = append(namespaces, ns.Name)
				}
			}
			log.Debugf("clust[%s] all namespaces: %v", cluster.Name, namespaces)
		}
		for _, ns := range namespaces {
			// delete pvc manually. pv will be deleted automatically
			if err := k8sRuntime.DeletePersistentVolumeClaim(ns, schema.ConcatenatePVCName(fsID), k8smeta.DeleteOptions{}); err != nil && !k8serrors.IsNotFound(err) {
				log.Errorf("delete pvc[%s/%s] err: %v", ns, schema.ConcatenatePVCName(fsID), err)
				return fmt.Errorf("delete pvc[%s-%s] err: %v", ns, schema.ConcatenatePVCName(fsID), err)
			}
		}
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

	items, err := db_service.ListFileSystem(int(limit), listUserName, marker, req.FsName)
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
