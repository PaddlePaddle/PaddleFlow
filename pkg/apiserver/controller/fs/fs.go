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
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	k8sCore "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	k8sMeta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

const (
	TimeFormat = "2006-01-02 15:04:05"
)

// obsoleted funcs: create/delete PVC/PVC code can be found in commit 23e7038cecd7bfa9acdc80bbe1d62d904dbe1568

// FileSystemService the service which contains the operation of file system
type FileSystemService struct{}

var fileSystemService *FileSystemService
var once sync.Once

// GetFileSystemService returns the instance of file system service
func GetFileSystemService() *FileSystemService {
	once.Do(func() {
		// default use db storage, mem used in the future maybe as the cache for db
		fileSystemService = new(FileSystemService)
	})
	return fileSystemService
}

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

// CreateFileSystem the function which performs the operation of creating FileSystem
func (s *FileSystemService) CreateFileSystem(ctx *logger.RequestContext, req *CreateFileSystemRequest) (model.FileSystem, error) {
	fsType, serverAddress, subPath := common.InformationFromURL(req.Url, req.Properties)
	fs := model.FileSystem{
		Name:          req.Name,
		PropertiesMap: req.Properties,
		ServerAddress: serverAddress,
		Type:          fsType,
		SubPath:       subPath,
		UserName:      req.Username,
	}
	fs.ID = common.ID(req.Username, req.Name)

	err := storage.Filesystem.CreatFileSystem(&fs)
	if err != nil {
		log.Errorf("create file system[%v] in db failed: %v", fs, err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return model.FileSystem{}, err
	}
	return fs, nil
}

// GetFileSystem the function which performs the operation of getting file system detail
func (s *FileSystemService) GetFileSystem(username, fsName string) (model.FileSystem, error) {
	modelsFs, err := storage.Filesystem.GetFileSystemWithFsID(common.ID(username, fsName))
	if err != nil {
		log.Errorf("get filesystem[%s] under username[%s] err[%v]", fsName, username, err)
		return model.FileSystem{}, err
	}
	return modelsFs, err
}

// DeleteFileSystem the function which performs the operation of delete file system
func (s *FileSystemService) DeleteFileSystem(ctx *logger.RequestContext, fsID string) error {
	return models.WithTransaction(storage.DB, func(tx *gorm.DB) error {
		if err := storage.Filesystem.DeleteFileSystem(tx, fsID); err != nil {
			ctx.Logging().Errorf("delete fs[%s] failed error[%v]", fsID, err)
			ctx.ErrorCode = common.FileSystemDataBaseError
			return err
		}
		// delete cache config if exist
		if err := storage.Filesystem.DeleteFSCacheConfig(tx, fsID); err != nil {
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

func (s *FileSystemService) HasFsPermission(username, fsID string) (bool, error) {
	fsName, owner := fsCommon.FsIDToFsNameUsername(fsID)
	fs, err := s.GetFileSystem(owner, fsName)
	if err != nil {
		return false, err
	}
	if common.IsRootUser(username) || fs.UserName == username {
		return true, nil
	} else {
		return false, nil
	}
}

func DeletePvPvc(fsID string) error {
	clusters, err := models.ListCluster(0, 0, nil, "")
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
			nsList, err := k8sRuntime.ListNamespaces(k8sMeta.ListOptions{})
			if err != nil {
				log.Errorf("DeletePvPvc: cluster[%s] ListNamespaces err: %v", cluster.Name, err)
				return err
			}
			if nsList == nil {
				log.Errorf("DeletePvPvc: cluster[%s] ListNamespaces nil", cluster.Name)
				return fmt.Errorf("clust[%s] namespace list nil", cluster.Name)
			}
			for _, ns := range nsList.Items {
				if ns.Status.Phase == k8sCore.NamespaceActive {
					namespaces = append(namespaces, ns.Name)
				}
			}
			log.Debugf("clust[%s] all namespaces: %v", cluster.Name, namespaces)
		}
		for _, ns := range namespaces {
			// delete pvc manually. pv will be deleted automatically
			if err := k8sRuntime.DeletePersistentVolumeClaim(ns, schema.ConcatenatePVCName(fsID), k8sMeta.DeleteOptions{}); err != nil && !k8sErrors.IsNotFound(err) {
				log.Errorf("delete pvc[%s/%s] err: %v", ns, schema.ConcatenatePVCName(fsID), err)
				return fmt.Errorf("delete pvc[%s-%s] err: %v", ns, schema.ConcatenatePVCName(fsID), err)
			}
		}
	}
	return nil
}

// ListFileSystem the function which performs the operation of list file systems
func (s *FileSystemService) ListFileSystem(ctx *logger.RequestContext, req *ListFileSystemRequest) ([]model.FileSystem, string, error) {
	limit := req.MaxKeys + 1
	marker := req.Marker
	if req.Marker == "" {
		marker = time.Now().Format(TimeFormat)
	}
	listUserName := req.Username
	if req.Username == common.UserRoot {
		listUserName = ""
	}

	items, err := storage.Filesystem.ListFileSystem(int(limit), listUserName, marker, req.FsName)
	if err != nil {
		ctx.Logging().Errorf("list file systems err[%v]", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return nil, "", err
	}

	itemsLen := len(items)
	if itemsLen == 0 {
		return []model.FileSystem{}, "", err
	}
	if itemsLen > int(req.MaxKeys) {
		return items[:len(items)-1], items[len(items)-1].UpdatedAt.Format(TimeFormat), err
	}

	return items, "", err
}
