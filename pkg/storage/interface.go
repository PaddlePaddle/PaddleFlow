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

package storage

import (
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

var (
	DB *gorm.DB

	Filesystem FileSystemStoreInterface
	FsCache    FsCacheStoreInterface
	Auth       AuthStoreInterface
)

func InitStores(db *gorm.DB) {
	// do not use once.Do() because unit test need to init db twice
	Filesystem = newFilesystemStore(db)
	FsCache = newDBFSCache(db)
	Auth = newAuthStore(db)
}

type FileSystemStoreInterface interface {
	// filesystem
	CreatFileSystem(fs *model.FileSystem) error
	GetFileSystemWithFsID(fsID string) (model.FileSystem, error)
	DeleteFileSystem(tx *gorm.DB, id string) error
	ListFileSystem(limit int, userName, marker, fsName string) ([]model.FileSystem, error)
	GetSimilarityAddressList(fsType string, ips []string) ([]model.FileSystem, error)
	// link
	CreateLink(link *model.Link) error
	FsNameLinks(fsID string) ([]model.Link, error)
	LinkWithFsIDAndFsPath(fsID, fsPath string) (model.Link, error)
	DeleteLinkWithFsID(tx *gorm.DB, id string) error
	DeleteLinkWithFsIDAndFsPath(fsID, fsPath string) error
	ListLink(limit int, marker, fsID string) ([]model.Link, error)
	GetLinkWithFsIDAndPath(fsID, fsPath string) ([]model.Link, error)
	// fs_cache_config
	CreateFSCacheConfig(fsCacheConfig *model.FSCacheConfig) error
	UpdateFSCacheConfig(fsCacheConfig *model.FSCacheConfig) error
	DeleteFSCacheConfig(tx *gorm.DB, fsID string) error
	GetFSCacheConfig(fsID string) (model.FSCacheConfig, error)
}

// FsCacheStoreInterface currently has two implementations: DB and memory
// use newMemFSCache() or newDBFSCache(db *gorm.DB) to initiate
type FsCacheStoreInterface interface {
	Add(value *model.FSCache) error
	Get(fsID string, cacheID string) (*model.FSCache, error)
	Delete(fsID, cacheID string) error
	List(fsID, cacheID string) ([]model.FSCache, error)
	ListNodes(fsID []string) ([]string, error)
	Update(value *model.FSCache) (int64, error)
}

type AuthStoreInterface interface {
	// user
	CreateUser(ctx *logger.RequestContext, user *model.User) error
	UpdateUser(ctx *logger.RequestContext, userName, password string) error
	ListUser(ctx *logger.RequestContext, pk int64, maxKey int) ([]model.User, error)
	DeleteUser(ctx *logger.RequestContext, userName string) error
	GetUserByName(ctx *logger.RequestContext, userName string) (model.User, error)
	GetLastUser(ctx *logger.RequestContext) (model.User, error)
	// grant
	CreateGrant(ctx *logger.RequestContext, grant *model.Grant) error
	DeleteGrant(ctx *logger.RequestContext, userName, resourceType, resourceID string) error
	GetGrant(ctx *logger.RequestContext, userName, resourceType, resourceID string) (*model.Grant, error)
	HasAccessToResource(ctx *logger.RequestContext, resourceType string, resourceID string) bool
	DeleteGrantByUserName(ctx *logger.RequestContext, userName string) error
	DeleteGrantByResourceID(ctx *logger.RequestContext, resourceID string) error
	ListGrant(ctx *logger.RequestContext, pk int64, maxKeys int, userName string) ([]model.Grant, error)
	GetLastGrant(ctx *logger.RequestContext) (model.Grant, error)
}
