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
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/resources"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

var (
	DB *gorm.DB

	Pipeline   PipelineStoreInterface
	Filesystem FileSystemStoreInterface
	FsCache    FsCacheStoreInterface
	Auth       AuthStoreInterface
	Cluster    ClusterStoreInterface
	Flavour    FlavourStoreInterface
	Queue      QueueStoreInterface
	Job        JobStoreInterface
	Image      ImageStoreInterface
	Artifact   ArtifactStoreInterface
)

func InitStores(db *gorm.DB) {
	// do not use once.Do() because unit test need to init db twice
	Pipeline = newPipelineStore(db)
	Filesystem = newFilesystemStore(db)
	FsCache = newDBFSCache(db)
	Auth = newAuthStore(db)
	Cluster = newClusterStore(db)
	Flavour = newFlavourStore(db)
	Job = newJobStore(db)
	Queue = newQueueStore(db)
	Image = newImageStore(db)
	Artifact = newRunArtifactStore(db)
}

type ArtifactStoreInterface interface {
	CreateArtifactEvent(logEntry *log.Entry, artifact model.ArtifactEvent) error
	CountArtifactEvent(logEntry *log.Entry, fsID, artifactPath string) (int64, error)
	GetArtifactEvent(logEntry *log.Entry, runID, fsID, artifactPath string) (model.ArtifactEvent, error)
	UpdateArtifactEvent(logEntry *log.Entry, fsID, artifactPath string, artifact model.ArtifactEvent) error
	DeleteArtifactEvent(logEntry *log.Entry, username, fsname, runID, artifactPath string) error
	ListArtifactEvent(logEntry *log.Entry, pk int64, maxKeys int, userFilter, fsFilter, runFilter, typeFilter, pathFilter []string) ([]model.ArtifactEvent, error)
	GetLastArtifactEvent(logEntry *log.Entry) (model.ArtifactEvent, error)
}

type QueueStoreInterface interface {
	CreateQueue(queue *model.Queue) error
	CreateOrUpdateQueue(queue *model.Queue) error
	UpdateQueue(queue *model.Queue) error
	UpdateQueueStatus(queueName string, queueStatus string) error
	UpdateQueueInfo(name, status string, max, min *resources.Resource) error
	DeleteQueue(queueName string) error
	IsQueueExist(queueName string) bool
	GetQueueByName(queueName string) (model.Queue, error)
	GetQueueByID(queueID string) (model.Queue, error)
	ListQueue(pk int64, maxKeys int, queueName string, userName string) ([]model.Queue, error)
	GetLastQueue() (model.Queue, error)
	ListQueuesByCluster(clusterID string) []model.Queue
	IsQueueInUse(queueID string) (bool, map[string]schema.JobStatus)
	DeepCopyQueue(queueSrc model.Queue, queueDesc *model.Queue)
}

type ClusterStoreInterface interface {
	CreateCluster(clusterInfo *model.ClusterInfo) error
	ListCluster(pk int64, maxKeys int, clusterNameList []string, clusterStatus string) ([]model.ClusterInfo, error)
	GetLastCluster() (model.ClusterInfo, error)
	GetClusterByName(clusterName string) (model.ClusterInfo, error)
	GetClusterById(clusterId string) (model.ClusterInfo, error)
	DeleteCluster(clusterName string) error
	UpdateCluster(clusterId string, clusterInfo *model.ClusterInfo) error
	ActiveClusters() []model.ClusterInfo
}

type FlavourStoreInterface interface {
	CreateFlavour(flavour *model.Flavour) error
	DeleteFlavour(flavourName string) error
	GetFlavour(flavourName string) (model.Flavour, error)
	ListFlavour(pk int64, maxKeys int, clusterID, queryKey string) ([]model.Flavour, error)
	UpdateFlavour(flavour *model.Flavour) error
	GetLastFlavour() (model.Flavour, error)
}

type PipelineStoreInterface interface {
	// pipeline
	CreatePipeline(logEntry *log.Entry, ppl *model.Pipeline, pplVersion *model.PipelineVersion) (pplID string, pplVersionID string, err error)
	UpdatePipeline(logEntry *log.Entry, ppl *model.Pipeline, pplVersion *model.PipelineVersion) (pplID string, pplVersionID string, err error)
	GetPipelineByID(id string) (model.Pipeline, error)
	GetPipeline(name, userName string) (model.Pipeline, error)
	ListPipeline(pk int64, maxKeys int, userFilter, nameFilter []string) ([]model.Pipeline, error)
	IsLastPipelinePk(logEntry *log.Entry, pk int64, userFilter, nameFilter []string) (bool, error)
	DeletePipeline(logEntry *log.Entry, id string) error
	// pipeline_version
	ListPipelineVersion(pipelineID string, pk int64, maxKeys int, fsFilter []string) ([]model.PipelineVersion, error)
	IsLastPipelineVersionPk(logEntry *log.Entry, pipelineID string, pk int64, fsFilter []string) (bool, error)
	CountPipelineVersion(pipelineID string) (int64, error)
	GetPipelineVersions(pipelineID string) ([]model.PipelineVersion, error)
	GetPipelineVersion(pipelineID string, pipelineVersionID string) (model.PipelineVersion, error)
	GetLastPipelineVersion(pipelineID string) (model.PipelineVersion, error)
	DeletePipelineVersion(logEntry *log.Entry, pipelineID string, pipelineVersionID string) error
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
	ListFSCacheConfig(fsIDs []string) ([]model.FSCacheConfig, error)
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

type JobStoreInterface interface {
	// job
	CreateJob(job *model.Job) error
	GetJobByID(jobID string) (model.Job, error)
	GetUnscopedJobByID(jobID string) (model.Job, error)
	GetJobStatusByID(jobID string) (schema.JobStatus, error)
	DeleteJob(jobID string) error
	UpdateJobStatus(jobId, errMessage string, newStatus schema.JobStatus) error
	UpdateJobConfig(jobId string, conf *schema.Conf) error
	UpdateJob(jobID string, status schema.JobStatus, runtimeInfo, runtimeStatus interface{}, message string) (schema.JobStatus, error)
	ListQueueJob(queueID string, status []schema.JobStatus) []model.Job
	ListQueueInitJob(queueID string) []model.Job
	ListJobsByQueueIDsAndStatus(queueIDs []string, status schema.JobStatus) []model.Job
	ListJobByStatus(status schema.JobStatus) []model.Job
	GetJobsByRunID(runID string, jobID string) ([]model.Job, error)
	ListJobByUpdateTime(updateTime string) ([]model.Job, error)
	ListJobByParentID(parentID string) ([]model.Job, error)
	GetLastJob() (model.Job, error)
	ListJob(pk int64, maxKeys int, queue, status, startTime, timestamp, userFilter string, labels map[string]string) ([]model.Job, error)
	// job_lable
	ListJobIDByLabels(labels map[string]string) ([]string, error)
	// job_task
	GetJobTaskByID(id string) (model.JobTask, error)
	UpdateTask(task *model.JobTask) error
	ListByJobID(jobID string) ([]model.JobTask, error)
}

type ImageStoreInterface interface {
	CreateImage(logEntry *log.Entry, image *model.Image) error
	ListImageIDsByFsID(logEntry *log.Entry, fsID string) ([]string, error)
	GetImage(logEntry *log.Entry, PFImageID string) (model.Image, error)
	GetUrlByPFImageID(logEntry *log.Entry, PFImageID string) (string, error)
	UpdateImage(logEntry *log.Entry, PFImageID string, image model.Image) error
}
