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
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/csiplugin/csiconfig"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	runtime "github.com/PaddlePaddle/PaddleFlow/pkg/job/runtime_v2"
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
var mountPodMutex sync.Mutex

// GetFileSystemService returns the instance of file system service
func GetFileSystemService() *FileSystemService {
	once.Do(func() {
		// default use db storage, mem used in the future maybe as the cache for db
		fileSystemService = new(FileSystemService)
	})
	return fileSystemService
}

type CreateFileSystemRequest struct {
	Name                    string            `json:"name"`
	Url                     string            `json:"url"`
	Properties              map[string]string `json:"properties"`
	Username                string            `json:"username"`
	IndependentMountProcess bool              `json:"independentMountProcess"`
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
	Id                      string            `json:"id"`
	Name                    string            `json:"name"`
	ServerAddress           string            `json:"serverAddress"`
	Type                    string            `json:"type"`
	SubPath                 string            `json:"subPath"`
	Username                string            `json:"username"`
	Properties              map[string]string `json:"properties"`
	IndependentMountProcess bool              `json:"independentMountProcess"`
}

type CreateFileSystemClaimsResponse struct {
	Message string `json:"message"`
}

func MountPodController(mountPodExpire, interval time.Duration,
	stopChan chan struct{}) {
	times := 0
	for {
		if times%3 == 0 {
			times = 0
			if err := cleanMountPod(mountPodExpire); err != nil {
				log.Errorf("clean mount pod err: %v", err)
			}
		}

		mountPodMutex.Lock()
		if err := scrapeCacheStats(); err != nil {
			log.Errorf("scrapeCacheStats err: %v", err)
		}
		mountPodMutex.Unlock()

		select {
		case <-stopChan:
			log.Info("mount pod controller stopped")
			return
		default:
			time.Sleep(interval)
			times++
		}
	}
}

func (s *FileSystemService) HasFsPermission(username, fsID string) (bool, error) {
	fsName, owner, err := utils.GetFsNameAndUserNameByFsID(fsID)
	if err != nil {
		return false, err
	}
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

// CreateFileSystem the function which performs the operation of creating FileSystem
func (s *FileSystemService) CreateFileSystem(ctx *logger.RequestContext, req *CreateFileSystemRequest) (model.FileSystem, error) {
	fsType, serverAddress, subPath := common.InformationFromURL(req.Url, req.Properties)
	fs := model.FileSystem{
		Name:                    req.Name,
		PropertiesMap:           req.Properties,
		ServerAddress:           serverAddress,
		Type:                    fsType,
		SubPath:                 subPath,
		UserName:                req.Username,
		IndependentMountProcess: req.IndependentMountProcess,
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
	isMounted, cleanPodMap, err := s.checkFsMountedAllClustersAndScheduledJobs(fsID)
	if err != nil {
		ctx.Logging().Errorf("checkFsMountedAllClustersAndScheduledJobs with fsID[%s] err: %v", fsID, err)
		return err
	}
	if isMounted {
		err := fmt.Errorf("fs[%s] is mounted. deletion is not allowed", fsID)
		ctx.Logging().Errorf(err.Error())
		ctx.ErrorCode = common.ActionNotAllowed
		return err
	}

	if err := s.cleanFsResources(cleanPodMap, fsID); err != nil {
		err := fmt.Errorf("fs[%s] cleanFsResources clean map: %+v, failed: %v", fsID, cleanPodMap, err)
		ctx.Logging().Errorf(err.Error())
		ctx.ErrorCode = common.InternalError
		return err
	}

	// delete filesystem, links, cache config in DB
	return storage.WithTransaction(storage.DB, func(tx *gorm.DB) error {
		// delete filesystem
		if err := storage.Filesystem.DeleteFileSystem(tx, fsID); err != nil {
			ctx.Logging().Errorf("delete fs[%s] err: %v", fsID, err)
			ctx.ErrorCode = common.FileSystemDataBaseError
			return err
		}
		// delete link if exists
		if err := storage.Filesystem.DeleteLinkWithFsID(tx, fsID); err != nil {
			ctx.Logging().Errorf("delete links with fsID[%s] err: %v", fsID, err)
			ctx.ErrorCode = common.FileSystemDataBaseError
			return err
		}
		// delete cache config if exists
		if err := storage.Filesystem.DeleteFSCacheConfig(tx, fsID); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			ctx.Logging().Errorf("delete cache config with fsID[%s] err: %v", fsID, err)
			ctx.ErrorCode = common.FileSystemDataBaseError
			return err
		}
		return nil
	})
}

func (s *FileSystemService) checkFsMountedAllClustersAndScheduledJobs(fsID string) (bool, map[*runtime.KubeRuntime][]k8sCore.Pod, error) {
	mountPodMutex.Lock()
	defer mountPodMutex.Unlock()
	// check fs used for pipeline scheduled jobs
	jobMap, err := models.ScheduleUsedFsIDs()
	if err != nil {
		err := fmt.Errorf("DeleteFileSystem GetUsedFsIDs for schecule failed: %v", err)
		log.Errorf(err.Error())
		return false, nil, err
	}
	_, exist := jobMap[fsID]
	if exist {
		log.Infof("fs[%s] is in use of pipeline scheduled jobs", fsID)
		return true, nil, nil
	}

	// check k8s mount pods
	clusters, err := storage.Cluster.ListCluster(0, 0, nil, "")
	if err != nil {
		err := fmt.Errorf("list clusters err: %v", err)
		log.Errorf("getClusterNamespaceMap failed: %v", err)
		return false, nil, err
	}

	if len(clusters) == 0 {
		return false, nil, nil
	}
	runtimePodsMap := make(map[*runtime.KubeRuntime][]k8sCore.Pod, 0)
	for _, cluster := range clusters {
		mounted, runtimePtr, podsToClean, err := checkFsMountedSingleCluster(cluster, fsID)
		if err != nil {
			err := fmt.Errorf("check fs[%s] mounted in cluster[%s] err: %v", fsID, cluster.Name, err)
			log.Errorf(err.Error())
			return false, nil, err
		}
		if mounted {
			return true, nil, nil
		}
		runtimePodsMap[runtimePtr] = podsToClean
	}
	return false, runtimePodsMap, nil
}

func checkFsMountedSingleCluster(cluster model.ClusterInfo, fsID string) (bool, *runtime.KubeRuntime, []k8sCore.Pod, error) {
	if cluster.ClusterType != schema.KubernetesType {
		log.Debugf("cluster[%s] type: %s, no need to investigate", cluster.Name, cluster.ClusterType)
		return false, nil, nil, nil
	}
	runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
	if err != nil {
		err := fmt.Errorf("cluster[%s] GetOrCreateRuntime err: %v", cluster.Name, err)
		log.Errorf(err.Error())
		return false, nil, nil, err
	}
	k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
	// label indicating a mount pod
	label := csiconfig.PodTypeKey + "=" + csiconfig.PodMount + "," + schema.LabelKeyFsID + "=" + fsID
	listOptions := k8sMeta.ListOptions{
		LabelSelector: label,
	}
	pods, err := k8sRuntime.ListPods(schema.MountPodNamespace, listOptions)
	if err != nil {
		log.Errorf("list mount pods failed: %v", err)
		return false, nil, nil, err
	}
	for _, po := range pods.Items {
		if checkMountPodMounted(po) {
			return true, nil, nil, nil
		}
	}
	return false, k8sRuntime, pods.Items, nil
}

func checkMountPodMounted(po k8sCore.Pod) bool {
	for key, targetPath := range po.Annotations {
		if strings.HasPrefix(key, schema.AnnotationKeyMountPrefix) {
			log.Debugf("pod[%s] mounted with key[%s] and target path[%s]", po.Name, key, targetPath)
			return true
		}
	}
	return false
}

func (s *FileSystemService) cleanFsResources(runtimePodsMap map[*runtime.KubeRuntime][]k8sCore.Pod, fsID string) (err error) {
	if err = deleteMountPods(runtimePodsMap); err != nil {
		err := fmt.Errorf("delete mount pods with fsID[%s] err: %v", fsID, err)
		log.Errorf(err.Error())
		return err
	}

	if err = storage.FsCache.Delete(fsID, ""); err != nil {
		err := fmt.Errorf("removeFSCache[%s] failed: %v", fsID, err)
		log.Error(err.Error())
		return err
	}

	if err = deletePvPvc(fsID); err != nil {
		err = fmt.Errorf("delete pv/pvc with fsID[%s] err: %v", fsID, err)
		log.Errorf(err.Error())
		return err
	}
	return nil
}

func deleteMountPods(podMap map[*runtime.KubeRuntime][]k8sCore.Pod) error {
	for k8sRuntime, pods := range podMap {
		for _, po := range pods {
			// delete pod
			if err := k8sRuntime.DeletePod(schema.MountPodNamespace, po.Name); err != nil && !k8sErrors.IsNotFound(err) {
				log.Errorf(fmt.Sprintf("deleteMountPods [%s] failed: %v", po.Name, err))
				return err
			}
		}
	}
	return nil
}

func cleanFSCache(podMap map[*runtime.KubeRuntime][]k8sCore.Pod) error {
	var err error
	for _, pods := range podMap {
		for _, pod := range pods {
			cacheID := pod.Labels[schema.LabelKeyCacheID]
			log.Debugf("cacheID is %v", cacheID)
			if cacheID == "" {
				log.Debugf("cacheId is empty with pod: %+v", pod)
				continue
			}

			if err = storage.FsCache.Delete("", cacheID); err != nil {
				err := fmt.Errorf("removeFSCacheWithCacheID[%s] failed: %v", cacheID, err)
				log.Error(err.Error())
				return err
			}
		}
	}
	return nil
}

func deletePvPvc(fsID string) error {
	cnm, err := getClusterNamespaceMap()
	if err != nil {
		err := fmt.Errorf("getClusterNamespaceMap failed: %v", err)
		log.Errorf(err.Error())
		return err
	}
	for k8sRuntime, namespaces := range cnm {
		for _, ns := range namespaces {
			if err = patchAndDeletePvcPv(k8sRuntime, ns, fsID); err != nil {
				err := fmt.Errorf("patchAndDeletePvcPv ns[%s] fsID[%s] failed: %v", ns, fsID, err)
				log.Errorf(err.Error())
				return err
			}
		}
	}
	return nil
}

func getClusterNamespaceMap() (map[*runtime.KubeRuntime][]string, error) {
	cnm := make(map[*runtime.KubeRuntime][]string)
	clusters, err := storage.Cluster.ListCluster(0, 0, nil, "")
	if err != nil {
		err := fmt.Errorf("list clusters err: %v", err)
		log.Errorf("getClusterNamespaceMap failed: %v", err)
		return nil, err
	}
	for _, cluster := range clusters {
		if cluster.ClusterType != schema.KubernetesType {
			log.Debugf("cluster[%s] type: %s, no need to delete pv pvc", cluster.Name, cluster.ClusterType)
			continue
		}
		runtimeSvc, err := runtime.GetOrCreateRuntime(cluster)
		if err != nil {
			err := fmt.Errorf("getClusterNamespaceMap: cluster[%s] GetOrCreateRuntime err: %v", cluster.Name, err)
			log.Errorf(err.Error())
			return nil, err
		}
		k8sRuntime := runtimeSvc.(*runtime.KubeRuntime)
		namespaces := cluster.NamespaceList
		if len(namespaces) == 0 { // cluster has no namespace restrictions. iterate all namespaces
			nsList, err := k8sRuntime.ListNamespaces(k8sMeta.ListOptions{})
			if err != nil {
				err := fmt.Errorf("getClusterNamespaceMap: cluster[%s] ListNamespaces err: %v", cluster.Name, err)
				log.Errorf(err.Error())
				return nil, err
			}
			for _, ns := range nsList.Items {
				if ns.Status.Phase == k8sCore.NamespaceActive {
					namespaces = append(namespaces, ns.Name)
				}
			}
			cnm[k8sRuntime] = namespaces
			log.Debugf("cluster[%s] namespaces: %v", cluster.Name, namespaces)
		}
	}
	return cnm, nil
}

func patchAndDeletePvcPv(k8sRuntime *runtime.KubeRuntime, namespace, fsID string) error {
	pvcName := schema.ConcatenatePVCName(fsID)
	pvc, err := k8sRuntime.GetPersistentVolumeClaims(namespace, pvcName, k8sMeta.GetOptions{})
	if k8sErrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		err := fmt.Errorf("get pvc[%s/%s] err: %v", namespace, pvcName, err)
		log.Errorf(err.Error())
		return err
	}

	if len(pvc.Finalizers) > 0 {
		if err := k8sRuntime.PatchPVCFinalizerNull(namespace, pvcName); err != nil {
			err := fmt.Errorf("PatchPVCFinalizerNull [%s/%s] err: %v", namespace, pvcName, err)
			log.Errorf(err.Error())
			return err
		}
	}

	// delete pvc manually. pv will be deleted automatically
	if err := k8sRuntime.DeletePersistentVolumeClaim(namespace, pvcName, k8sMeta.DeleteOptions{}); err != nil && !k8sErrors.IsNotFound(err) {
		err := fmt.Errorf("delete pvc[%s/%s] err: %v", namespace, pvcName, err)
		log.Errorf(err.Error())
		return err
	}
	// delete pv in case pv not deleted
	if err := k8sRuntime.DeletePersistentVolume(schema.ConcatenatePVName(namespace, fsID), k8sMeta.DeleteOptions{}); err != nil && !k8sErrors.IsNotFound(err) {
		err := fmt.Errorf("delete pv[%s] err: %v", schema.ConcatenatePVName(namespace, fsID), err)
		log.Errorf(err.Error())
		return err
	}
	return nil
}

func getPVCNamespaceFromMountPod(podName, fsID string) (string, error) {
	// pfs-{hostName}-pfs-{fsID}-{namespace}-pv
	strs := strings.Split(podName, fsID)
	if len(strs) < 2 {
		err := fmt.Errorf("mount pod name[%s] not valid to retrieve pvc info: no fsID", podName)
		log.Errorf(err.Error())
		return "", err
	}
	names := strings.Split(strs[len(strs)-1], "-")
	if len(names) < 2 || names[len(names)-1] != "pv" {
		err := fmt.Errorf("mount pod name[%s] not valid to retrieve pvc info", podName)
		log.Errorf(err.Error())
		return "", err
	}
	return names[len(names)-2], nil
}

// ListFileSystem the function which performs the operation of list file systems
func (s *FileSystemService) ListFileSystem(ctx *logger.RequestContext, req *ListFileSystemRequest) ([]model.FileSystem, string, error) {
	limit := req.MaxKeys + 1
	marker := req.Marker
	if req.Marker == "" {
		marker = time.Now().Format(model.TimeFormat)
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
		return items[:len(items)-1], items[len(items)-1].UpdatedAt.Format(model.TimeFormat), err
	}

	return items, "", err
}
