package csi

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	k8sCore "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

const (
	VolumesKeyMount     = "pfs-mount"
	VolumesKeyDataCache = "data-cache"
	VolumesKeyMetaCache = "meta-cache"

	FusePodMountPoint = schema.FusePodMntDir + "/storage"
	FusePodCachePath  = "/home/paddleflow/pfs-cache"
	DataCacheDir      = "/data-cache"
	MetaCacheDir      = "/meta-cache"
	CacheWorkerBin    = "/home/paddleflow/cache-worker"

	ContainerNameCacheWorker = "cache-worker"
	ContainerNamePfsMount    = "pfs-mount"

	mountName                             = "mount"
	PfsFuseIndependentMountProcessCMDName = "/home/paddleflow/mount.sh"
	pfsFuseMountPodCMDName                = "/home/paddleflow/pfs-fuse mount"
	ReadOnly                              = "ro"
)

var umountLock sync.RWMutex

type Info struct {
	CacheConfig model.FSCacheConfig
	FS          model.FileSystem
	FSBase64Str string
	TargetPath  string
	SourcePath  string
	Cmd         string
	Args        []string
	ReadOnly    bool
	K8sClient   utils.Client
	PodResource corev1.ResourceRequirements
}

func mountInPod(volumeContext map[string]string, mountPath, volumeID string, readOnly bool) error {
	k8sClient, err := utils.GetK8sClient()
	if err != nil {
		log.Errorf("get k8s client failed: %v", err)
		return err
	}

	mountInfo, err := ConstructMountInfo(volumeContext[schema.PFSInfo], volumeContext[schema.PFSCache],
		mountPath, k8sClient, readOnly)
	if err != nil {
		log.Errorf("ConstructMountInfo err: %v", err)
		return err
	}
	log.Infof("Node publish mountInfo [%+v]", mountInfo)

	// business pods use a separate source path
	if err = PFSMount(volumeID, mountInfo); err != nil {
		log.Errorf("MountThroughPod err: %v", err)
		return err
	}
	return nil
}

func ConstructMountInfo(fsInfoBase64, fsCacheBase64, targetPath string, k8sClient utils.Client, readOnly bool) (Info, error) {
	// FS info
	fs, err := utils.ProcessFSInfo(fsInfoBase64)
	if err != nil {
		retErr := fmt.Errorf("FSprocess FS info err: %v", err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}

	// FS CacheConfig config
	cacheConfig, err := utils.ProcessCacheConfig(fsCacheBase64)
	if err != nil {
		retErr := fmt.Errorf("FS process FS CacheConfig err: %v", err)
		log.Errorf(retErr.Error())
		return Info{}, retErr
	}

	info := Info{
		CacheConfig: cacheConfig,
		FS:          fs,
		FSBase64Str: fsInfoBase64,
		TargetPath:  targetPath,
		ReadOnly:    readOnly,
		K8sClient:   k8sClient,
	}

	if !fs.IndependentMountProcess && fs.Type != common.GlusterFSType {
		info.SourcePath = schema.GetBindSource(info.FS.ID)
		info.PodResource, err = ParsePodResources(cacheConfig.Resource.CpuLimit, cacheConfig.Resource.MemoryLimit)
		if err != nil {
			err := fmt.Errorf("ParsePodResources: %+v err: %v", cacheConfig.Resource, err)
			log.Errorf(err.Error())
			return Info{}, err
		}
	} else {
		info.SourcePath = utils.GetSourceMountPath(filepath.Dir(info.TargetPath))
	}
	info.Cmd, info.Args = info.cmdAndArgs()
	return info, nil
}

func (mountInfo *Info) cmdAndArgs() (string, []string) {
	if mountInfo.FS.Type == common.GlusterFSType {
		return mountName, mountInfo.glusterArgs()
	} else if mountInfo.FS.IndependentMountProcess {
		return PfsFuseIndependentMountProcessCMDName, mountInfo.processMountArgs()
	} else {
		return pfsFuseMountPodCMDName, mountInfo.podMountArgs()
	}
}

func (mountInfo *Info) glusterArgs() (args []string) {
	args = append(args, "-t", mountInfo.FS.Type,
		strings.Join([]string{mountInfo.FS.ServerAddress, mountInfo.FS.SubPath}, ":"), mountInfo.SourcePath)
	return args
}

func (mountInfo *Info) processMountArgs() (args []string) {
	args = append(args, mountInfo.commonOptions()...)
	args = append(args, mountInfo.cachePathArgs(true)...)
	args = append(args, fmt.Sprintf("--%s=%s", "mount-point", mountInfo.SourcePath))
	return args
}

func (mountInfo *Info) podMountArgs() (args []string) {
	args = append(args, fmt.Sprintf("--%s=%s", "mount-point", FusePodMountPoint))
	args = append(args, mountInfo.commonOptions()...)
	args = append(args, mountInfo.cachePathArgs(false)...)
	return args
}

func (mountInfo *Info) cachePathArgs(independentProcess bool) (args []string) {
	cacheDir := ""
	if independentProcess {
		cacheDir = mountInfo.CacheConfig.CacheDir
	} else {
		cacheDir = FusePodCachePath
	}
	hasCache := false
	if mountInfo.CacheConfig.CacheDir != "" {
		hasCache = true
		args = append(args, fmt.Sprintf("--%s=%s", "data-cache-path", cacheDir+DataCacheDir))
	}
	if mountInfo.CacheConfig.MetaDriver != schema.FsMetaMemory &&
		mountInfo.CacheConfig.CacheDir != "" {
		hasCache = true
		args = append(args, fmt.Sprintf("--%s=%s", "meta-cache-path", cacheDir+MetaCacheDir))
	}

	if hasCache && mountInfo.CacheConfig.CleanCache {
		args = append(args, "--clean-cache=true")
	}
	return args
}

func (mountInfo *Info) commonOptions() []string {
	var options []string
	options = append(options, fmt.Sprintf("--%s=%s", "fs-id", mountInfo.FS.ID))
	options = append(options, fmt.Sprintf("--%s=%s", "fs-info", mountInfo.FSBase64Str))

	if mountInfo.ReadOnly {
		options = append(options, fmt.Sprintf("--%s=%s", "mount-options", ReadOnly))
	}

	if mountInfo.CacheConfig.BlockSize > 0 {
		options = append(options, fmt.Sprintf("--%s=%d", "block-size", mountInfo.CacheConfig.BlockSize))
	}
	if mountInfo.CacheConfig.MetaDriver != "" {
		options = append(options, fmt.Sprintf("--%s=%s", "meta-cache-driver", mountInfo.CacheConfig.MetaDriver))
	}
	if mountInfo.CacheConfig.ExtraConfigMap != nil {
		for configName, item := range mountInfo.CacheConfig.ExtraConfigMap {
			options = append(options, fmt.Sprintf("--%s=%s", configName, item))
		}
	}
	if mountInfo.CacheConfig.Debug {
		options = append(options, "--log-level=debug")
	}

	// s3 default mount permission
	if mountInfo.FS.Type == common.S3Type {
		if mountInfo.FS.PropertiesMap[common.FileMode] != "" {
			options = append(options, fmt.Sprintf("--%s=%s", "file-mode", mountInfo.FS.PropertiesMap[common.FileMode]))
		} else {
			options = append(options, fmt.Sprintf("--%s=%s", "file-mode", "0666"))
		}
		if mountInfo.FS.PropertiesMap[common.DirMode] != "" {
			options = append(options, fmt.Sprintf("--%s=%s", "dir-mode", mountInfo.FS.PropertiesMap[common.DirMode]))
		} else {
			options = append(options, fmt.Sprintf("--%s=%s", "dir-mode", "0777"))
		}
	}
	return options
}

func (mountInfo *Info) CacheWorkerCmd() string {
	cmd := CacheWorkerBin + " --podCachePath="
	if mountInfo.CacheConfig.CacheDir != "" {
		cmd += FusePodCachePath
	}
	return cmd
}

func PodUnmount(volumeID string, mountInfo Info) error {
	podName := GeneratePodNameByVolumeID(volumeID)
	log.Infof("PodUnmount pod name is %s", podName)
	umountLock.Lock()
	defer umountLock.Unlock()

	k8sClient, err := utils.GetK8sClient()
	if err != nil {
		log.Errorf("PodUnmount: Get k8s client failed: %v", err)
		return err
	}
	pod, err := k8sClient.GetPod(Namespace, podName)
	if err != nil && !k8sErrors.IsNotFound(err) {
		log.Errorf("PodUnmount: Get pod %s err: %v", podName, err)
		return err
	}
	// if mount pod not exists. might be process mount
	if pod == nil {
		// log.Infof("PodUnmount: Mount pod %s not exists.", podName)
		return nil
	}

	workPodUID := utils.GetPodUIDFromTargetPath(mountInfo.TargetPath)
	if workPodUID != "" {
		return removeRef(k8sClient, pod, workPodUID)
	}
	return nil
}

func PFSMount(volumeID string, mountInfo Info) error {
	// create mount pod or add ref in server db
	if err := createOrUpdatePod(volumeID, mountInfo); err != nil {
		log.Errorf("PodMount: info: %+v err: %v", mountInfo, err)
		return err
	}
	return waitUtilPodReady(mountInfo.K8sClient, GeneratePodNameByVolumeID(volumeID))
}

func createOrUpdatePod(volumeID string, mountInfo Info) error {
	podName := GeneratePodNameByVolumeID(volumeID)
	log.Infof("pod name is %s", podName)
	for i := 0; i < 120; i++ {
		// wait for old pod deleted
		oldPod, errGetPod := mountInfo.K8sClient.GetPod(Namespace, podName)
		if errGetPod != nil {
			if k8sErrors.IsNotFound(errGetPod) {
				// mount pod not exist, create
				log.Infof("createOrAddRef: Need to create pod %s.", podName)
				if createPodErr := createMountPod(mountInfo.K8sClient, volumeID, mountInfo); createPodErr != nil {
					return createPodErr
				}
			} else {
				// unexpect error
				log.Errorf("createOrAddRef: Get pod %s err: %v", podName, errGetPod)
				return errGetPod
			}
		} else if oldPod.DeletionTimestamp != nil {
			// mount pod deleting. wait for 1 min.
			log.Infof("createOrAddRef: wait for old mount pod deleted.")
			time.Sleep(time.Millisecond * 500)
			continue
		} else {
			// mount pod exist, update annotation
			return addRef(mountInfo.K8sClient, oldPod, mountInfo.TargetPath)
		}
	}
	return status.Errorf(codes.Internal, "Mount %v failed: mount pod %s has been deleting for 1 min",
		mountInfo.FS.ID, podName)
}

func addRef(c utils.Client, pod *k8sCore.Pod, targetPath string) error {
	err := buildAnnotation(pod, targetPath)
	if err != nil {
		log.Errorf("mount_pod[%s] addRef: buildAnnotation err: %v", pod.Name, err)
		return err
	}
	if err = c.PatchPodAnnotation(pod); err != nil {
		retErr := fmt.Errorf("mount_pod addRef: patch pod[%s] annotation:%+v err:%v", pod.Name, pod.Annotations, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	return nil
}

func removeRef(c utils.Client, pod *k8sCore.Pod, workPodUID string) error {
	annotation := pod.ObjectMeta.Annotations
	if annotation == nil {
		log.Warnf("mount_pod removeRef: pod[%s] has no annotation", pod.Name)
		return nil
	}
	mountKey := schema.AnnotationKeyMountPrefix + workPodUID
	if _, ok := annotation[mountKey]; !ok {
		log.Infof("mount_pod removeRef: workPodUID [%s] in pod [%s] already not exists.", workPodUID, pod.Name)
		return nil
	}
	delete(annotation, mountKey)
	annotation[schema.AnnotationKeyMTime] = time.Now().Format(model.TimeFormat)
	pod.ObjectMeta.Annotations = annotation
	if err := c.PatchPodAnnotation(pod); err != nil {
		retErr := fmt.Errorf("mount_pod removeRef: patch pod[%s] annotation:%+v err:%v", pod.Name, annotation, err)
		log.Errorf(retErr.Error())
		return retErr
	}
	return nil
}

func createMountPod(k8sClient utils.Client, volumeID string, mountInfo Info) error {
	mountPod, err := buildMountPod(volumeID, mountInfo)
	if err != nil {
		log.Errorf("buildMountPod[%s] err: %v", mountInfo.FS.ID, err)
		return err
	}
	log.Debugf("creating mount pod: %+v\n", *mountPod)
	_, err = k8sClient.CreatePod(mountPod)
	if err != nil {
		log.Errorf("createMountPod for fsID %s err: %v", mountInfo.FS.ID, err)
		return err
	}
	return nil
}

func buildMountPod(volumeID string, mountInfo Info) (*k8sCore.Pod, error) {
	pod := GeneratePodTemplate()
	pod.Name = GeneratePodNameByVolumeID(volumeID)
	// annotate mount point & modified time
	err := buildAnnotation(pod, mountInfo.TargetPath)
	if err != nil {
		return nil, err
	}
	// build volumes & containers
	pod.Spec.Volumes = generatePodVolumes(mountInfo.CacheConfig.CacheDir)
	pod.Spec.Containers[0] = buildMountContainer(baseContainer(pod.Name, mountInfo.PodResource), mountInfo)
	pod.Spec.Containers[1] = buildCacheWorkerContainer(baseContainer(pod.Name, mountInfo.PodResource), mountInfo)

	// label for pod listing
	pod.Labels[schema.LabelKeyFsID] = mountInfo.FS.ID
	pod.Labels[schema.LabelKeyNodeName] = NodeName
	// labels for cache stats
	pod.Labels[schema.LabelKeyCacheID] = model.CacheID(ClusterID,
		NodeName, mountInfo.CacheConfig.CacheDir, mountInfo.FS.ID)
	// cache dir has "/" and is not allowed in label
	pod.Annotations[schema.AnnotationKeyCacheDir] = mountInfo.CacheConfig.CacheDir
	return pod, nil
}

func buildAnnotation(pod *k8sCore.Pod, targetPath string) error {
	workPodUID := utils.GetPodUIDFromTargetPath(targetPath)
	if workPodUID == "" {
		err := fmt.Errorf("mount_pod[%s] buildAnnotation: failed obtain workPodUID from target path: %s", pod.Name, targetPath)
		log.Errorf(err.Error())
		return err
	}
	pod.ObjectMeta.Annotations[schema.AnnotationKeyMountPrefix+workPodUID] = targetPath
	pod.ObjectMeta.Annotations[schema.AnnotationKeyMTime] = time.Now().Format(model.TimeFormat)
	return nil
}

func GeneratePodNameByVolumeID(volumeID string) string {
	return volumeID
}

func waitUtilPodReady(k8sClient utils.Client, podName string) error {
	// Wait until the mount pod is ready
	for i := 0; i < 60; i++ {
		pod, err := k8sClient.GetPod(Namespace, podName)
		if err != nil {
			return status.Errorf(codes.Internal, "waitUtilPodReady: Get pod %v failed: %v", podName, err)
		}
		if isPodReady(pod) {
			log.Infof("waitUtilPodReady: Pod %v is successful", podName)
			return nil
		}
		time.Sleep(time.Millisecond * 500)
	}
	podLog, err := getErrContainerLog(k8sClient, podName)
	if err != nil {
		log.Errorf("waitUtilPodReady: get pod %s log error %v", podName, err)
	}
	return status.Errorf(codes.Internal, "waitUtilPodReady: mount pod %s isn't ready in 30 seconds: %v", podName, podLog)
}

func isPodReady(pod *k8sCore.Pod) bool {
	conditionsTrue := 0
	for _, cond := range pod.Status.Conditions {
		if cond.Status == k8sCore.ConditionTrue && (cond.Type == k8sCore.ContainersReady || cond.Type == k8sCore.PodReady) {
			conditionsTrue++
		}
	}
	return conditionsTrue == 2
}

func getErrContainerLog(K8sClient utils.Client, podName string) (log string, err error) {
	pod, err := K8sClient.GetPod(Namespace, podName)
	if err != nil {
		return
	}
	for _, cn := range pod.Status.InitContainerStatuses {
		if !cn.Ready {
			log, err = K8sClient.GetPodLog(pod.Namespace, pod.Name, cn.Name)
			return
		}
	}
	for _, cn := range pod.Status.ContainerStatuses {
		if !cn.Ready {
			log, err = K8sClient.GetPodLog(pod.Namespace, pod.Name, cn.Name)
			return
		}
	}
	return
}

func baseContainer(podName string, podResource k8sCore.ResourceRequirements) k8sCore.Container {
	isPrivileged := true
	return k8sCore.Container{
		// Name:  containerName, to be set at invoker side
		Image: MountImage,
		SecurityContext: &k8sCore.SecurityContext{
			Privileged: &isPrivileged,
		},
		Env: []k8sCore.EnvVar{
			{
				Name:  schema.EnvKeyMountPodName,
				Value: podName,
			},
			{
				Name:  schema.EnvKeyNamespace,
				Value: Namespace,
			},
		},
		Resources: podResource,
	}
}

func buildMountContainer(mountContainer k8sCore.Container, mountInfo Info) k8sCore.Container {
	mountContainer.Name = ContainerNamePfsMount
	mkdir := "mkdir -p " + FusePodMountPoint + ";"

	cmd := mkdir + mountInfo.Cmd + " " + strings.Join(mountInfo.Args, " ")
	mountContainer.Command = []string{"sh", "-c", cmd}
	statCmd := "stat -c %i " + FusePodMountPoint
	mountContainer.ReadinessProbe = &k8sCore.Probe{
		Handler: k8sCore.Handler{
			Exec: &k8sCore.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"if [ x$(%v) = x1 ]; then exit 0; else exit 1; fi ", statCmd)},
			}},
		InitialDelaySeconds: 1,
		PeriodSeconds:       1,
	}
	mountContainer.Lifecycle = &k8sCore.Lifecycle{
		PreStop: &k8sCore.Handler{
			Exec: &k8sCore.ExecAction{Command: []string{"sh", "-c", fmt.Sprintf(
				"umount %s && rmdir %s", FusePodMountPoint, FusePodMountPoint)}},
		},
	}

	mp := k8sCore.MountPropagationBidirectional
	volumeMounts := []k8sCore.VolumeMount{
		{
			Name:             VolumesKeyMount,
			MountPath:        schema.FusePodMntDir,
			SubPath:          mountInfo.FS.ID,
			MountPropagation: &mp,
		},
	}
	if mountInfo.CacheConfig.CacheDir != "" {
		dataCacheVM := k8sCore.VolumeMount{
			Name:             VolumesKeyDataCache,
			MountPath:        FusePodCachePath + DataCacheDir,
			MountPropagation: &mp,
		}
		metaCacheVM := k8sCore.VolumeMount{
			Name:             VolumesKeyMetaCache,
			MountPath:        FusePodCachePath + MetaCacheDir,
			MountPropagation: &mp,
		}
		volumeMounts = append(volumeMounts, dataCacheVM, metaCacheVM)
	}
	mountContainer.VolumeMounts = volumeMounts
	return mountContainer
}

func generatePodVolumes(cacheDir string) []k8sCore.Volume {
	typeDir := k8sCore.HostPathDirectoryOrCreate
	volumes := []k8sCore.Volume{
		{
			Name: VolumesKeyMount,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: HostMntDir,
					Type: &typeDir,
				},
			},
		},
	}
	if cacheDir != "" {
		// data CacheConfig
		dataCacheVolume := k8sCore.Volume{
			Name: VolumesKeyDataCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: cacheDir + DataCacheDir,
					Type: &typeDir,
				},
			},
		}
		// meta CacheConfig
		metaCacheVolume := k8sCore.Volume{
			Name: VolumesKeyMetaCache,
			VolumeSource: k8sCore.VolumeSource{
				HostPath: &k8sCore.HostPathVolumeSource{
					Path: cacheDir + MetaCacheDir,
					Type: &typeDir,
				},
			},
		}
		volumes = append(volumes, dataCacheVolume, metaCacheVolume)
	}
	return volumes
}

func buildCacheWorkerContainer(cacheContainer k8sCore.Container, mountInfo Info) k8sCore.Container {
	cacheContainer.Name = ContainerNameCacheWorker
	cacheContainer.Command = []string{"sh", "-c", mountInfo.CacheWorkerCmd()}
	if mountInfo.CacheConfig.CacheDir != "" {
		mp := k8sCore.MountPropagationBidirectional
		volumeMounts := []k8sCore.VolumeMount{
			{
				Name:             VolumesKeyDataCache,
				MountPath:        FusePodCachePath + DataCacheDir,
				MountPropagation: &mp,
			},
			{
				Name:             VolumesKeyMetaCache,
				MountPath:        FusePodCachePath + MetaCacheDir,
				MountPropagation: &mp,
			},
		}
		cacheContainer.VolumeMounts = volumeMounts
	}
	return cacheContainer
}
