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

package job

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"time"


	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/handler"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/errors"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/common/uuid"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/server/api/request"
	"paddleflow/pkg/fs/server/service"
)

type Interface interface {
	CreateJob(conf *models.Conf) (string, error)
	StopJobByID(jobID string) error
}

var JobMap = map[schema.JobType]Interface{
	schema.TypeVcJob:    &VCJob{},
	schema.TypeSparkJob: &SparkJob{},
}

// defaultJobContent indicate bytes of default job from template yaml file for special jobType
var defaultJobContent = map[schema.JobType]func(conf *models.Conf) string{
	schema.TypeVcJob:    getVCJobFromDefaultPath,
	schema.TypeSparkJob: getSparkJobYamlPath,
}

func CreateJob(conf *models.Conf) (string, error) {
	if err := ValidateJob(conf); err != nil {
		return "", err
	}
	if err := checkResource(conf); err != nil {
		return "", err
	}
	jobType := schema.JobType(conf.Env[schema.EnvJobType])
	return JobMap[jobType].CreateJob(conf)
}

func ValidateJob(conf *models.Conf) error {
	var err error
	if len(conf.Name) == 0 {
		return errors.EmptyJobNameError()
	}
	if len(conf.Image) == 0 {
		return errors.EmptyJobImageError()
	}
	var userName string
	var queueName string
	var found bool
	if userName, found = conf.Env[schema.EnvJobUserName]; !found || len(userName) == 0 {
		return errors.EmptyUserNameError()
	}
	if queueName, found = conf.Env[schema.EnvJobQueueName]; !found || len(queueName) == 0 {
		return errors.EmptyQueueNameError()
	}
	ctx := &logger.RequestContext{
		UserName: userName,
	}
	if !models.HasAccessToResource(ctx, common.ResourceTypeQueue, queueName) {
		return common.NoAccessError(userName, common.ResourceTypeQueue, queueName)
	}
	if fsID, found := conf.Env[schema.EnvJobFsID]; !found || len(fsID) == 0 {
		return errors.EmptyFSIDError()
	}

	if jobType, found := conf.Env[schema.EnvJobType]; !found {
		return errors.EmptyJobTypeError()
	} else {
		if jobType == string(schema.TypeVcJob) {
			if mode, found := conf.Env[schema.EnvJobMode]; !found {
				return errors.EmptyJobModeError()
			} else {
				switch mode {
				case schema.EnvJobModePod:
					err = validatePodMode(conf)
				case schema.EnvJobModePS:
					err = validatePSMode(conf)
				case schema.EnvJobModeCollective:
					err = validateCollectiveMode(conf)
				default:
					return errors.InvalidJobModeError(mode)
				}
			}
		} else if jobType == string(schema.TypeSparkJob) {
			err = validateSparkMode(conf)
		} else {
			return errors.InvalidJobTypeError(jobType)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func checkResource(conf *models.Conf) error {
	var namespace string
	var err error
	if namespace, err = getNamespaceByQueueName(conf.Env[schema.EnvJobQueueName]); err != nil {
		return err
	}
	conf.Env[schema.EnvJobNamespace] = namespace

	fsID, _ := conf.Env[schema.EnvJobFsID]
	if pvcName, err := createFSClaims(namespace, fsID); err != nil {
		log.Errorf("create fs claims failed, err %v", err)
		return err
	} else {
		conf.Env[schema.EnvJobPVCName] = pvcName
	}

	if priority, found := conf.Env[schema.EnvJobPriority]; !found {
		conf.Env[schema.EnvJobPriority] = schema.EnvJobNormalPriority
	} else {
		if priority != schema.EnvJobLowPriority && priority != schema.EnvJobNormalPriority && priority != schema.EnvJobHighPriority {
			return errors.InvalidJobPriorityError(priority)
		}
	}

	return nil
}

func validatePodMode(conf *models.Conf) error {
	if len(conf.Command) == 0 {
		return errors.EmptyJobCommandError()
	}
	if flavourName, found := conf.Env[schema.EnvJobFlavour]; !found {
		return errors.EmptyFlavourError()
	} else {
		if _, exists := config.GlobalServerConfig.FlavourMap[flavourName]; !exists {
			return errors.InvalidFlavourError(flavourName)
		}
	}
	return nil
}

func validatePSMode(conf *models.Conf) error {
	if len(conf.Env[schema.EnvJobPServerCommand]) == 0 || len(conf.Env[schema.EnvJobWorkerCommand]) == 0 {
		return errors.EmptyJobCommandError()
	}
	if replicas, found := conf.Env[schema.EnvJobPServerReplicas]; found {
		if _, err := strconv.Atoi(replicas); err != nil {
			log.Errorf("cannot convert %s to int", replicas)
			return err
		}
	}
	if flavourName, found := conf.Env[schema.EnvJobPServerFlavour]; !found {
		return errors.EmptyFlavourError()
	} else {
		if _, exists := config.GlobalServerConfig.FlavourMap[flavourName]; !exists {
			return errors.InvalidFlavourError(flavourName)
		}
	}
	if replicas, found := conf.Env[schema.EnvJobWorkerReplicas]; found {
		if _, err := strconv.Atoi(replicas); err != nil {
			log.Errorf("cannot convert %s to int", replicas)
			return err
		}
	}
	if flavourName, found := conf.Env[schema.EnvJobWorkerFlavour]; !found {
		return errors.EmptyFlavourError()
	} else {
		if _, exists := config.GlobalServerConfig.FlavourMap[flavourName]; !exists {
			return errors.InvalidFlavourError(flavourName)
		}
	}
	return nil
}

func validateCollectiveMode(conf *models.Conf) error {
	if len(conf.Command) == 0 {
		return errors.EmptyJobCommandError()
	}
	if replicas, found := conf.Env[schema.EnvJobReplicas]; found {
		if _, err := strconv.Atoi(replicas); err != nil {
			log.Errorf("cannot convert %s to int", replicas)
			return err
		}
	}
	if flavourName, found := conf.Env[schema.EnvJobFlavour]; !found {
		return errors.EmptyFlavourError()
	} else {
		if _, exists := config.GlobalServerConfig.FlavourMap[flavourName]; !exists {
			return errors.InvalidFlavourError(flavourName)
		}
	}
	return nil
}

func validateSparkMode(conf *models.Conf) error {
	if mainFile, found := conf.Env[schema.EnvJobSparkMainFile]; !found || len(mainFile) == 0 {
		return errors.EmptySparkMainFileError()
	}
	if flavourName, found := conf.Env[schema.EnvJobDriverFlavour]; !found {
		return errors.EmptyFlavourError()
	} else {
		if _, exists := config.GlobalServerConfig.FlavourMap[flavourName]; !exists {
			return errors.InvalidFlavourError(flavourName)
		}
	}
	if flavourName, found := conf.Env[schema.EnvJobExecutorFlavour]; !found {
		return errors.EmptyFlavourError()
	} else {
		if _, exists := config.GlobalServerConfig.FlavourMap[flavourName]; !exists {
			return errors.InvalidFlavourError(flavourName)
		}
	}
	if replicas, found := conf.Env[schema.EnvJobExecutorReplicas]; found {
		if _, err := strconv.Atoi(replicas); err != nil {
			log.Errorf("cannot convert %s to int", replicas)
			return err
		}
	}
	return nil
}

func GetJobByID(jobID string) (models.Job, error) {
	var job models.Job
	tx := database.DB.Table("job").Where("id = ?", jobID).First(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("get job failed, err %v", tx.Error.Error())
		return models.Job{}, tx.Error
	}
	return job, nil
}

func GetJobStatusByID(jobID string) (schema.JobStatus, error) {
	job, err := GetJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	return job.Status, nil
}

func UpdateJob(jobID string, status schema.JobStatus, info interface{}, message string) (schema.JobStatus, error) {
	job, err := GetJobByID(jobID)
	if err != nil {
		return "", errors.JobIDNotFoundError(jobID)
	}
	if status != "" && !IsImmutableJobStatus(job.Status) {
		job.Status = status
	}
	if info != nil {
		job.RuntimeInfo = info
	}
	if message != "" {
		job.Message = message
	}
	if status == schema.StatusJobRunning {
		job.ActivatedAt.Time = time.Now()
		job.ActivatedAt.Valid = true
	}
	tx := database.DB.Table("job").Where("id = ?", jobID).Save(&job)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("update job failed, err %v", err)
		return "", err
	}
	return job.Status, nil
}

func StopJobByID(jobID string) error {
	job, err := GetJobByID(jobID)
	if err != nil {
		return errors.JobIDNotFoundError(jobID)
	}
	jobType := schema.JobType(job.Type)
	logger.LoggerForJob(jobID).Infof("stop %s job", jobType)
	return JobMap[jobType].StopJobByID(jobID)
}

func DeleteJobByID(jobID string) error {
	tx := database.DB.Table("job").Delete(&models.Job{}, "id = ?", jobID)
	if tx.Error != nil {
		logger.LoggerForJob(jobID).Errorf("delete job failed, err %v", tx.Error)
		return tx.Error
	}
	return nil
}

func IsImmutableJobStatus(status schema.JobStatus) bool {
	switch status {
	case schema.StatusJobSucceeded, schema.StatusJobFailed, schema.StatusJobTerminated:
		return true
	default:
		return false
	}
}

func generateJobID(param string) string {
	return uuid.GenerateID(fmt.Sprintf("%s-%s", schema.JobPrefix, param))
}

func getNamespaceByQueueName(queueName string) (string, error) {
	queue, err := models.GetQueueByName(&logger.RequestContext{}, queueName)
	if err != nil {
		log.Errorf("get queue %s failed, err %v", queueName, err)
		return "", fmt.Errorf("queueName[%s] is not exist\n", queueName)
	}
	return queue.Namespace, nil
}

func persistAndExecuteJob(job *models.Job, executeFunc func() error) error {
	return database.DB.Table("job").Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(job).Error; err != nil {
			return err
		}
		if err := executeFunc(); err != nil {
			return err
		}
		return nil
	})
}

func createFSClaims(namespace string, fsID string) (string, error) {
	// mock类型fs，pvc直接从记录中返回
	fileSystem, err := models.GetFsWithID(fsID)
	if err != nil {
		logger.Logger().Errorf("get file system for fsID[%s] failed, err %v", fsID, err)
		return "", err
	}
	if fileSystem.Type == base.MockType {
		return fileSystem.PropertiesMap[base.PVC], nil
	}

	ctx := &logger.RequestContext{}
	req := &request.CreateFileSystemClaimsRequest{
		Namespaces: []string{namespace},
		FsIDs:      []string{fsID},
	}

	fsService := service.GetFileSystemService()
	err = fsService.CreateFileSystemClaims(ctx, req)
	if err != nil {
		return "", err
	}
	claimID := fmt.Sprintf("pfs-%s-pvc", fsID)
	return claimID, nil
}

func fixContainerCommand(command string) string {
	command = strings.TrimPrefix(command, "bash -c")
	command = fmt.Sprintf("%s %s;%s", "cd", schema.DefaultFSMountPath, command)
	return command
}

func generateResourceRequirements(flavour schema.Flavour) corev1.ResourceRequirements {
	resources := corev1.ResourceRequirements{
		Requests: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse(flavour.Cpu),
			corev1.ResourceMemory: resource.MustParse(flavour.Mem),
		},
		Limits: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    resource.MustParse(flavour.Cpu),
			corev1.ResourceMemory: resource.MustParse(flavour.Mem),
		},
	}

	for key, value := range flavour.ScalarResources {
		resources.Requests[corev1.ResourceName(key)] = resource.MustParse(value)
		resources.Limits[corev1.ResourceName(key)] = resource.MustParse(value)
	}

	return resources
}

func generateVolume(fsID string, pvcName string) corev1.Volume {
	volume := corev1.Volume{
		Name: fsID,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	}
	return volume
}

func generateVolumeMount(fsID string) corev1.VolumeMount {
	volumeMount := corev1.VolumeMount{
		Name:      fsID,
		ReadOnly:  false,
		MountPath: schema.DefaultFSMountPath,
	}
	return volumeMount
}

func generateEnvVars(conf *models.Conf) []corev1.EnvVar {
	envs := make([]corev1.EnvVar, 0)
	for key, value := range conf.Env {
		env := corev1.EnvVar{
			Name:  key,
			Value: value,
		}
		envs = append(envs, env)
	}
	return envs
}

func getPriorityClass(priority string) string {
	switch priority {
	case schema.EnvJobVeryLowPriority:
		return schema.PriorityClassVeryLow
	case schema.EnvJobLowPriority:
		return schema.PriorityClassLow
	case schema.EnvJobNormalPriority:
		return schema.PriorityClassNormal
	case schema.EnvJobHighPriority:
		return schema.PriorityClassHigh
	case schema.EnvJobVeryHighPriority:
		return schema.PriorityClassVeryHigh
	}
	return schema.PriorityClassNormal
}

// getJobYamlContent get job from yaml path
func getJobYamlContent(conf *models.Conf) ([]byte, error) {
	yamlFilePath, found := conf.Env[schema.EnvJobYamlPath]
	if !found {
		// get job from template
		jobType := schema.JobType(conf.Env[schema.EnvJobType])
		if _, exist := defaultJobContent[jobType]; !exist {
			return nil, errors.UnSupportedOperate("read defaultJob")
		}
		defaultJobYamlPath := defaultJobContent[jobType](conf)
		return getDefaultJobYamlContent(defaultJobYamlPath)
	}

	yamlContent, err := handler.ReadFileFromFs(conf.Env[schema.EnvJobFsID], yamlFilePath, logger.Logger())
	if err != nil {
		log.Errorf("get job from path[%s] failed, err=[%v]", yamlFilePath, err)
		return nil, err
	}

	log.Debugf("reading job yaml[%s]", yamlContent)
	return yamlContent, nil
}

// getDefaultJobYamlContent get job from default yaml path
func getDefaultJobYamlContent(yamlFilePath string) ([]byte, error) {
	// check file exist
	if exist, err := config.PathExists(yamlFilePath); !exist || err != nil {
		log.Errorf("get job from path[%s] failed, file.exsit=[%v], err=[%v]", yamlFilePath, exist, err)
		return nil, errors.JobFileNotFound(yamlFilePath)
	}

	// read yaml as []byte
	yamlFile, err := ioutil.ReadFile(yamlFilePath)
	if err != nil {
		log.Errorf("read file yaml[%s] failed! err:[%v]\n", yamlFilePath, err)
		return nil, err
	}
	return yamlFile, nil
}

func parseBytesToObject(yamlContent []byte, jobEntity interface{}) error {
	log.Debugf("createJobFromYaml jobEntity[%+v] %v", jobEntity, reflect.TypeOf(jobEntity))

	// decode []byte into unstructured.Unstructured
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)
	unstructuredObj := &unstructured.Unstructured{}

	if _, _, err := dec.Decode(yamlContent, nil, unstructuredObj); err != nil {
		log.Errorf("Decode from yamlFIle[%s] failed! err:[%v]\n", yamlContent, err)
		return err
	}

	// convert unstructuredObj.Object into entity
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, jobEntity); err != nil {
		log.Errorf("convert map struct object[%+v] to acutal job type failed: %v", unstructuredObj.Object, err)
		return err
	}

	log.Debugf("get jobEntity[%+v] from yamlContent[%s]job", jobEntity, yamlContent)
	return nil
}

// createJobFromYaml parse the object of job from specified yaml file path
func createJobFromYaml(conf *models.Conf, jobEntity interface{}) error {
	// get bytes from yaml
	yamlFileContent, err := getJobYamlContent(conf)
	if err != nil {
		log.Errorf("read job by conf[%v] failed, err %v", conf, err)
		return err
	}
	// parse bytes to object
	if err := parseBytesToObject(yamlFileContent, jobEntity); err != nil {
		log.Errorf("parse job from bytes[%s] failed, err %v", yamlFileContent, err)
		return err
	}
	return nil
}

// appendMountIfAbsent append volumeMount if not exist in volumeMounts
func appendMountIfAbsent(vmSlice []corev1.VolumeMount, element corev1.VolumeMount) []corev1.VolumeMount {
	if vmSlice == nil {
		return []corev1.VolumeMount{element}
	}
	for _, cur := range vmSlice {
		if cur.Name == element.Name {
			return vmSlice
		}
	}
	vmSlice = append(vmSlice, element)
	return vmSlice
}

// appendVolumeIfAbsent append volume if not exist in volumes
func appendVolumeIfAbsent(vSlice []corev1.Volume, element corev1.Volume) []corev1.Volume {
	if vSlice == nil {
		return []corev1.Volume{element}
	}
	for _, cur := range vSlice {
		if cur.Name == element.Name {
			return vSlice
		}
	}
	vSlice = append(vSlice, element)
	return vSlice
}
