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

package config

import (
	"time"

	apiv1 "k8s.io/api/core/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/trace_logger"
)

var (
	GlobalServerConfig *ServerConfig                // the global ServerConfig
	DefaultPV          *apiv1.PersistentVolume      // the global default pv instance
	DefaultPVC         *apiv1.PersistentVolumeClaim // the global default pvc instance
	DefaultJobTemplate map[string][]byte            // the global default job template

	defaultJobTemplatePath = "./config/server/default/job/job_template.yaml"

	DefaultRunYamlPath    = "./run.yaml"
	serverDefaultConfPath = "./config/server/default/paddleserver.yaml"
	// DefaultClusterName for default cluster in single cluster
	DefaultClusterName = "default-cluster"
	// DefaultQueueName for default queue in single cluster
	DefaultQueueName = "default-queue"
	// DefaultNamespace for default namespace of default queue in single cluster
	DefaultNamespace = "default"
)

type ServerConfig struct {
	Storage   StorageConfig                  `yaml:"database"`
	Log       logger.LogConfig               `yaml:"log"`
	TraceLog  trace_logger.TraceLoggerConfig `yaml:"traceLog"`
	ApiServer ApiServerConfig                `yaml:"apiServer"`
	Job       JobConfig                      `yaml:"job"`
	Fs        FsServerConf                   `yaml:"fs"`
	ImageConf ImageConfig                    `yaml:"imageRepository"`
	Monitor   PrometheusConfig               `yaml:"monitor"`
	Metrics   MetricsConfig                  `yaml:"metrics"`
}

type StorageConfig struct {
	Driver                               string `yaml:"driver"`
	Host                                 string `yaml:"host"`
	Port                                 string `yaml:"port"`
	User                                 string `yaml:"user"`
	Password                             string `yaml:"password"`
	Database                             string `yaml:"database"`
	ConnectTimeoutInSeconds              int    `yaml:"connectTimeoutInSeconds,omitempty"`
	LockTimeoutInMilliseconds            int    `yaml:"lockTimeoutInMilliseconds,omitempty"`
	IdleTransactionTimeoutInMilliseconds int    `yaml:"idleTransactionTimeoutInMilliseconds,omitempty"`
	MaxIdleConns                         *int   `yaml:"maxIdleConns,omitempty"`
	MaxOpenConns                         *int   `yaml:"maxOpenConns,omitempty"`
	ConnMaxLifetimeInHours               *int   `yaml:"connMaxLifetimeInHours,omitempty"`
}

type ApiServerConfig struct {
	// Host Port used for FS to create pv/pvc with volumeAttributes point pfs-server pod
	Host                string `yaml:"host"`
	Port                int    `yaml:"port"`
	TokenExpirationHour int    `yaml:"tokenExpirationHour"`
}

type JobConfig struct {
	Reclaim       ReclaimConfig `yaml:"reclaim"`
	SchedulerName string        `yaml:"schedulerName"`
	// period second for job manager
	ClusterSyncPeriod int `yaml:"clusterSyncPeriod"`
	QueueExpireTime   int `yaml:"queueExpireTime"`
	QueueCacheSize    int `yaml:"queueCacheSize"`
	JobLoopPeriod     int `yaml:"jobLoopPeriod"`
	// SyncClusterQueue defines whether aware cluster resource or not, such as queue
	SyncClusterQueue bool `yaml:"syncClusterQueue"`
	// DefaultJobYamlDir is directory that stores default template yaml files for job; Deprecated
	DefaultJobYamlDir string `yaml:"defaultJobYamlDir"`
	// DefaultJobYamlPath defines file path that stores all default templates in one yaml
	DefaultJobYamlPath string `yaml:"defaultJobYamlPath"`
	IsSingleCluster    bool   `yaml:"isSingleCluster"`
}

type FsServerConf struct {
	DefaultPVPath        string        `yaml:"defaultPVPath"`
	DefaultPVCPath       string        `yaml:"defaultPVCPath"`
	LinkMetaDirPrefix    string        `yaml:"linkMetaDirPrefix"`
	MountPodExpire       time.Duration `yaml:"mountPodExpire"`
	MountPodIntervalTime time.Duration `yaml:"mountPodIntervalTime"`
}

type ReclaimConfig struct {
	CleanJob               bool `yaml:"isCleanJob"`
	SkipCleanFailedJob     bool `yaml:"isSkipCleanFailedJob"`
	FailedJobTTLSeconds    int  `yaml:"failedJobTTLSeconds,omitempty"`
	SucceededJobTTLSeconds int  `yaml:"succeededJobTTLSeconds,omitempty"`
	PendingJobTTLSeconds   int  `yaml:"pendingJobTTLSeconds,omitempty"`
}

type ImageConfig struct {
	Server           string `yaml:"server"`
	Namespace        string `yaml:"namespace"`
	Username         string `yaml:"username"`
	Password         string `yaml:"password"`
	Concurrency      int    `yaml:"concurrency"`
	RemoveLocalImage bool   `yaml:"removeLocalImage"`
}

type PrometheusConfig struct {
	Server              string `yaml:"server"`
	ExporterServicePort int    `yaml:"exporterServicePort"`
}

type MetricsConfig struct {
	Port   int  `yaml:"port"`
	Enable bool `yaml:"enable"`
}
