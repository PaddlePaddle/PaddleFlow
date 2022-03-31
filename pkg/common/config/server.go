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

package config

import (
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/common/schema"
)

var serverDefaultConfPath = "./config/server/default/paddleserver.yaml"

type ServerConfig struct {
	Database      DatabaseConfig            `yaml:"database"`
	Log           logger.LogConfig          `yaml:"log"`
	ApiServer     ApiServerConfig           `yaml:"apiServer"`
	KubeConfig    KubeConfig                `yaml:"kubeConfig"`
	Job           JobConfig                 `yaml:"job"`
	Fs            FsServerConf              `yaml:"fs"`
	NamespaceList []string                  `yaml:"namespaceList"`
	Flavour       []schema.Flavour          `yaml:"flavour"`
	FlavourMap    map[string]schema.Flavour `yaml:"-"`
	ImageConf     ImageConfig               `yaml:"imageRepository"`
}

type ApiServerConfig struct {
	Host                string `yaml:"host"`
	Port                int    `yaml:"port"`
	PrintVersionAndExit bool   `yaml:"printVersionAndExit"`
	TokenExpirationHour int    `yaml:"tokenExpirationHour"`
}

type JobConfig struct {
	Reclaim             ReclaimConfig `yaml:"reclaim"`
	SchedulerName       string        `yaml:"schedulerName"`
	ScalarResourceArray []string      `yaml:"scalarResourceArray"`
	// period second for job manager
	ClusterSyncPeriod int `yaml:"clusterSyncPeriod"`
	QueueExpireTime   int `yaml:"queueExpireTime"`
	QueueCacheSize    int `yaml:"queueCacheSize"`
	JobLoopPeriod     int `yaml:"jobLoopPeriod"`
	// DefaultJobYamlDir is directory that stores default template yaml files for job
	DefaultJobYamlDir string `yaml:"defaultJobYamlDir"`
}

type FsServerConf struct {
	DefaultPVPath     string `yaml:"defaultPVPath"`
	DefaultPVCPath    string `yaml:"defaultPVCPath"`
	LinkMetaDirPrefix string `yaml:"linkMetaDirPrefix"`
	// K8sServiceName K8sServicePort used to create pv/pvc with volumeAttributes point pfs-server pod
	K8sServiceName string `yaml:"k8sServiceName"`
	K8sServicePort int    `yaml:"k8sServicePort"`
}

type ReclaimConfig struct {
	CleanJob             bool `yaml:"isCleanJob"`
	SkipCleanFailedJob   bool `yaml:"isSkipCleanFailedJob"`
	JobTTLSeconds        int  `yaml:"jobTTLSeconds"`
	JobPendingTTLSeconds int  `yaml:"JobPendingTTLSeconds,omitempty"`
}

type ImageConfig struct {
	Server           string `yaml:"server"`
	Namespace        string `yaml:"namespace"`
	Username         string `yaml:"username"`
	Password         string `yaml:"password"`
	Concurrency      int    `yaml:"concurrency"`
	RemoveLocalImage bool   `yaml:"removeLocalImage"`
}

var (
	GlobalServerConfig *ServerConfig
)

func InitConfigFromDefaultYaml(conf interface{}) error {
	return InitConfigFromYaml(conf, serverDefaultConfPath)
}
