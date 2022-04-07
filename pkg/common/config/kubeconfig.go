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

/*
This file is deprecated.
Multi-cluster case (with multiple kubeconfigs) is supported by DB
*/

package config

import (
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubeConfig struct {
	ConfigPath    string `yaml:"configPath"`
	ClientQPS     int    `yaml:"clientQps"`
	ClientBurst   int    `yaml:"clientBurst"`
	ClientTimeout int    `yaml:"clientTimeout"`
}

func AddKubeConfigFlagSet(fs *pflag.FlagSet, kubeConf *KubeConfig) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	fs.StringVar(&kubeConf.ConfigPath, "k8s-config-path", kubeConf.ConfigPath, "ConfigPath")
	fs.IntVar(&kubeConf.ClientQPS, "k8s-client-qps", kubeConf.ClientQPS, "ClientQPS")
	fs.IntVar(&kubeConf.ClientBurst, "k8s-client-burst", kubeConf.ClientBurst, "ClientBurst")
	fs.IntVar(&kubeConf.ClientTimeout, "k8s-client-timeout", kubeConf.ClientTimeout, "ClientTimeout")
}

func InitKubeConfig(kubeConf KubeConfig) *rest.Config {
	conf, err := clientcmd.BuildConfigFromFlags("", kubeConf.ConfigPath)
	if err != nil {
		log.Errorf("Failed to build kube config from kubeConf.ConfigPath[%s], err:[%v]", kubeConf.ConfigPath, err)
		panic(err)
	}
	conf.QPS = float32(kubeConf.ClientQPS)
	conf.Burst = kubeConf.ClientBurst
	conf.Timeout = time.Duration(kubeConf.ClientTimeout)
	return conf
}

func InitKubeConfigFromBytes(configBytes []byte) (*rest.Config, error) {
	conf, err := clientcmd.RESTConfigFromKubeConfig(configBytes)
	if err != nil {
		log.Errorf("Failed to build kube config from kubeConfBytes[%s], err:[%v]", string(configBytes[:]), err)
		return nil, err
	}
	return conf, nil
}
