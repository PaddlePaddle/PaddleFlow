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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	yaml2 "gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/util/yaml"
)

// InitDefaultPV initialize the default pv instance
func InitDefaultPV(path string) error {
	reader, err := os.Open(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	return yaml.NewYAMLOrJSONDecoder(reader, 1024).Decode(&DefaultPV)
}

// InitDefaultPVC initialize the default pvc instance
func InitDefaultPVC(path string) error {
	reader, err := os.Open(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	return yaml.NewYAMLOrJSONDecoder(reader, 1024).Decode(&DefaultPVC)
}

func InitConfigFromYaml(conf interface{}, configPath string) error {
	// if not set by user, use default
	if configPath == "" {
		log.Infoln("config yaml path not specified. use default config")
		configPath = serverDefaultConfPath
	}
	// readConfig
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		fmt.Printf("read file yaml[%s] failed! err:[%v]\n", configPath, err)
		return err
	}
	if err = yaml2.Unmarshal(yamlFile, conf); err != nil {
		fmt.Printf("decodes yaml[%s] failed! err:[%v]", configPath, err)
		return err
	}
	return nil
}

func PrettyFormat(data interface{}) []byte {
	p, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		panic(err)
	}
	return p
}

// PathExists indicate path exist or not
// 1. path exist: return true, nil
// 2. path not exist: return false, nil
// 3. unknown error: return false, err
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// FileNumsInDir caculate files number in path, include dir type
func FileNumsInDir(path string) (int, error) {
	if exist, err := PathExists(path); !exist {
		return 0, err
	}
	files, _ := ioutil.ReadDir(path)
	return len(files), nil
}

func GetServiceAddress() string {
	return fmt.Sprintf("%s:%d", GlobalServerConfig.ApiServer.Host, GlobalServerConfig.ApiServer.Port)
}
