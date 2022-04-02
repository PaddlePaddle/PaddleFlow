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

	"gopkg.in/yaml.v2"
)

func InitConfigFromYaml(conf interface{}, configPath string) error {
	// readConfig
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		fmt.Printf("read file yaml[%s] failed! err:[%v]\n", configPath, err)
		return err
	}
	if err = yaml.Unmarshal(yamlFile, conf); err != nil {
		fmt.Printf("decodes yaml[%s] failed! err:[%v]", configPath, err)
		return err
	}
	return nil
}

func InitConfigFromUserYaml(conf interface{}, confPath string) error {
	if confPath == "" {
		return nil
	}
	return InitConfigFromYaml(conf, confPath)
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
