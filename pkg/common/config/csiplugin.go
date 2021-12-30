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
)

// CSIPlugin config
type CSIPlugin struct {
	UnixEndpoint string `yaml:"unixEndpoint"`
	NodeID       string `yaml:"nodeID"`
	PprofEnable  bool   `yaml:"pprofEnable"`
	PprofPort    int    `yaml:"pprofPort"`
}

type CSIPluginConfig struct {
	Log       logger.LogConfig `yaml:"log"`
	CSIPlugin CSIPlugin        `yaml:"csiPlugin"`
}

var defaultCSIPluginConfig = CSIPluginConfig{
	Log: logger.LogConfig{
		Dir:             "./log",
		FilePrefix:      "./pfs-csi-plugin",
		Level:           "INFO",
		MaxKeepDays:     90,
		MaxFileNum:      100,
		MaxFileSizeInMB: 200 * 1024 * 1024,
		IsCompress:      true,
	},
	CSIPlugin: CSIPlugin{
		UnixEndpoint: "unix://tmp/csi.sock",
		NodeID:       "nodeId",
		PprofEnable:  false,
		PprofPort:    6060,
	},
}

var (
	CSIPluginConf *CSIPluginConfig
)

func InitCSIPluginConfig() {
	CSIPluginConf = &defaultCSIPluginConfig
	// CSI Plugin暂时不需要配置文件
	if err := InitConfigFromUserYaml(CSIPluginConf, ""); err != nil {
		panic(err)
	}
}
