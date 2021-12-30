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

package app

import (
	"github.com/spf13/pflag"

	"paddleflow/cmd/fs/csi-plugin/app/options"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
)

func initConfig() {
	// init from yaml config
	config.InitCSIPluginConfig()
	f := options.NewCSIPluginOption()
	f.InitFlag(pflag.CommandLine)
}

func Init() error {
	initConfig()
	return logger.Init(&config.CSIPluginConf.Log)
}
