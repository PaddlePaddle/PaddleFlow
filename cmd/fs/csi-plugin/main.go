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

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"paddleflow/cmd/fs/csi-plugin/app"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/fs/csiplugin/controller"
	"paddleflow/pkg/fs/csiplugin/csidriver"
)

func exitWithError() {
	exit(true)
}

func exitNormal() {
	exit(false)
}

// exit the function that stop program with return value
func exit(hasError bool) {
	// it is required, to work around bug of log4go
	// 在程序退出前，需要先sleep一段时间，否则有可能日志打印不全
	time.Sleep(100 * time.Millisecond)
	if hasError {
		os.Exit(-1)
	}
	os.Exit(0)
}

// main the function where execution of the program begins
func main() {
	if err := app.Init(); err != nil {
		log.Errorf("init csi plugin failed: %v", err)
		exitWithError()
	}

	if config.CSIPluginConf.CSIPlugin.PprofEnable {
		go func() {
			router := gin.Default()
			pprof.Register(router, "debug/pprof")
			if err := router.Run(fmt.Sprintf(":%d", config.CSIPluginConf.CSIPlugin.PprofPort)); err != nil {
				log.Errorf("run pprof failed: %s, skip this error", err.Error())
			} else {
				log.Infof("pprof started")
			}
		}()
	}

	stopChan := make(chan struct{})
	defer close(stopChan)
	controller := controller.GetMountPointController(config.CSIPluginConf.CSIPlugin.NodeID)
	go controller.Start(stopChan)
	defer controller.Stop()

	d := csidriver.NewDriver(config.CSIPluginConf.CSIPlugin.NodeID, config.CSIPluginConf.CSIPlugin.UnixEndpoint)
	d.Run()

	exitNormal()
}
