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

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"paddleflow/cmd/fs/csi-plugin/flag"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/fs/csiplugin/client/k8s"
	"paddleflow/pkg/fs/csiplugin/controller"
	"paddleflow/pkg/fs/csiplugin/csiconfig"
	"paddleflow/pkg/fs/csiplugin/csidriver"
	"paddleflow/pkg/metric"
)

var logConf = logger.LogConfig{
	Dir:             "./log",
	FilePrefix:      "./pfs-csi-plugin",
	Level:           "INFO",
	MaxKeepDays:     90,
	MaxFileNum:      100,
	MaxFileSizeInMB: 200 * 1024 * 1024,
	IsCompress:      true,
}

// init obtain csi-plugin pod, to assign same parameters to mount pods in csiconfig
func init() {
	csiconfig.Namespace = os.Getenv("CSI_NAMESPACE")
	csiconfig.NodeName = os.Getenv("KUBE_NODE_NAME")
	csiconfig.PodName = os.Getenv("CSI_POD_NAME")

	if csiconfig.PodName == "" || csiconfig.Namespace == "" {
		log.Fatalf("Pod name[%s] & namespace[%s] can't be null\n", csiconfig.PodName, csiconfig.Namespace)
		os.Exit(0)
	}

	k8sClient, err := k8s.GetK8sClient()
	if err != nil {
		log.Errorf("get k8s client failed: %v", err)
		return
	}
	pod, err := k8sClient.GetPod(csiconfig.PodName, csiconfig.Namespace)
	if err != nil {
		log.Infof("Can't get pod %s: %v", csiconfig.PodName, err)
		os.Exit(0)
	}
	csiconfig.CSIPod = *pod
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "csi-storage-driver" {
			csiconfig.MountImage = pod.Spec.Containers[i].Image
			//csiconfig.ContainerResource = pod.Spec.Containers[i].Resources
			return
		}
	}
	log.Infof("Can't get container csi-storage-driver in pod %s", csiconfig.PodName)
	os.Exit(0)
}

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "print only the version",
	}

	compoundFlags := [][]cli.Flag{
		logger.LogFlags(&logConf),
		flag.CsiPluginFlags(),
		metric.MetricsFlags(),
	}

	app := &cli.App{
		Name:                 "paddleflow-csi-plugin",
		Usage:                "csi-plugin for paddleflow",
		Version:              "1.4",
		Copyright:            "Apache License 2.0",
		HideHelpCommand:      true,
		EnableBashCompletion: true,
		Flags:                flag.ExpandFlags(compoundFlags),
		Action:               act,
	}
	if err := app.Run(os.Args); err != nil {
		exit(true)
	}
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

func act(c *cli.Context) error {
	log.Tracef("csi-plugin main act")
	if err := logger.InitStandardFileLogger(&logConf); err != nil {
		log.Errorf("csi-plugin logger.InitStandardFileLogger err: %v", err)
		return err
	}

	if c.Bool("pprof-enable") {
		go func() {
			router := gin.Default()
			pprof.Register(router, "debug/pprof")
			if err := router.Run(fmt.Sprintf(":%d", c.Int("pprof-port"))); err != nil {
				log.Errorf("run pprof failed: %s, skip this error", err.Error())
			} else {
				log.Infof("pprof started")
			}
		}()
	}

	stopChan := make(chan struct{})
	defer close(stopChan)
	ctrl := controller.GetMountPointController(c.String("node-id"))
	go ctrl.Start(stopChan)
	defer ctrl.Stop()

	d := csidriver.NewDriver(c.String("node-id"), c.String("unix-endpoint"))
	d.Run()

	return nil
}
