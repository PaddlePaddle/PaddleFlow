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
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	expand "github.com/PaddlePaddle/PaddleFlow/cmd/fs/csi-plugin/flag"
	"github.com/PaddlePaddle/PaddleFlow/cmd/fs/location-awareness/cache-worker/flag"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	location_awareness "github.com/PaddlePaddle/PaddleFlow/pkg/fs/location-awareness"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/version"
)

var logConf = logger.LogConfig{
	Dir:             "./log",
	FilePrefix:      "./cache-worker",
	Level:           "INFO",
	MaxKeepDays:     90,
	MaxFileNum:      100,
	MaxFileSizeInMB: 200 * 1024 * 1024,
	IsCompress:      true,
}

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name: "version", Aliases: []string{"V"},
		Usage: "print only the version",
	}

	compoundFlags := [][]cli.Flag{
		logger.LogFlags(&logConf),
		flag.CacheWorkerFlags(),
	}

	app := &cli.App{
		Name:                 "paddleflow-fs-cache-worker",
		Usage:                "cache-worker for file system",
		Version:              version.InfoStr(),
		Copyright:            "Apache License 2.0",
		HideHelpCommand:      true,
		EnableBashCompletion: true,
		Flags:                expand.ExpandFlags(compoundFlags),
		Action:               act,
	}
	if err := app.Run(os.Args); err != nil {
		exit(true)
	}
	exit(false)
}

// exit the function that stop program with return value
func exit(hasError bool) {
	time.Sleep(100 * time.Millisecond)
	if hasError {
		os.Exit(-1)
	}
	os.Exit(0)
}

func act(c *cli.Context) error {
	log.Infof("cache-worker main act")
	if err := logger.InitStandardFileLogger(&logConf); err != nil {
		log.Errorf("cache-worker logger.InitStandardFileLogger err: %v", err)
		return err
	}

	podNamespace := os.Getenv(schema.EnvKeyNamespace)
	podName := os.Getenv(schema.EnvKeyMountPodName)

	if podName == "" || podNamespace == "" {
		log.Fatalf("mount pod name[%s] or podNamespace[%s] can't be null\n", podName, podNamespace)
		os.Exit(0)
	}

	k8sClient, err := utils.GetK8sClient()
	if err != nil {
		log.Errorf("get k8s client failed: %v", err)
		os.Exit(0)
	}

	podCachePath := c.String("podCachePath")

	go func() {
		location_awareness.PatchCacheStatsLoop(k8sClient, podNamespace, podName, podCachePath)
	}()

	stopSig := make(chan os.Signal, 1)
	signal.Notify(stopSig, syscall.SIGTERM, syscall.SIGINT)
	sig := <-stopSig
	log.Errorf("PatchCacheStatsLoop stopped err: %s", sig.String())

	return errors.New(sig.String())
}
