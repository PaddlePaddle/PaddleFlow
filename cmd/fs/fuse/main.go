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

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"paddleflow/cmd/fs/fuse/app"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/fs/client/vfs"
)

func main() {

	if err := app.Init(); err != nil {
		log.Errorf("init fuse failed: %v", err)
		os.Exit(-1)
	}

	if !config.FuseConf.Fuse.Local {
		stopChan := make(chan struct{})
		defer close(stopChan)
		fuseConf := config.FuseConf.Fuse
		if !config.FuseConf.Fuse.SkipCheckLinks {
			go vfs.GetVFS().Meta.LinksMetaUpdateHandler(stopChan, fuseConf.LinkUpdateInterval, fuseConf.LinkMetaDirPrefix)
		}
	}

	if config.FuseConf.Fuse.PprofEnable {
		go func() {
			router := gin.Default()
			pprof.Register(router, "debug/pprof")
			if err := router.Run(fmt.Sprintf(":%d", config.FuseConf.Fuse.PprofPort)); err != nil {
				log.Errorf("run pprof failed: %s, skip this error", err.Error())
			} else {
				log.Infof("pprof started")
			}
		}()
	}

	log.Infof("start to init pfs fuse")
	server, err := app.Mount()
	if err != nil {
		log.Fatalf("Mount fail: %v", err)
		os.Exit(-1)
	}
	server.Wait()
}
