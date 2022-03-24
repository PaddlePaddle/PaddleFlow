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

package service

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	libfuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"paddleflow/cmd/fs/fuse/flag"
	"paddleflow/pkg/client"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/http/api"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/cache"
	"paddleflow/pkg/fs/client/fuse"
	"paddleflow/pkg/fs/client/meta"
	"paddleflow/pkg/fs/client/vfs"
	"paddleflow/pkg/fs/common"
	mountUtil "paddleflow/pkg/fs/utils/mount"
	"paddleflow/pkg/metric"
)

var opts *libfuse.MountOptions

func CmdMount() *cli.Command {
	compoundFlags := [][]cli.Flag{
		flag.MountFlags(&config.FuseConf.Fuse),
		flag.CacheFlags(&config.FuseConf.Fuse),
		flag.LinkFlags(&config.FuseConf.Fuse),
		flag.GlobalFlags(&config.FuseConf.Fuse),
		flag.LogFlags(&config.FuseConf.Log),
		flag.UserFlags(&config.FuseConf.Fuse),
		flag.MetricsFlags(&config.FuseConf.Fuse),
	}
	return &cli.Command{
		Name:      "mount",
		Action:    mount,
		Category:  "SERVICE",
		Usage:     "Mount a volume",
		ArgsUsage: "META-URL MOUNTPOINT",
		Description: `
Usage please refer to docs`,
		Flags: flag.ExpandFlags(compoundFlags),
	}
}

func setup() error {
	opts = &libfuse.MountOptions{}
	fuseConf := config.FuseConf.Fuse
	if len(fuseConf.MountPoint) == 0 || fuseConf.MountPoint == "/" {
		log.Errorf("invalid mount point: [%s]", fuseConf.MountPoint)
		return fmt.Errorf("invalid mountpoint: %s", fuseConf.MountPoint)
	}

	isMounted, err := mountUtil.IsMountPoint(fuseConf.MountPoint)
	if err != nil {
		log.Errorf("check mount point failed: %v", err)
		return fmt.Errorf("check mountpoint failed: %v", err)
	}
	if isMounted {
		return fmt.Errorf("%s is already the mountpoint", fuseConf.MountPoint)
	}

	if len(fuseConf.MountOptions) != 0 {
		mopts := strings.Split(fuseConf.MountOptions, ",")
		opts.Options = append(opts.Options, mopts...)
	}

	if config.FuseConf.Log.Level == "DEBUG" {
		opts.Debug = true
	}

	opts.IgnoreSecurityLabels = fuseConf.IgnoreSecurityLabels
	opts.DisableXAttrs = fuseConf.DisableXAttrs
	opts.AllowOther = fuseConf.AllowOther

	// Wrap the default registry, all prometheus.MustRegister() calls should be afterwards
	// InitVFS() has many registers, should be after wrapRegister()
	wrapRegister(fuseConf.MountPoint)
	if err := InitVFS(); err != nil {
		log.Errorf("init vfs failed: %v", err)
		return err
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
	return nil
}

func exposeMetrics(conf config.Fuse) string {
	// default set
	ip, _, err := net.SplitHostPort(conf.Server)
	if err != nil {
		log.Fatalf("metrics format error: %v", err)
	}
	port := conf.MetricsPort
	log.Debugf("metrics server - ip:%s, port:%d", ip, port)
	go metric.UpdateMetrics()
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
	metricsAddr := fmt.Sprintf(":%d", port)
	go func() {
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			log.Errorf("metrics ListenAndServe error: %s", err)
		}
	}()

	log.Infof("metrics listening on %s", metricsAddr)
	return metricsAddr
}

func wrapRegister(mountPoint string) {
	registry := prometheus.NewRegistry() // replace default so only pfs-fuse metrics are exposed
	prometheus.DefaultGatherer = registry
	metricLabels := prometheus.Labels{"mp": mountPoint}
	prometheus.DefaultRegisterer = prometheus.WrapRegistererWithPrefix("pfs_",
		prometheus.WrapRegistererWith(metricLabels, registry))
	prometheus.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	prometheus.MustRegister(prometheus.NewGoCollector())
}

func Mount() (*libfuse.Server, error) {
	if err := setup(); err != nil {
		log.Errorf("mount setup() err: %v", err)
		return nil, err
	}
	metricsAddr := exposeMetrics(config.FuseConf.Fuse)
	log.Debugf("mount opts: %+v, metricsAddr: %s", opts, metricsAddr)
	return fuse.Server(config.FuseConf.Fuse.MountPoint, *opts)
}

func mount(c *cli.Context) error {
	if err := setup(); err != nil {
		log.Errorf("mount setup() err: %v", err)
		return err
	}
	log.Infof("start mount()")
	server, err := fuse.Server(config.FuseConf.Fuse.MountPoint, *opts)
	if err != nil {
		log.Fatalf("mount fail: %v", err)
		os.Exit(-1)
	}
	server.Wait()
	return err
}

func InitVFS() error {
	var fsMeta common.FSMeta
	var links map[string]common.FSMeta
	fuseConf := config.FuseConf.Fuse
	if fuseConf.Local == true {
		if fuseConf.LocalRoot == "" || fuseConf.LocalRoot == "/" {
			log.Errorf("invalid localRoot: [%s]", fuseConf.LocalRoot)
			return fmt.Errorf("invalid localRoot: [%s]", fuseConf.LocalRoot)
		}
		fsMeta = common.FSMeta{
			UfsType: common.LocalType,
			SubPath: fuseConf.LocalRoot,
		}
		if fuseConf.LinkPath != "" && fuseConf.LinkRoot != "" {
			links = map[string]common.FSMeta{
				path.Clean(fuseConf.LinkPath): common.FSMeta{
					UfsType: common.LocalType,
					SubPath: fuseConf.LinkRoot,
				},
			}
		}
	} else if fuseConf.FsInfoPath != "" {
		reader, err := os.Open(fuseConf.FsInfoPath)
		if err != nil {
			return err
		}
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
		err = json.Unmarshal(data, &fsMeta)
		if err != nil {
			log.Errorf("get fsInfo fail: %s", err.Error())
			return err
		}
		fsMeta.UfsType = fsMeta.Type
		fsMeta.Type = "fs"
		log.Infof("fuse meta is %+v", fsMeta)
		links = map[string]common.FSMeta{}
	} else {
		if fuseConf.FsID == "" {
			log.Errorf("invalid fsID: [%s]", fuseConf.FsID)
			return fmt.Errorf("invalid fsID: [%s]", fuseConf.FsID)
		}

		if fuseConf.Server == "" {
			log.Errorf("invalid server: [%s]", fuseConf.Server)
			return fmt.Errorf("invalid server: [%s]", fuseConf.Server)
		}

		if fuseConf.UserName == "" {
			log.Errorf("invalid username: [%s]", fuseConf.UserName)
			return fmt.Errorf("invalid username: [%s]", fuseConf.UserName)
		}

		if fuseConf.Password == "" {
			log.Errorf("invalid password: [%s]", fuseConf.Password)
			return fmt.Errorf("invalid password: [%s]", fuseConf.Password)
		}

		httpClient := client.NewHttpClient(fuseConf.Server, client.DefaultTimeOut)

		// 获取token
		login := api.LoginParams{
			UserName: fuseConf.UserName,
			Password: fuseConf.Password,
		}
		loginResponse, err := api.LoginRequest(login, httpClient)
		if err != nil {
			log.Errorf("fuse login failed: %v", err)
			return err
		}

		fuseClient, err := base.NewClient(fuseConf.FsID, httpClient, fuseConf.UserName, loginResponse.Authorization)
		if err != nil {
			log.Errorf("init client with fs[%s] and server[%s] failed: %v", fuseConf.FsID, fuseConf.Server, err)
			return err
		}
		fsMeta, err = fuseClient.GetFSMeta()
		if err != nil {
			log.Errorf("get fs[%s] meta from pfs server[%s] failed: %v",
				fuseConf.FsID, fuseConf.Server, err)
			return err
		}
		fuseClient.FsName = fsMeta.Name
		links, err = fuseClient.GetLinks()
		if err != nil {
			log.Errorf("get fs[%s] links from pfs server[%s] failed: %v",
				fuseConf.FsID, fuseConf.Server, err)
			return err
		}
	}
	m := meta.Config{
		AttrCacheExpire:  config.FuseConf.Fuse.MetaCacheExpire,
		EntryCacheExpire: config.FuseConf.Fuse.EntryCacheExpire,
		Driver:           config.FuseConf.Fuse.MetaDriver,
		CachePath:        config.FuseConf.Fuse.MetaCachePath,
	}
	d := cache.Config{
		BlockSize:    config.FuseConf.Fuse.BlockSize,
		MaxReadAhead: config.FuseConf.Fuse.MaxReadAheadSize,
		Mem: &cache.MemConfig{
			CacheSize: config.FuseConf.Fuse.MemorySize,
			Expire:    config.FuseConf.Fuse.MemoryExpire,
		},
		Disk: &cache.DiskConfig{
			Dir:    config.FuseConf.Fuse.DiskCachePath,
			Expire: config.FuseConf.Fuse.DiskExpire,
		},
	}
	vfsOptions := []vfs.Option{
		vfs.WithDataCacheConfig(d),
		vfs.WithMetaConfig(m),
	}
	if !config.FuseConf.Fuse.RawOwner {
		vfsOptions = append(vfsOptions, vfs.WithOwner(
			uint32(config.FuseConf.Fuse.Uid),
			uint32(config.FuseConf.Fuse.Gid)))
	}
	vfsConfig := vfs.InitConfig(vfsOptions...)

	if _, err := vfs.InitVFS(fsMeta, links, true, vfsConfig); err != nil {
		log.Errorf("init vfs failed: %v", err)
		return err
	}
	return nil
}
