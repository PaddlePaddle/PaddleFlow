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
	"paddleflow/pkg/common/http/api"
	"paddleflow/pkg/common/logger"
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

var logConf = logger.LogConfig{
	Dir:             "./log",
	FilePrefix:      "./pfs-fuse",
	Level:           "INFO",
	MaxKeepDays:     90,
	MaxFileNum:      100,
	MaxFileSizeInMB: 200 * 1024 * 1024,
	IsCompress:      true,
}

func CmdMount() *cli.Command {
	compoundFlags := [][]cli.Flag{
		flag.MountFlags(),
		flag.LinkFlags(),
		flag.BasicFlags(),
		flag.CacheFlags(fuse.FuseConf),
		flag.UserFlags(fuse.FuseConf),
		logger.LogFlags(&logConf),
		metric.MetricsFlags(),
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

func setup(c *cli.Context) error {
	if err := logger.InitStandardFileLogger(&logConf); err != nil {
		log.Errorf("cmd mount setup() logger.Init err:%v", err)
		return err
	}
	opts = &libfuse.MountOptions{}
	mountPoint := c.String("mount-point")
	if len(mountPoint) == 0 || mountPoint == "/" {
		log.Errorf("invalid mount point: [%s]", mountPoint)
		return fmt.Errorf("invalid mountpoint: %s", mountPoint)
	}

	isMounted, err := mountUtil.IsMountPoint(mountPoint)
	if err != nil {
		log.Errorf("check mount point failed: %v", err)
		return fmt.Errorf("check mountpoint failed: %v", err)
	}
	if isMounted {
		return fmt.Errorf("%s is already the mountpoint", mountPoint)
	}

	mountOps := c.String("mount-options")
	if len(mountOps) != 0 {
		mopts := strings.Split(mountOps, ",")
		opts.Options = append(opts.Options, mopts...)
	}

	if logConf.Level == "DEBUG" || logConf.Level == "TRACE" {
		opts.Debug = true
	}

	opts.IgnoreSecurityLabels = c.Bool("ignore-security-labels")
	opts.DisableXAttrs = c.Bool("disable-xattrs")
	opts.AllowOther = c.Bool("allow-other")

	// Wrap the default registry, all prometheus.MustRegister() calls should be afterwards
	// InitVFS() has many registers, should be after wrapRegister()
	registry := wrapRegister(mountPoint)
	if err := InitVFS(c, registry); err != nil {
		log.Errorf("init vfs failed: %v", err)
		return err
	}

	if !c.Bool("local") {
		stopChan := make(chan struct{})
		defer close(stopChan)
		if !c.Bool("skip-check-links") {
			f := func() {
				if err := vfs.GetVFS().Meta.LinksMetaUpdateHandler(stopChan,
					c.Int("link-update-interval"), c.String("link-meta-dir-prefix")); err != nil {
					log.Errorf("mount setup() vfs.GetVFS().Meta.LinksMetaUpdateHandler err: %v", err)
				}
			}
			go f()
		}
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

	if c.Bool("metrics-service-on") {
		metricsAddr := exposeMetrics(c.String("server"), c.Int("metrics-service-port"))
		log.Debugf("mount opts: %+v, metricsAddr: %s", opts, metricsAddr)
	}

	return nil
}

func exposeMetrics(hostServer string, port int) string {
	// default set
	ip, _, err := net.SplitHostPort(hostServer)
	if err != nil {
		log.Fatalf("metrics format error: %v", err)
	}
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

func wrapRegister(mountPoint string) *prometheus.Registry {
	registry := prometheus.NewRegistry() // replace default so only pfs-fuse metrics are exposed
	prometheus.DefaultGatherer = registry
	metricLabels := prometheus.Labels{"mp": mountPoint}
	prometheus.DefaultRegisterer = prometheus.WrapRegistererWithPrefix("pfs_",
		prometheus.WrapRegistererWith(metricLabels, registry))
	prometheus.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	prometheus.MustRegister(prometheus.NewGoCollector())
	return registry
}

func mount(c *cli.Context) error {
	log.Tracef("mount setup VFS")
	if err := setup(c); err != nil {
		log.Errorf("mount setup() err: %v", err)
		return err
	}

	log.Debugf("start mount service")
	server, err := fuse.Server(c.String("mount-point"), *opts)
	if err != nil {
		log.Fatalf("mount fail: %v", err)
		os.Exit(-1)
	}
	server.Wait()
	return err
}

func InitVFS(c *cli.Context, registry *prometheus.Registry) error {
	var fsMeta common.FSMeta
	var links map[string]common.FSMeta
	server := c.String("server")
	if c.Bool("local") == true {
		localRoot := c.String("local-root")
		if localRoot == "" || localRoot == "/" {
			log.Errorf("invalid localRoot: [%s]", localRoot)
			return fmt.Errorf("invalid localRoot: [%s]", localRoot)
		}
		fsMeta = common.FSMeta{
			UfsType: common.LocalType,
			SubPath: localRoot,
		}
		linkPath, linkRoot := c.String("link-path"), c.String("link-root")
		if linkPath != "" && linkRoot != "" {
			links = map[string]common.FSMeta{
				path.Clean(linkPath): common.FSMeta{
					UfsType: common.LocalType,
					SubPath: linkRoot,
				},
			}
		}
	} else if c.String("fs-info-path") != "" {
		reader, err := os.Open(c.String("fs-info-path"))
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
		fsID := c.String("fs-id")
		if fsID == "" {
			log.Errorf("invalid fsID: [%s]", fsID)
			return fmt.Errorf("invalid fsID: [%s]", fsID)
		}

		if server == "" {
			log.Errorf("invalid server: [%s]", server)
			return fmt.Errorf("invalid server: [%s]", server)
		}
		username := c.String("user-name")
		if username == "" {
			log.Errorf("invalid username: [%s]", username)
			return fmt.Errorf("invalid username: [%s]", username)
		}
		password := c.String("password")
		if password == "" {
			log.Errorf("invalid password: [%s]", password)
			return fmt.Errorf("invalid password: [%s]", password)
		}

		httpClient := client.NewHttpClient(server, client.DefaultTimeOut)

		// 获取token
		login := api.LoginParams{
			UserName: username,
			Password: password,
		}
		loginResponse, err := api.LoginRequest(login, httpClient)
		if err != nil {
			log.Errorf("fuse login failed: %v", err)
			return err
		}

		fuseClient, err := base.NewClient(fsID, httpClient, username, loginResponse.Authorization)
		if err != nil {
			log.Errorf("init client with fs[%s] and server[%s] failed: %v", fsID, server, err)
			return err
		}
		fsMeta, err = fuseClient.GetFSMeta()
		if err != nil {
			log.Errorf("get fs[%s] meta from pfs server[%s] failed: %v",
				fsID, server, err)
			return err
		}
		fuseClient.FsName = fsMeta.Name
		links, err = fuseClient.GetLinks()
		if err != nil {
			log.Errorf("get fs[%s] links from pfs server[%s] failed: %v",
				fsID, server, err)
			return err
		}
	}
	m := meta.Config{
		AttrCacheExpire:  c.Duration("meta-cache-expire"),
		EntryCacheExpire: c.Duration("entry-cache-expire"),
		Driver:           c.String("meta-driver"),
		CachePath:        c.String("meta-path"),
	}
	d := cache.Config{
		BlockSize:    c.Int("block-size"),
		MaxReadAhead: c.Int("data-read-ahead-size"),
		Mem: &cache.MemConfig{
			CacheSize: c.Int("data-mem-size"),
			Expire:    c.Duration("data-mem-cache-expire"),
		},
		Disk: &cache.DiskConfig{
			Dir:    c.String("data-disk-cache-path"),
			Expire: c.Duration("data-disk-cache-expire"),
		},
	}
	vfsOptions := []vfs.Option{
		vfs.WithDataCacheConfig(d),
		vfs.WithMetaConfig(m),
	}
	if !fuse.FuseConf.RawOwner {
		vfsOptions = append(vfsOptions, vfs.WithOwner(
			uint32(fuse.FuseConf.Uid),
			uint32(fuse.FuseConf.Gid)))
	}
	vfsConfig := vfs.InitConfig(vfsOptions...)

	if _, err := vfs.InitVFS(fsMeta, links, true, vfsConfig, registry); err != nil {
		log.Errorf("init vfs failed: %v", err)
		return err
	}
	return nil
}
