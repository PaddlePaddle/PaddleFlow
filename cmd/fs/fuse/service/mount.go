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
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	libfuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"

	"github.com/PaddlePaddle/PaddleFlow/cmd/fs/fuse/flag"
	"github.com/PaddlePaddle/PaddleFlow/pkg/client"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/api"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/base"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fuse"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/meta"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/vfs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/monitor"
)

const TimeFormat = "2006-01-02-15:04:05"

var opts *libfuse.MountOptions

var logConf = logger.LogConfig{
	Dir:             "./log",
	FilePrefix:      "./pfs-fuse-" + time.Now().Format(TimeFormat),
	Level:           "INFO",
	MaxKeepDays:     90,
	MaxFileNum:      100,
	MaxFileSizeInMB: 200 * 1024 * 1024,
	IsCompress:      true,
}

func CmdMount() *cli.Command {
	compoundFlags := [][]cli.Flag{
		flag.MountFlags(fuse.FuseConf),
		flag.LinkFlags(),
		flag.BasicFlags(),
		flag.CacheFlags(fuse.FuseConf),
		flag.UserFlags(fuse.FuseConf),
		logger.LogFlags(&logConf),
		monitor.MetricsFlags(),
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

	isMounted, err := utils.IsMountPoint(mountPoint)
	if isMounted {
		if err != nil {
			if errUmount := doUmount(mountPoint, true); errUmount != nil {
				log.Errorf("unmount mountpoint %s failed: %v", mountPoint, errUmount)
				return fmt.Errorf("unmount mountpoint %s failed: %v", mountPoint, errUmount)
			}
		} else {
			log.Errorf("mountpoint %s is already mounted", mountPoint)
			return fmt.Errorf("mountpoint %s is already mounted", mountPoint)
		}
	} else if err != nil {
		log.Errorf("check mount point failed: %v", err)
		return fmt.Errorf("check mountpoint failed: %v", err)
	}

	mountOps := c.String("mount-options")
	if len(mountOps) != 0 {
		mopts := strings.Split(mountOps, ",")
		opts.Options = append(opts.Options, mopts...)
	}

	if strings.ToTitle(logConf.Level) == "DEBUG" || strings.ToTitle(logConf.Level) == "TRACE" {
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
	go monitor.UpdateBaseMetrics()
	// whether start metrics server
	if c.Bool("metrics-service-on") {
		metricsAddr := exposeMetricsService(c.String("server"), c.Int("metrics-service-port"))
		log.Debugf("mount opts: %+v, metricsAddr: %s", opts, metricsAddr)
	}
	if c.Int("pprof-port") != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", c.Int("pprof-port")), nil)
		}()
	}

	if c.Bool("clean-cache") {
		if c.String("meta-cache-path") != "" {
			cleanCacheInfo.CachePaths = append(cleanCacheInfo.CachePaths, c.String("meta-cache-path"))
		}
		if c.String("data-cache-path") != "" {
			cleanCacheInfo.CachePaths = append(cleanCacheInfo.CachePaths, c.String("data-cache-path"))
		}
		if len(cleanCacheInfo.CachePaths) > 0 {
			cleanCacheInfo.Clean = true
		}
	}
	signalHandle(mountPoint)
	processStatistics()
	return nil
}

func processStatistics() {
	go func() {
		for {
			processMemPercent := utils.GetProcessMemPercent()
			processCpuPercent := utils.GetProcessCPUPercent()
			_, sysMemPercent := utils.GetSysMemPercent()
			sysCpuPercent := utils.GetSysCpuPercent()
			gNum := runtime.NumGoroutine()
			log.Infof("processMemPercent %v%% processCpuPercent %v%% sysMemPercent %v%% sysCpuPercent %v%% "+
				"goroutine num %v", fmt.Sprintf("%.2f", processMemPercent),
				fmt.Sprintf("%.2f", processCpuPercent), fmt.Sprintf("%.2f", sysMemPercent),
				fmt.Sprintf("%.2f", sysCpuPercent), gNum)
			time.Sleep(30 * time.Second)
		}
	}()
}

func exposeMetricsService(hostServer string, port int) string {
	// default set
	ip, _, err := net.SplitHostPort(hostServer)
	if err != nil {
		log.Fatalf("metrics format error: %v", err)
	}
	mx := http.NewServeMux()
	log.Debugf("metrics server - ip:%s, port:%d", ip, port)
	mx.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
	metricsAddr := fmt.Sprintf(":%d", port)
	go func() {
		if err := http.ListenAndServe(metricsAddr, mx); err != nil {
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
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("panic err: %v", err)
		}
	}()
	if err := setup(c); err != nil {
		log.Errorf("mount setup() err: %v", err)
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

	log.Debugf("start mount service")
	server, err := fuse.Server(c.String("mount-point"), *opts)
	if err != nil {
		log.Fatalf("mount fail: %v", err)
		os.Exit(-1)
	}
	server.Wait()
	return cleanCache()
}

func cleanCache() (errRet error) {
	// clean cache if set
	if cleanCacheInfo.Clean {
		log.Infof("start clean cache dir: %+v", cleanCacheInfo)
		for _, dir := range cleanCacheInfo.CachePaths {
			if err := os.RemoveAll(dir); err != nil {
				log.Errorf("doUmount: remove path[%s] failed: %v", dir, err)
				errRet = err
			}
		}
	}
	return errRet
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
	} else if c.String(schema.FuseKeyFsInfo) != "" {
		fs, err := utils.ProcessFSInfo(c.String(schema.FuseKeyFsInfo))
		if err != nil {
			retErr := fmt.Errorf("InitVFS process fs info[%s] err: %v", c.String(schema.FuseKeyFsInfo), err)
			log.Errorf(retErr.Error())
			return retErr
		}
		fsMeta.ID = fs.ID
		fsMeta.Name = fs.Name
		fsMeta.ServerAddress = fs.ServerAddress
		fsMeta.SubPath = fs.SubPath
		fsMeta.Properties = fs.PropertiesMap
		fsMeta.UfsType = fs.Type
		fsMeta.Type = "fs"
	} else if c.String("config") != "" {
		reader, err := os.Open(c.String("config"))
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

		httpClient, err := client.NewHttpClient(server, client.DefaultTimeOut)
		if err != nil {
			return err
		}

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

		fuseClient, err := base.NewClient(fsID, httpClient, loginResponse.Authorization)
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
		PathCacheExpire:  c.Duration("path-cache-expire"),
		Config: kv.Config{
			FsID:      fsMeta.ID,
			Driver:    c.String("meta-cache-driver"),
			CachePath: c.String("meta-cache-path"),
		},
	}
	d := cache.Config{
		BlockSize:    c.Int("block-size"),
		MaxReadAhead: c.Int("data-read-ahead-size"),
		Expire:       c.Duration("data-cache-expire"),
		Config: kv.Config{
			CachePath: c.String("data-cache-path"),
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

	properties := fsMeta.Properties
	if properties[common.FileMode] == "" {
		properties[common.FileMode] = strconv.Itoa(fuse.FuseConf.FileMode)
	}
	if properties[common.DirMode] == "" {
		properties[common.DirMode] = strconv.Itoa(fuse.FuseConf.DirMode)
	}
	if _, err := vfs.InitVFS(fsMeta, links, true, vfsConfig, registry); err != nil {
		log.Errorf("init vfs failed: %v", err)
		return err
	}
	return nil
}

func signalHandle(mp string) {
	signalChan := make(chan os.Signal, 10)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGKILL)
	go func() {
		for {
			waitForSignal := <-signalChan
			log.Infof("fuse exit with signal %v", waitForSignal)
			go func() {
				if doUmount(mp, false) != nil {
					if err := doUmount(mp, true); err != nil {
						log.Errorf("doUmount failed: %v", err)
					}
				}
			}()
			go func() {
				time.Sleep(time.Second * 3)
				os.Exit(1)
			}()
		}
	}()
}
