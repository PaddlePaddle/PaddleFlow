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
	"fmt"
	"path"
	"strings"
	"time"

	libfuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"paddleflow/cmd/fs/fuse/app/options"
	"paddleflow/pkg/client"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/http/api"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/client/fuse"
	"paddleflow/pkg/fs/client/vfs"
	mountUtil "paddleflow/pkg/fs/utils/mount"
)

var opts *libfuse.MountOptions

func initConfig() {
	// init from yaml config
	config.InitFuseConfig()
	f := options.NewFuseOption()
	f.InitFlag(pflag.CommandLine)
	logger.Init(&config.FuseConf.Log)
	fmt.Printf("The final fuse config is:\n %s \n", config.PrettyFormat(config.FuseConf))
}

func Init() error {
	initConfig()
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
	if err := InitVFS(); err != nil {
		log.Errorf("init vfs failed: %v", err)
		return err
	}
	return nil
}

func Mount() (*libfuse.Server, error) {
	fuseConf := config.FuseConf.Fuse
	log.Debugf("the opts is %+v", opts)
	return mount(fuseConf.MountPoint, opts)
}

// Mount mounts the given NodeFS on the directory, and starts serving
// requests. This is a convenience wrapper around NewNodeFS and
// fuse.NewServer.  If nil is given as options, default settings are
// applied, which are 1 second entry and attribute timeout.
// todo: remove
func oldMount(dir string, mops *libfuse.MountOptions) (*libfuse.Server, error) {
	fuseConf := config.FuseConf.Fuse
	nops := nodefs.NewOptions()
	nops.Debug = mops.Debug
	nops.EntryTimeout = time.Duration(fuseConf.EntryTimeout) * time.Second
	nops.AttrTimeout = time.Duration(fuseConf.AttrTimeout) * time.Second
	if fuseConf.RawOwner {
		nops.Owner = nil
	} else {
		nops.Uid = fuseConf.Uid
		nops.Gid = fuseConf.Gid
	}
	rawFS := fuse.NewFileSystem(mops.Debug)
	pathFS := pathfs.NewPathNodeFs(rawFS, &pathfs.PathNodeFsOptions{Debug: mops.Debug})
	server, _, err := nodefs.Mount(dir, pathFS.Root(), mops, nops)
	if err != nil {
		log.Fatal("Mount fail:", err)
		return nil, err
	}

	go server.Serve()
	if err := server.WaitMount(); err != nil {
		// we don't shutdown the serve loop. If the mount does
		// not succeed, the loop won't work and exit.
		return nil, err
	}

	return server, nil
}

func mount(dir string, mops *libfuse.MountOptions) (*libfuse.Server, error) {
	fuseConfig := config.FuseConf.Fuse
	mops.AllowOther = fuseConfig.AllowOther
	mops.DisableXAttrs = fuseConfig.DisableXAttrs
	server, err := fuse.Server(dir, *mops)

	return server, err
}

func InitVFS() error {
	var fsMeta base.FSMeta
	var links map[string]base.FSMeta
	fuseConf := config.FuseConf.Fuse
	if fuseConf.Local == true {
		if fuseConf.LocalRoot == "" || fuseConf.LocalRoot == "/" {
			log.Errorf("invalid localRoot: [%s]", fuseConf.LocalRoot)
			return fmt.Errorf("invalid localRoot: [%s]", fuseConf.LocalRoot)
		}
		fsMeta = base.FSMeta{
			UfsType: base.LocalType,
			SubPath: fuseConf.LocalRoot,
		}
		if fuseConf.LinkPath != "" && fuseConf.LinkRoot != "" {
			links = map[string]base.FSMeta{
				path.Clean(fuseConf.LinkPath): base.FSMeta{
					UfsType: base.LocalType,
					SubPath: fuseConf.LinkRoot,
				},
			}
		}
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

	vfsConfig := vfs.InitConfig(
		vfs.WithMemorySize(config.FuseConf.Fuse.MemorySize),
		vfs.WithMemoryExpire(config.FuseConf.Fuse.MemoryExpire),
		vfs.WithBlockSize(config.FuseConf.Fuse.BlockSize),
		vfs.WithDiskCachePath(config.FuseConf.Fuse.DiskCachePath),
		vfs.WithDiskExpire(config.FuseConf.Fuse.DiskExpire),
	)

	if _, err := vfs.InitVFS(fsMeta, links, true, vfsConfig); err != nil {
		log.Errorf("init vfs failed: %v", err)
		return err
	}
	return nil
}
