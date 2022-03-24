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

package options

import (
	goflag "flag"

	"github.com/spf13/pflag"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
)

// FuseOption is the main context object for the controllers.
type FuseOption struct{}

func NewFuseOption() *FuseOption {
	return &FuseOption{}
}

func (f *FuseOption) AddFlagSet(fs *pflag.FlagSet) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	fuseConf := &config.FuseConf.Fuse
	fs.StringVar(&fuseConf.MountPoint, "mount-point", fuseConf.MountPoint, "The path to mount")
	fs.StringVar(&fuseConf.MountOptions, "mount-options", fuseConf.MountOptions, "The mount options")
	fs.StringVar(&fuseConf.Server, "server", fuseConf.Server, "The pfs server for grpc")
	fs.StringVar(&fuseConf.FsID, "fs-id", fuseConf.FsID, "The filesystem ID")
	fs.StringVar(&fuseConf.FsInfoPath, "fs-info-path", fuseConf.FsInfoPath, "The filesystem info path")
	fs.BoolVar(&fuseConf.Local, "local", fuseConf.Local, "Local mode for test")
	fs.StringVar(&fuseConf.LocalRoot, "local-root", fuseConf.LocalRoot, "The local root for fs")
	fs.StringVar(&fuseConf.LinkRoot, "link-root", fuseConf.LinkRoot, "The local root for mock link")
	fs.StringVar(&fuseConf.LinkPath, "link-path", fuseConf.LinkPath, "The fs path for link")
	fs.StringVar(&fuseConf.DepPath, "dep-path", fuseConf.DepPath, "The dependency path")
	fs.StringVar(&fuseConf.UserName, "user-name", fuseConf.UserName, "User name")
	fs.StringVar(&fuseConf.Password, "password", fuseConf.Password, "The fs server password for fsusername")
	fs.IntVar(&fuseConf.AttrTimeout, "attr-timeout", fuseConf.AttrTimeout, "Attribute cache TTL")
	fs.IntVar(&fuseConf.EntryTimeout, "entry-timeout", fuseConf.EntryTimeout, "Entry cache TTL")
	fs.Uint32Var(&fuseConf.Uid, "uid", fuseConf.Uid, "The given UID to replace default uid")
	fs.Uint32Var(&fuseConf.Gid, "gid", fuseConf.Gid, "The given GID to replace default gid")
	fs.BoolVar(&fuseConf.IgnoreSecurityLabels, "ignore-security-labels", fuseConf.IgnoreSecurityLabels,
		"Ignore security labels")
	fs.BoolVar(&fuseConf.DisableXAttrs, "disable-xattrs", fuseConf.DisableXAttrs,
		"The kernel does not issue anyXAttr operations at all")
	fs.BoolVar(&fuseConf.AllowOther, "allow-other", fuseConf.AllowOther, "Allow other user to access fs")
	fs.BoolVar(&fuseConf.RawOwner, "raw-owner", fuseConf.RawOwner, "Show the same uid and gid to ufs")
	fs.BoolVar(&fuseConf.PprofEnable, "pprof-enable", fuseConf.PprofEnable, "Enable go pprof")
	fs.IntVar(&fuseConf.PprofPort, "pprof-port", fuseConf.PprofPort, "Pprof port")
	fs.IntVar(&fuseConf.LinkUpdateInterval, "link-update-interval", fuseConf.LinkUpdateInterval, "The link update interval")
	fs.StringVar(&fuseConf.LinkMetaDirPrefix, "link-meta-dir-prefix", fuseConf.LinkMetaDirPrefix, "The link meta dir prefix")
	fs.BoolVar(&fuseConf.SkipCheckLinks, "skip-check-links", fuseConf.SkipCheckLinks, "Skip check links")
	fs.IntVar(&fuseConf.MaxReadAheadSize, "data-read-ahead-size", fuseConf.MaxReadAheadSize, "The size of read-ahead data")
	fs.DurationVar(&fuseConf.MemoryExpire, "data-mem-cache-expire", fuseConf.MemoryExpire, "The fuse memory data cache expire")
	fs.IntVar(&fuseConf.MemorySize, "data-mem-size", fuseConf.MemorySize, "The number of data cache item in mem cache")
	fs.DurationVar(&fuseConf.DiskExpire, "data-disk-cache-expire", fuseConf.DiskExpire, "The fuse disk data cache expire")
	fs.IntVar(&fuseConf.BlockSize, "block-size", fuseConf.BlockSize, "The fuse block size")
	fs.StringVar(&fuseConf.DiskCachePath, "data-disk-cache-path", fuseConf.DiskCachePath, "The disk cache path")
	fs.StringVar(&fuseConf.MetaDriver, "meta-driver", fuseConf.MetaDriver, "The fuse meta driver")
	fs.StringVar(&fuseConf.MetaCachePath, "meta-path", fuseConf.MetaCachePath, "The meta cache local path")
	fs.DurationVar(&fuseConf.MetaCacheExpire, "meta-cache-expire", fuseConf.MetaCacheExpire, "The fuse meta cache expire")
	fs.DurationVar(&fuseConf.EntryCacheExpire, "entry-cache-expire", fuseConf.EntryCacheExpire, "The fuse entry cache expire")
	fs.IntVar(&fuseConf.MetricsPort, "metrics-port", fuseConf.MetricsPort, "metrics server port")
}

func (f *FuseOption) InitFlag(fs *pflag.FlagSet) {
	if fs == nil {
		fs = pflag.CommandLine
	}
	f.AddFlagSet(fs)
	logger.AddFlagSet(fs, &config.FuseConf.Log)
	pflag.CommandLine.AddGoFlagSet(goflag.CommandLine)
	pflag.Parse()
}
