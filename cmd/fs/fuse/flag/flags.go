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

package flag

import (
	"os"
	"time"

	"github.com/urfave/cli/v2"

	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/fs/client/meta"
)

func GlobalFlags(fuseConf *config.Fuse) []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:        "local",
			Value:       false,
			Usage:       "local mode for test",
			Destination: &fuseConf.Local,
		},
		&cli.StringFlag{
			Name:        "server",
			Value:       "127.0.0.1:8082",
			Usage:       "pfs server for REST request",
			Destination: &fuseConf.Server,
		},
		&cli.StringFlag{
			Name:        "fs-id",
			Value:       "",
			Usage:       "filesystem ID",
			Destination: &fuseConf.FsID,
		},
		&cli.StringFlag{
			Name:        "fs-info-path",
			Value:       "",
			Usage:       "filesystem info path",
			Destination: &fuseConf.FsInfoPath,
		},
		&cli.StringFlag{
			Name:        "local-root",
			Value:       "",
			Usage:       "local root for fs",
			Destination: &fuseConf.LocalRoot,
		},
		&cli.StringFlag{
			Name:        "dep-path",
			Value:       "./dep",
			Usage:       "dependency path",
			Destination: &fuseConf.DepPath,
		},
		&cli.BoolFlag{
			Name:        "ignore-security-labels",
			Value:       true,
			Usage:       "ignore security labels",
			Destination: &fuseConf.IgnoreSecurityLabels,
		},
	}
}

func CacheFlags(fuseConf *config.Fuse) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "data-disk-cache-path",
			Value:       "/var/cache/pfs_cache_dir",
			Usage:       "data-disk-cache-path",
			Destination: &fuseConf.DiskCachePath,
		},
		&cli.StringFlag{
			Name:        "meta-driver",
			Value:       meta.DefaultName,
			Usage:       "fuse meta driver: mem, leveldb",
			Destination: &fuseConf.MetaDriver,
		},
		&cli.StringFlag{
			Name:        "meta-path",
			Value:       "/var/cache/pfs_cache_dir/meta-driver",
			Usage:       "meta cache local path",
			Destination: &fuseConf.MetaCachePath,
		},
		&cli.DurationFlag{
			Name:        "data-mem-cache-expire",
			Value:       100 * time.Second,
			Usage:       "fuse memory data cache expire",
			Destination: &fuseConf.MemoryExpire,
		},
		&cli.DurationFlag{
			Name:        "data-disk-cache-expire",
			Value:       15 * 60 * time.Second,
			Usage:       "fuse disk data cache expire",
			Destination: &fuseConf.DiskExpire,
		},
		&cli.DurationFlag{
			Name:        "meta-cache-expire",
			Value:       10 * time.Second,
			Usage:       "fuse meta cache expire",
			Destination: &fuseConf.MetaCacheExpire,
		},
		&cli.DurationFlag{
			Name:        "entry-cache-expire",
			Value:       10 * time.Second,
			Usage:       "fuse entry cache expire",
			Destination: &fuseConf.EntryCacheExpire,
		},
		&cli.IntFlag{
			Name:        "data-mem-size",
			Value:       0,
			Usage:       "number of data cache item in mem cache",
			Destination: &fuseConf.MemorySize,
		},
		&cli.IntFlag{
			Name:        "block-size",
			Value:       0,
			Usage:       "fuse block size",
			Destination: &fuseConf.BlockSize,
		},
		&cli.IntFlag{
			Name:        "attr-timeout",
			Value:       1,
			Usage:       "attribute cache TTL",
			Destination: &fuseConf.AttrTimeout,
		},
		&cli.IntFlag{
			Name:        "entry-timeout",
			Value:       1,
			Usage:       "entry cache TTL",
			Destination: &fuseConf.EntryTimeout,
		},
		&cli.IntFlag{
			Name:        "data-read-ahead-size",
			Value:       200 * 1024 * 1024,
			Usage:       "size of read-ahead data",
			Destination: &fuseConf.MaxReadAheadSize,
		},
	}
}

func MountFlags(fuseConf *config.Fuse) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "mount-point",
			Aliases:     []string{"mp"},
			Value:       "./mock",
			Usage:       "an empty path to mount",
			Destination: &fuseConf.MountPoint,
		},
		&cli.StringFlag{
			Name:        "mount-options",
			Aliases:     []string{"mo"},
			Value:       "",
			Usage:       "mount options",
			Destination: &fuseConf.MountOptions,
		},
		&cli.BoolFlag{
			Name:        "disable-xattrs",
			Value:       true,
			Usage:       "kernel does not issue anyXAttr operations at all",
			Destination: &fuseConf.DisableXAttrs,
		},
	}
}

func LinkFlags(fuseConf *config.Fuse) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "link-root",
			Value:       "",
			Usage:       "local root for mock link",
			Destination: &fuseConf.LinkRoot,
		},
		&cli.StringFlag{
			Name:        "link-path",
			Value:       "",
			Usage:       "fs path for link",
			Destination: &fuseConf.LinkPath,
		},
		&cli.IntFlag{
			Name:        "link-update-interval",
			Value:       15,
			Usage:       "link update interval",
			Destination: &fuseConf.LinkUpdateInterval,
		},
		&cli.StringFlag{
			Name:        "link-meta-dir-prefix",
			Value:       "",
			Usage:       "link meta dir prefix",
			Destination: &fuseConf.LinkMetaDirPrefix,
		},
		&cli.BoolFlag{
			Name:        "skip-check-links",
			Value:       false,
			Usage:       "skip check links",
			Destination: &fuseConf.SkipCheckLinks,
		},
	}
}

func UserFlags(fuseConf *config.Fuse) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "user-name",
			Value:       "root",
			Usage:       "username",
			Destination: &fuseConf.UserName,
		},
		&cli.StringFlag{
			Name:        "password",
			Usage:       "fs server password for fs username",
			Destination: &fuseConf.Password,
		},
		&cli.IntFlag{
			Name:        "uid",
			Value:       os.Getuid(),
			Usage:       "uid given to replace default uid",
			Destination: &fuseConf.Uid,
		},
		&cli.IntFlag{
			Name:        "gid",
			Value:       os.Getgid(),
			Usage:       "gid given to replace default gid",
			Destination: &fuseConf.Gid,
		},
		&cli.BoolFlag{
			Name:        "allow-other",
			Value:       true,
			Usage:       "allow other user to access fs",
			Destination: &fuseConf.AllowOther,
		},
		&cli.BoolFlag{
			Name:        "raw-owner",
			Value:       false,
			Usage:       "show the same uid and gid to ufs",
			Destination: &fuseConf.RawOwner,
		},
	}
}

func MetricsFlags(fuseConf *config.Fuse) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "schema",
			Value: "ufmco",
			Usage: "schema string that controls the output sections (u: usage, f: fuse, m: meta, c: blockcache, o: object, g: go)",
		},
		&cli.UintFlag{
			Name:  "interval",
			Value: 1,
			Usage: "interval in seconds between each update",
		},
		&cli.UintFlag{
			Name:    "verbosity",
			Aliases: []string{"l"},
			Usage:   "verbosity level, 0 or 1 is enough for most cases",
		},
		&cli.BoolFlag{
			Name:        "pprof-enable",
			Value:       false,
			Usage:       "enable go pprof",
			Destination: &fuseConf.PprofEnable,
		},
		&cli.IntFlag{
			Name:        "pprof-port",
			Value:       6060,
			Usage:       "pprof port",
			Destination: &fuseConf.PprofPort,
		},
		&cli.IntFlag{
			Name:        "metrics-port",
			Value:       8993,
			Usage:       "metrics service port",
			Destination: &fuseConf.MetricsPort,
		},
	}
}

func LogFlags(logConf *logger.LogConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "log-dir",
			Value:       "./log",
			Usage:       "directory of log",
			Destination: &logConf.Dir,
		},
		&cli.StringFlag{
			Name:        "log-file-prefix",
			Value:       "./pfs-fuse",
			Usage:       "prefix of log file",
			Destination: &logConf.FilePrefix,
		},
		&cli.StringFlag{
			Name:        "log-level",
			Value:       "INFO",
			Usage:       "log level",
			Destination: &logConf.Level,
		},
		&cli.StringFlag{
			Name:        "log-formatter",
			Value:       "",
			Usage:       "log formatter",
			Destination: &logConf.Formatter,
		},
		&cli.BoolFlag{
			Name:        "log-is-compress",
			Value:       true,
			Usage:       "log compress",
			Destination: &logConf.IsCompress,
		},
		&cli.IntFlag{
			Name:        "log-max-keep-days",
			Value:       90,
			Usage:       "log max keep days",
			Destination: &logConf.MaxKeepDays,
		},
		&cli.IntFlag{
			Name:        "log-max-file-num",
			Value:       100,
			Usage:       "log max file number",
			Destination: &logConf.MaxFileNum,
		},
		&cli.IntFlag{
			Name:        "log-max-file-size-in-mb",
			Value:       200 * 1024 * 1024,
			Usage:       "log max file size in MiB",
			Destination: &logConf.MaxFileSizeInMB,
		},
	}
}

func ExpandFlags(compoundFlags [][]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, flag := range compoundFlags {
		flags = append(flags, flag...)
	}
	return flags
}
