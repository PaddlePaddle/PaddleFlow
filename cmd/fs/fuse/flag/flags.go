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

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fuse"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
)

func BasicFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:  "local",
			Value: false,
			Usage: "local mode for test",
		},
		&cli.StringFlag{
			Name:  "server",
			Value: "127.0.0.1:8082",
			Usage: "pfs server for REST request",
		},
		&cli.StringFlag{
			Name:  "fs-id",
			Value: "",
			Usage: "filesystem ID",
		},
		&cli.StringFlag{
			Name:  "config",
			Value: "",
			Usage: "filesystem config",
		},
		&cli.StringFlag{
			Name:  schema.FuseKeyFsInfo,
			Value: "",
			Usage: "filesystem config in json string",
		},
		&cli.StringFlag{
			Name:  "local-root",
			Value: "",
			Usage: "local root for fs",
		},
		&cli.BoolFlag{
			Name:  "ignore-security-labels",
			Value: true,
			Usage: "ignore security labels",
		},
	}
}

func CacheFlags(fuseConf *fuse.FuseConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "data-cache-path",
			Value: "/var/cache/pfs-cache-dir/data-cache",
			Usage: "data cache local path",
		},
		&cli.StringFlag{
			Name:  "meta-cache-driver",
			Value: kv.LevelDB,
			Usage: "meta cache driver, e.g. mem, leveldb",
		},
		&cli.StringFlag{
			Name:  "meta-cache-path",
			Value: "/var/cache/pfs-cache-dir/meta-cache",
			Usage: "meta cache local path",
		},
		&cli.DurationFlag{
			Name:  "data-cache-expire",
			Value: 0,
			Usage: "data cache expire",
		},
		&cli.DurationFlag{
			Name:  "meta-cache-expire",
			Value: 5 * time.Second,
			Usage: "meta cache expire",
		},
		&cli.DurationFlag{
			Name:  "entry-cache-expire",
			Value: 5 * time.Second,
			Usage: "entry cache expire",
		},
		&cli.IntFlag{
			Name:  "block-size",
			Value: 20971520,
			Usage: "block size",
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
			Name:  "data-read-ahead-size",
			Value: 200 * 1024 * 1024,
			Usage: "size of read-ahead data",
		},
	}
}

func MountFlags(fuseConf *fuse.FuseConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "mount-point",
			Aliases: []string{"mp"},
			Value:   "./mock",
			Usage:   "an empty path to mount",
		},
		&cli.StringFlag{
			Name:    "mount-options",
			Aliases: []string{"mo"},
			Value:   "",
			Usage:   "mount options",
		},
		&cli.BoolFlag{
			Name:  "disable-xattrs",
			Value: true,
			Usage: "kernel does not issue anyXAttr operations at all",
		},
		&cli.IntFlag{
			Name:        "dir-mode",
			Value:       ufs.DefaultDirMode,
			Usage:       "Permission bits for directories, only effective for S3 file system. (default: 0755)",
			Destination: &fuseConf.DirMode,
		},
		&cli.IntFlag{
			Name:        "file-mode",
			Value:       ufs.DefaultFileMode,
			Usage:       "Permission bits for files, only effective for S3 file system. (default: 0644)",
			Destination: &fuseConf.FileMode,
		},
	}
}

func LinkFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "link-root",
			Value: "",
			Usage: "local root for mock link",
		},
		&cli.StringFlag{
			Name:  "link-path",
			Value: "",
			Usage: "fs path for link",
		},
		&cli.BoolFlag{
			Name:  "skip-check-links",
			Value: true,
			Usage: "skip check links",
		},
		&cli.IntFlag{
			Name:  "link-update-interval",
			Value: 15,
			Usage: "link update interval",
		},
		&cli.StringFlag{
			Name:  "link-meta-dir-prefix",
			Value: "",
			Usage: "link meta dir prefix",
		},
	}
}

func UserFlags(fuseConf *fuse.FuseConfig) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "user-name",
			Value: "root",
			Usage: "username",
		},
		&cli.StringFlag{
			Name:  "password",
			Usage: "fs server password for fs username",
		},
		&cli.BoolFlag{
			Name:  "allow-other",
			Value: true,
			Usage: "allow other user to access fs",
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
			Name:        "raw-owner",
			Value:       false,
			Usage:       "show the same uid and gid to ufs",
			Destination: &fuseConf.RawOwner,
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
