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

package config

import (
	"fmt"
	"os"
	"time"

	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/fs/client/meta"
)

var defaultFuseConfig = FuseConfig{
	Log: logger.LogConfig{
		Dir:             "./log",
		FilePrefix:      "./pfs-fuse",
		Level:           "INFO",
		MaxKeepDays:     90,
		MaxFileNum:      100,
		MaxFileSizeInMB: 200 * 1024 * 1024,
		IsCompress:      true,
	},
	Fuse: Fuse{
		MountPoint:           "./mock",
		MountOptions:         "",
		Server:               "10.21.195.71:8082",
		FsID:                 "",
		Local:                false,
		LocalRoot:            "",
		LinkRoot:             "",
		LinkPath:             "",
		DepPath:              "./dep",
		UserName:             "root",
		EntryTimeout:         1,
		AttrTimeout:          1,
		Uid:                  os.Getuid(),
		Gid:                  os.Getgid(),
		IgnoreSecurityLabels: true,
		DisableXAttrs:        true,
		AllowOther:           true,
		RawOwner:             false,
		PprofEnable:          false,
		PprofPort:            6060,
		LinkUpdateInterval:   15,
		LinkMetaDirPrefix:    "",
		SkipCheckLinks:       false,
		MetaDriver:           meta.DefaultName,
		MetricsPort:          8993,
		Cache: Cache{
			MemoryExpire:     100 * time.Second,
			MemorySize:       0, // memorySize * BlockSize才是实际的内存cache大小
			BlockSize:        0, // BlockSize == 0 表示关闭cache
			DiskCachePath:    "/var/cache/pfs_cache_dir",
			DiskExpire:       15 * 60 * time.Second,
			MetaCacheExpire:  10 * time.Second,
			EntryCacheExpire: 10 * time.Second,
			MetaCachePath:    "/var/cache/pfs_cache_dir/meta-driver",
			MaxReadAheadSize: 200 * 1024 * 1024,
		},
	},
}

type FuseConfig struct {
	Log  logger.LogConfig `yaml:"log"`
	Fuse Fuse             `yaml:"fuse"`
}

// fuse config
type Fuse struct {
	MountPoint           string `yaml:"mountPoint"`
	MountOptions         string `yaml:"mountOptions"`
	Server               string `yaml:"server"`
	FsID                 string `yaml:"fsID"`
	FsInfoPath           string `yaml:"fsInfoPath"`
	Local                bool   `yaml:"local"`
	LocalRoot            string `yaml:"localRoot"`
	LinkRoot             string `yaml:"linkRoot"`
	LinkPath             string `yaml:"linkPath"`
	DepPath              string `yaml:"depPath"`
	UserName             string `yaml:"userName"`
	EntryTimeout         int    `yaml:"entryTimeout"`
	AttrTimeout          int    `yaml:"attrTimeout"`
	Uid                  int    `yaml:"uid"`
	Gid                  int    `yaml:"gid"`
	IgnoreSecurityLabels bool   `yaml:"ignoreSecurityLabels"`
	DisableXAttrs        bool   `yaml:"disableXAttrs"`
	AllowOther           bool   `yaml:"allowOther"`
	RawOwner             bool   `yaml:"rawOwner"`
	PprofEnable          bool   `yaml:"pprofEnable"`
	PprofPort            int    `yaml:"pprofPort"`
	LinkUpdateInterval   int    `yaml:"linkUpdateInterval"`
	LinkMetaDirPrefix    string `yaml:"linkMetaDirPrefix"`
	SkipCheckLinks       bool   `yaml:"skipCheckLinks"`
	Cache                `yaml:"cache"`
	Password             string `yaml:"password"`
	MetaDriver           string `yaml:"metaDriver"`
	MetricsPort          int    `yaml:"metricsPort"`
}

type Cache struct {
	BlockSize        int
	MemorySize       int
	MemoryExpire     time.Duration
	DiskExpire       time.Duration
	DiskCachePath    string
	MetaCacheExpire  time.Duration
	EntryCacheExpire time.Duration
	MetaCachePath    string
	MaxReadAheadSize int
}

var (
	FuseConf *FuseConfig
)

func InitFuseConfig() {
	fmt.Println("Init Config")
	FuseConf = &defaultFuseConfig
	// Fuse暂时不需要配置文件
	if err := InitConfigFromUserYaml(FuseConf, ""); err != nil {
		panic(err)
	}
}
