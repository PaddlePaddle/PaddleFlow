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

package mount

import (
	csiCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/common"
)

type Info struct {
	Server       string
	FSID         string
	FsInfoStr    string
	FsCacheStr   string
	TargetPath   string
	LocalPath    string
	UsernameRoot string
	PasswordRoot string
	ClusterID    string
	UID          int
	GID          int
	ReadOnly     bool
}

func GetMountInfo(id, server, fsInfo, fsCache string, readOnly bool) Info {
	return Info{
		FSID:       id,
		Server:     server,
		FsInfoStr:  fsInfo,
		FsCacheStr: fsCache,
		UID:        csiCommon.GetDefaultUID(),
		GID:        csiCommon.GetDefaultGID(),
		ReadOnly:   readOnly,
	}
}
