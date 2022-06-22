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

package pfs

import (
	"fmt"
	"path/filepath"
	"strings"

	"paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/fs/csiplugin/csiconfig"
	csiCommon "paddleflow/pkg/fs/utils/common"
)

const (
	mountName       = "mount"
	pfsMountCmdName = "./mount.sh"
	ReadOnly        = "ro"
	ReadWrite       = "rw"

	UIDOption       = "--uid=%d"
	GIDOption       = "--gid=%d"
	MountPoint      = "--mount-point=%s"
	MountOptions    = "--mount-options=%s"
	UserNameOption  = "--user-name=%s"
	FSIDOptions     = "--fs-id=%s"
	PFSServerOption = "--server=%s"
)

type FSMountParameter struct {
	FSID   string
	Server string
	UserID string
}

type MountInfo struct {
	Server        string
	FSID          string
	TargetPath    string
	LocalPath     string
	UsernameRoot  string
	PasswordRoot  string
	ClusterID     string
	Type          string
	ServerAddress string
	SubPath       string
	UID           int
	GID           int
	Options       []string
}

func (m *MountInfo) GetMountCmd() (string, []string) {
	var cmdName string
	var args []string
	if m.Type == common.Glusterfs {
		cmdName = mountName
		args = append(args, "-t", m.Type)
		if csiconfig.GlusterFsLogLevel != "" {
			m.Options = append(m.Options, fmt.Sprintf("log-level=%s", csiconfig.GlusterFsLogLevel))
			if csiconfig.GlusterFsLogPath == "" {
				csiconfig.GlusterFsLogPath = "/home/paddleflow/log/"
			}
			m.Options = append(m.Options, fmt.Sprintf("log-file=%s", filepath.Join(csiconfig.GlusterFsLogPath, "glusterfs-" + m.FSID+".log")))
		}
		for _, option := range m.Options {
			args = append(args, "-o", option)
		}
		args = append(args, strings.Join([]string{m.ServerAddress, m.SubPath}, ":"), m.LocalPath)
	} else {
		cmdName = pfsMountCmdName
		if len(m.Options) > 0 {
			mountOptions := strings.Join(m.Options[:], ",")
			args = append(args, fmt.Sprintf(MountOptions, mountOptions))
		}
		// set pfs-fuse arguments
		args = append(args, fmt.Sprintf(PFSServerOption, m.Server))
		args = append(args, fmt.Sprintf(FSIDOptions, m.FSID))
		args = append(args, fmt.Sprintf(UIDOption, m.UID))
		args = append(args, fmt.Sprintf(GIDOption, m.GID))
		args = append(args, fmt.Sprintf(MountPoint, m.LocalPath))
	}

	return cmdName, args
}

func GetMountInfo(id, server string, readOnly bool) MountInfo {
	return MountInfo{
		FSID:    id,
		Server:  server,
		UID:     csiCommon.GetDefaultUID(),
		GID:     csiCommon.GetDefaultGID(),
		Options: GetOptions(readOnly),
	}
}

func GetOptions(readOnly bool) []string {
	var options []string

	if readOnly {
		options = append(options, ReadOnly)
	}
	return options
}
