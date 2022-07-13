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

package common

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

const (
	LocalType            = "local"
	HDFSType             = "hdfs"
	HDFSWithKerberosType = "hdfsWithKerberos"
	S3Type               = "s3"
	SFTPType             = "sftp"
	MockType             = "mock"
	CFSType              = "cfs"
	GlusterfsType        = "glusterfs"

	// common
	Owner = "owner"
	Group = "group"

	// local properties and root path
	RootKey = "root"
	SubPath = "subpath"
	Type    = "type"

	// HDFS properties
	NameNodeAddress = "dfs.namenode.address"
	UserKey         = "user"
	BlockSizeKey    = "blockSize"
	ReplicationKey  = "replication"

	// HDFS Kerbers properties
	Realm                  = "kerberos.realm"
	Kdc                    = "kerberos.kdc"
	NameNodePrincipal      = "kerberos.namenode.principal"
	Principal              = "kerberos.principal"
	DataTransferProtection = "kerberos.data.transfer.protection"
	KeyTabData             = "kerberos.keytab.data"

	// S3 properties
	AccessKey          = "accessKey"
	SecretKey          = "secretKey"
	Endpoint           = "endpoint"
	Bucket             = "bucket"
	Region             = "region"
	InsecureSkipVerify = "insecureSkipVerify"
	S3ForcePathStyle   = "s3ForcePathStyle"
	DirMode            = "dirMode"
	FileMode           = "fileMode"

	// sftp properties
	Address  = "address"
	Password = "password"

	// mock properties
	PVC       = "pvc"
	Namespace = "namespace"

	// FSMeta类型
	FSType   = "fs"
	LinkType = "link"

	// Link操作
	ADD    = "ADD"
	DELETE = "DELETE"

	// Link Meta
	LinkMetaDir  = ".config"
	LinkMetaFile = "links_meta"
)

type FSMeta struct {
	ID            string
	Name          string
	UfsType       string
	ServerAddress string
	SubPath       string
	Properties    map[string]string
	// type: fs 表示是默认的后端存储；link 表示是外部存储
	Type string
}

func GetFsNameAndUserNameByFsID(fsID string) (userName, fsName string, err error) {
	fsArray := strings.Split(fsID, "-")
	if len(fsArray) < 3 {
		err = fmt.Errorf("fsID[%s] is not valid", fsID)
		log.Error(err.Error())
		return
	}
	if len(fsArray) > 3 {
		// such as fs-root-v-xxxx
		fsName = strings.Join(fsArray[2:len(fsArray)], "-")
		userName = fsArray[1]
		return
	}
	userName = strings.Join(fsArray[1:len(fsArray)-1], "")
	fsName = fsArray[len(fsArray)-1]
	return
}
