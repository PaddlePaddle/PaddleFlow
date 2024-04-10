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

const (
	LocalType            = "local"
	HDFSType             = "hdfs"
	HDFSWithKerberosType = "hdfsWithKerberos"
	S3Type               = "s3"
	SFTPType             = "sftp"
	MockType             = "mock"
	CFSType              = "cfs"
	GlusterFSType        = "glusterfs"
	AFSType              = "afs"
	BosType              = "bos"

	// common
	Owner = "owner"
	Group = "group"

	// local properties and root path
	RootKey = "root"
	SubPath = "subpath"
	Type    = "type"

	// HDFS properties
	NameNodeAddress     = "dfs.namenode.address"
	UseDatanodeHostname = "dfs.client.use.datanode.hostname"
	UserKey             = "user"
	BlockSizeKey        = "blockSize"
	ReplicationKey      = "replication"

	Sts             = "sts"
	Token           = "token"
	FsName          = "fsname"
	UserName        = "userName"
	StsDuration     = "duration"
	StsACL          = "acl"
	BosSessionToken = "sessionToken"
	Server          = "server"

	// AFS properties
	AFSUser     = "username"
	AFSPassword = "password"

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
	ImplicitDir        = "implicitDir"

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
