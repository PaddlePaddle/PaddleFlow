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

package fs

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/fs/client/base"
	"paddleflow/pkg/fs/server/api/common"
)

const (
	HDFS                          = "hdfs"
	HDFSWithKerberos              = "hdfsWithKerberos"
	S3                            = "s3"
	Local                         = "local"
	SFTP                          = "sftp"
	Mock                          = "mock"
	IPDomainOrIPDomainPortPattern = "^([a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+)" +
		"(:([1-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{3}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]))?$"

	TypeSplit          = 0
	HDFSSplit          = 3
	S3Split            = 3
	S3EndpointSplit    = 2
	S3SplitLen         = 4
	LocalSplit         = 2
	ServerAddressSplit = 2
	S3AddressLength    = 3
	IDSliceLen         = 3

	FsPrefix = "fs-"
	UserRoot = "root"
)

func ID(userName, fsName string) string {
	return FsPrefix + userName + "-" + fsName
}

// InformationFromURL get fs system information from url
func InformationFromURL(url string, properties map[string]string) (fileSystemType, serverAddress, subPath string) {
	fileSystemType = strings.Split(url, ":")[TypeSplit]
	serverAddress = ""
	subPath = ""
	urlSplit := strings.Split(url, "/")

	switch fileSystemType {
	case Local, Mock:
		serverAddress = ""
		subPath = "/" + SubPathFromUrl(urlSplit, LocalSplit)
	case HDFS:
		serverAddress = urlSplit[ServerAddressSplit]
		if properties == nil {
			properties = make(map[string]string)
		}
		properties[base.NameNodeAddress] = serverAddress
		subPath = "/" + SubPathFromUrl(urlSplit, HDFSSplit)
		if properties[base.KeyTabData] != "" {
			fileSystemType = HDFSWithKerberos
		}
	case SFTP:
		serverAddress = urlSplit[ServerAddressSplit]
		subPath = "/" + SubPathFromUrl(urlSplit, HDFSSplit)
	case S3:
		serverAddress = properties[base.Endpoint]
		subPath = "/" + SubPathFromUrl(urlSplit, S3Split)
	default:
		return
	}
	return
}

func SubPathFromUrl(urlSplit []string, split int) string {
	temp := urlSplit[split:]
	return strings.Join(temp, "/")
}

func CheckKerberosProperties(properties map[string]string) error {
	if properties[base.Principal] == "" {
		return common.InvalidField(base.Principal, fmt.Sprintf("kerberos hdfs, %s must be provided", base.Principal))
	}
	if properties[base.KeyTabData] == "" {
		return common.InvalidField(base.KeyTabData, fmt.Sprintf("kerberos hdfs, %s must be provided", base.KeyTabData))
	}
	if properties[base.DataTransferProtection] == "" {
		return common.InvalidField(base.DataTransferProtection, fmt.Sprintf("kerberos hdfs, %s must be provided", base.DataTransferProtection))
	}

	_, err := base64.StdEncoding.DecodeString(properties[base.KeyTabData])
	if err != nil {
		log.Errorf("Decode keyTab data failed, %v", err)
		return err
	}

	if KDCStr, ok := properties[base.Kdc]; ok {
		for _, kdc := range strings.Split(KDCStr, ",") {
			if matched, err := regexp.MatchString(IPDomainOrIPDomainPortPattern, kdc); err != nil || !matched {
				return common.InvalidField(base.Kdc,
					"kerberos.kdc should be ip or ip:port format")
			}
		}
	} else {
		return common.InvalidField(base.Kdc,
			"for kerberos hdfs, kerberos.Kdc must be provided")
	}

	nameNodePrincipal, ok := properties[base.NameNodePrincipal]
	if !ok && nameNodePrincipal == "" {
		return common.InvalidField(base.NameNodePrincipal,
			"for kerberos hdfs, kerberos.namenode.principal must be provided")
	}

	if realm, ok := properties[base.Realm]; ok {
		if strings.Contains(nameNodePrincipal, "@") &&
			!strings.HasSuffix(nameNodePrincipal, "@"+realm) {
			return common.InvalidField(base.Realm, fmt.Sprintf("nameNodePrincipal has not suffix realm[%s]", realm))
		}
	} else {
		return common.InvalidField(base.Realm, fmt.Sprintf("kerberos hdfs, %s must be provided", base.Realm))
	}

	return nil
}

func CheckFsNested(path1, path2 string) bool {
	path1 = strings.TrimRight(path1, "/")
	path2 = strings.TrimRight(path2, "/")

	path1Arr := strings.Split(path1, "/")
	path2Arr := strings.Split(path2, "/")

	minIndex := len(path1Arr)
	if len(path1Arr) > len(path2Arr) {
		minIndex = len(path2Arr)
	}

	for index := 0; index < minIndex; index++ {
		if path1Arr[index] != path2Arr[index] {
			return false
		}
	}
	return true
}

// NameToFsID if user use fsName as fsID (ex. fsName=fs-root-abc), then return fsName(fs-root-abc) direct
func NameToFsID(fsName, userName string) string {
	fsTemp := strings.Split(fsName, "-")
	fsUserName := strings.TrimPrefix(fsName, fsTemp[0]+"-")
	fsUserName = strings.TrimSuffix(fsUserName, "-"+fsTemp[len(fsTemp)-1])
	log.Debugf("fsUserName %s", fsUserName)
	if len(fsTemp) < IDSliceLen || fsTemp[0] != "fs" || (fsUserName != userName && userName != UserRoot) {
		fsName = ID(userName, fsName)
	}
	return fsName
}

func FSIDToName(fsID string) string {
	fsArr := strings.Split(fsID, "-")
	return fsArr[len(fsArr)-1]
}
