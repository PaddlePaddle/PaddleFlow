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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/common/schema"
	"paddleflow/pkg/fs/common"
)

const (
	RegPatternQueueName    = "^[a-z0-9][a-z0-9-]{0,8}[a-z0-9]$"
	RegPatternUserName     = "^[A-Za-z0-9-]{4,16}$"
	RegPatternRunName      = "^[A-Za-z0-9_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$"
	RegPatternResource     = "^[1-9][0-9]*([numkMGTPE]|Ki|Mi|Gi|Ti|Pi|Ei)?$"
	RegPatternPipelineName = "^[A-Za-z0-9_][A-Za-z0-9-_]{1,49}[A-Za-z0-9_]$"
	RegPatternClusterName  = "^[A-Za-z0-9_][A-Za-z0-9-_]{0,253}[A-Za-z0-9_]$"

	// DNS1123LabelMaxLength is a label's max length in DNS (RFC 1123)
	DNS1123LabelMaxLength = 63
	DNS1123LabelFmt       = "[a-z0-9]([-a-z0-9]*[a-z0-9])?"
	dns1123LabelErrMsg    = "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-'," +
		" and must start and end with an alphanumeric character"

	HDFS                          = "hdfs"
	HDFSWithKerberos              = "hdfsWithKerberos"
	S3                            = "s3"
	Local                         = "local"
	SFTP                          = "sftp"
	Mock                          = "mock"
	CFS                           = "cfs"
	IPDomainOrIPDomainPortPattern = "^([a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+)" +
		"(:([1-9]|[1-9]\\d{1,3}|[1-5]\\d{4}|6[0-4]\\d{3}|65[0-4]\\d{2}|655[0-2]\\d|6553[0-5]))?$"

	TypeSplit          = 0
	HDFSSplit          = 3
	S3Split            = 3
	S3EndpointSplit    = 2
	S3SplitLen         = 4
	LocalSplit         = 2
	CFSSplit           = 3
	ServerAddressSplit = 2
	S3AddressLength    = 3
	IDSliceLen         = 3

	FsPrefix = "fs-"
	UserRoot = "root"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func IsRootUser(userName string) bool {
	return strings.EqualFold(userName, "root")
}

func IsValidQueueStatus(status string) bool {
	if strings.EqualFold(status, schema.StatusQueueCreating) || strings.EqualFold(status, schema.StatusQueueOpen) ||
		strings.EqualFold(status, schema.StatusQueueClosing) || strings.EqualFold(status, schema.StatusQueueClosed) ||
		strings.EqualFold(status, schema.StatusQueueUnavailable) {
		return true
	}
	log.Errorf("Not valid queue status. status:%s", status)
	return false
}

// check string is slice or not
func StringInSlice(s string, strSlice []string) bool {
	for _, str := range strSlice {
		if s == str {
			return true
		}
	}
	return false
}

func RemoveDuplicateStr(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}
	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

func SplitString(str, sep string) []string {
	var result []string
	strList := strings.Split(str, sep)

	for _, s := range strList {
		result = append(result, strings.TrimSpace(s))
	}
	return result
}

func GetRandID(randNum int) string {
	b := make([]byte, randNum/2)
	rand.Read(b)
	return hex.EncodeToString(b)
}

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
		properties[common.NameNodeAddress] = serverAddress
		subPath = "/" + SubPathFromUrl(urlSplit, HDFSSplit)
		if properties[common.KeyTabData] != "" {
			fileSystemType = HDFSWithKerberos
		}
	case SFTP:
		serverAddress = urlSplit[ServerAddressSplit]
		subPath = "/" + SubPathFromUrl(urlSplit, HDFSSplit)
	case S3:
		serverAddress = properties[common.Endpoint]
		subPath = "/" + SubPathFromUrl(urlSplit, S3Split)
	case CFS:
		serverAddress = urlSplit[ServerAddressSplit]
		subPath = "/" + SubPathFromUrl(urlSplit, CFSSplit)
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
	if properties[common.Principal] == "" {
		return InvalidField(common.Principal, fmt.Sprintf("kerberos hdfs, %s must be provided", common.Principal))
	}
	if properties[common.KeyTabData] == "" {
		return InvalidField(common.KeyTabData, fmt.Sprintf("kerberos hdfs, %s must be provided", common.KeyTabData))
	}
	if properties[common.DataTransferProtection] == "" {
		return InvalidField(common.DataTransferProtection, fmt.Sprintf("kerberos hdfs, %s must be provided", common.DataTransferProtection))
	}

	_, err := base64.StdEncoding.DecodeString(properties[common.KeyTabData])
	if err != nil {
		log.Errorf("Decode keyTab data failed, %v", err)
		return err
	}

	if KDCStr, ok := properties[common.Kdc]; ok {
		for _, kdc := range strings.Split(KDCStr, ",") {
			if matched, err := regexp.MatchString(IPDomainOrIPDomainPortPattern, kdc); err != nil || !matched {
				return InvalidField(common.Kdc,
					"kerberos.kdc should be ip or ip:port format")
			}
		}
	} else {
		return InvalidField(common.Kdc,
			"for kerberos hdfs, kerberos.Kdc must be provided")
	}

	nameNodePrincipal, ok := properties[common.NameNodePrincipal]
	if !ok && nameNodePrincipal == "" {
		return InvalidField(common.NameNodePrincipal,
			"for kerberos hdfs, kerberos.namenode.principal must be provided")
	}

	if realm, ok := properties[common.Realm]; ok {
		if strings.Contains(nameNodePrincipal, "@") &&
			!strings.HasSuffix(nameNodePrincipal, "@"+realm) {
			return InvalidField(common.Realm, fmt.Sprintf("nameNodePrincipal has not suffix realm[%s]", realm))
		}
	} else {
		return InvalidField(common.Realm, fmt.Sprintf("kerberos hdfs, %s must be provided", common.Realm))
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

func FSIDToName(fsID string) string {
	fsArr := strings.Split(fsID, "-")
	return fsArr[len(fsArr)-1]
}

// IsDNS1123Label tests for a string that conforms to the definition of a label in
// DNS (RFC 1123).
func IsDNS1123Label(value string) []string {
	var errs []string
	var dns1123LabelRegexp = regexp.MustCompile("^" + DNS1123LabelFmt + "$")
	if len(value) > DNS1123LabelMaxLength {
		errs = append(errs, fmt.Sprintf("must be no more than %d characters", DNS1123LabelMaxLength))
	}
	if !dns1123LabelRegexp.MatchString(value) {
		errs = append(errs, dns1123LabelErrMsg+" (regex used for validation is '"+DNS1123LabelFmt+"')")
	}
	return errs
}
