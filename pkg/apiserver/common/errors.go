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
	"errors"
	"fmt"
	"net/http"
)

const (
	AccessDenied         = "AccessDenied"         // 无权限访问对应的资源
	ActionNotAllowed     = "ActionNotAllowed"     // 此操作不被允许
	InappropriateJSON    = "InappropriateJSON"    // 请求中的JSON格式正确，但语义上不符合要求。如缺少某个必需项，或者值类型不匹配等。出于兼容性考虑，对于所有无法识别的项应直接忽略，不应该返回这个错误。
	InternalError        = "InternalError"        // 所有未定义的其他错误
	InvalidHTTPRequest   = "InvalidHTTPRequest"   // HTTP body格式错误。例如不符合指定的Encoding等
	InvalidURI           = "InvalidURI"           // URI形式不正确。例如一些服务定义的关键词不匹配等。对于ID不匹配等问题，应定义更加具体的错误码，例如NoSuchKey。
	MalformedJSON        = "MalformedJSON"        // JSON格式不合法
	MalformedYaml        = "MalformedYaml"        // Yaml格式不合法
	InvlidPipeline       = "InvalidPipeline"      // Pipeline校验不通过
	InvalidVersion       = "InvalidVersion"       // URI的版本号不合法
	FileTypeNotSupported = "FileTypeNotSupported" // 文件类型不支持
	InvalidNamePattern   = "InvalidNamePattern"   // 命名格式不规范
	RequestExpired       = "RequestExpired"       // 请求超时
	OnlyRootAllowed      = "OnlyRootAllowed"      // 仅限管理员操作
	InvalidMarker        = "InvalidMarker"        // 列表操作marker解析失败
	InvalidScaleResource = "InvalidScaleResource" // 扩展资源类型不支持
	IOOperationFailure   = "IOOperationFailure"   // I/O操作失败
	NamespaceNotFound    = "NamespaceNotFound"
	CPUNotFound          = "CPUNotFound"
	MemoryNotFound       = "MemoryNotFound"
	PathNotFound         = "PathNotFound"
	MethodNotAllowed     = "MethodNotAllowed"
	DuplicatedName       = "DuplicatedName"
	DuplicatedContent    = "DuplicatedContent"
	InvalidArguments     = "InvalidArguments"
	RecordNotFound       = "RecordNotFound"
	RequiredFieldEmpty   = "RequiredFieldEmpty"

	AuthWithoutToken = "AuthWithoutToken" // 请求没有携带token
	AuthInvalidToken = "AuthInvalidToken" // 无效token
	AuthFailed       = "AuthFailed"       // 用户名或者密码错误
	AuthIllegalUser  = "AuthIllegalUser"  // 非法用户

	DBUpdateFailed = "UpdateDatabaseFailed"

	UserNameDuplicated = "UserNameDuplicated"
	UserNotExist       = "UserNotExist"
	UserPasswordWeak   = "UserPasswordWeak"

	InvalidComputeResource = "InvalidComputeResource"

	QueueNameDuplicated          = "QueueNameDuplicated"
	QueueActionIsNotSupported    = "QueueActionIsNotSupported"
	QueueNameNotFound            = "QueueNameNotFound"
	QueueResourceNotMatch        = "QueueResourceNotMatch"
	QueueQuotaTypeIsNotSupported = "QueueQuotaTypeIsNotSupported"
	QueueIsNotClosed             = "QueueIsNotClosed"
	QueueIsInUse                 = "QueueIsInUse"
	QueueInvalidField            = "QueueInvalidField"
	QueueUpdateFailed            = "QueueUpdateFailed"

	GrantResourceTypeNotFound = "GrantResourceTypeNotFound"
	GrantNotFound             = "GrantNotFound"
	GrantAlreadyExist         = "GrantAlreadyExist"
	GrantRootActionNotSupport = "GrantRootActionNotSupport"

	RunNameDuplicated     = "RunNameDuplicated"
	RunNotFound           = "RunNotFound"
	PipelineNotFound      = "PipelineNotFound"
	RunCacheNotFound      = "RunCacheNotFound"
	ArtifactEventNotFound = "ArtifactEventNotFound"

	FlavourNotFound     = "FlavourNotFound"     // 未找到对应的资源套餐
	FlavourNameEmpty    = "FlavourNameEmpty"    // 资源套餐名称为空
	FlavourInvalidField = "FlavourInvalidField" // 资源套餐名称为空

	JobInvalidField = "JobInvalidField" // job field invalid
	JobCreateFailed = "JobCreateFailed" // job create failed
	JobNotFound     = "JobNotFound"

	ClusterNameNotFound      = "ClusterNameNotFound"
	ClusterIdNotFound        = "ClusterIdNotFound"
	ClusterNotFound          = "ClusterNotFound"
	InvalidClusterProperties = "InvalidClusterProperties"
	InvalidCredential        = "InvalidClusterCredential"
	InvalidClusterStatus     = "InvalidClusterStatus"

	InvalidFileSystemURL        = "InvalidFileSystemParamsURL"
	InvalidFileSystemProperties = "InvalidFileSystemParamsProperties"
	InvalidFileSystemMaxKeys    = "InvalidFileSystemMaxKeys"
	InvalidFileSystemFsName     = "InvalidFileSystemFsName"
	InvalidLinkURL              = "InvalidLinkURL"
	InvalidLinkProperties       = "InvalidLinkProperties"
	InvalidLinkMaxKeys          = "InvalidFileSystemMaxKeys"
	FileSystemDataBaseError     = "FileSystemDataBaseError"
	LinkModelError              = "LinkModelError"
	LinkPathExist               = "LinkPathExist"
	FileSystemClientBusy        = "FileSystemClientBusy"
	K8sOperatorError            = "K8sOperatorError"
	InvalidState                = "InvalidState"
	GrantUserNameAndFs          = "GrantUserNameAndFs"
	FileSystemNotExist          = "FileSystemNotExist"
	FileSystemNameFormatError   = "FileSystemNameFormatError"
	LinkFileSystemNotExist      = "LinkFileSystemNotExist"
	FuseClientError             = "FuseClientError"
	LinkFileSystemPathNotExist  = "LinkFileSystemPathNotExist"
	LinkNotExist                = "LinkNotExist"
	LinkPathMustBeEmpty         = "LinkPathMustBeEmpty"
	ConnectivityFailed          = "ConnectivityFailed"
	InvalidPVClaimsParams       = "InvalidPVClaimsParams"
	GetNamespaceFail            = "GetNamespaceFail"
	LinkMetaPersistError        = "LinkMetaPersistError"
)

var errorHTTPStatus = map[string]int{
	AccessDenied:         http.StatusForbidden,
	ActionNotAllowed:     http.StatusForbidden,
	InappropriateJSON:    http.StatusBadRequest,
	InternalError:        http.StatusInternalServerError,
	InvalidHTTPRequest:   http.StatusBadRequest,
	InvalidURI:           http.StatusBadRequest,
	MalformedJSON:        http.StatusBadRequest,
	MalformedYaml:        http.StatusBadRequest,
	FileTypeNotSupported: http.StatusBadRequest,
	InvalidVersion:       http.StatusNotFound,
	InvalidNamePattern:   http.StatusBadRequest,
	RequestExpired:       http.StatusBadRequest,
	OnlyRootAllowed:      http.StatusForbidden,
	InvalidMarker:        http.StatusBadRequest,
	InvalidScaleResource: http.StatusBadRequest,
	IOOperationFailure:   http.StatusInternalServerError,
	NamespaceNotFound:    http.StatusBadRequest,
	CPUNotFound:          http.StatusBadRequest,
	MemoryNotFound:       http.StatusBadRequest,
	PathNotFound:         http.StatusNotFound,
	MethodNotAllowed:     http.StatusMethodNotAllowed,
	DuplicatedName:       http.StatusBadRequest,
	DuplicatedContent:    http.StatusBadRequest,
	InvalidArguments:     http.StatusBadRequest,
	RecordNotFound:       http.StatusNotFound,
	RequiredFieldEmpty:   http.StatusBadRequest,

	UserNameDuplicated: http.StatusForbidden,
	UserNotExist:       http.StatusBadRequest,
	UserPasswordWeak:   http.StatusBadRequest,

	AuthWithoutToken: http.StatusBadRequest,
	AuthInvalidToken: http.StatusBadRequest,
	AuthFailed:       http.StatusBadRequest,
	AuthIllegalUser:  http.StatusBadRequest,

	QueueNameDuplicated:          http.StatusForbidden,
	QueueActionIsNotSupported:    http.StatusBadRequest,
	QueueQuotaTypeIsNotSupported: http.StatusBadRequest,
	QueueNameNotFound:            http.StatusBadRequest,
	QueueResourceNotMatch:        http.StatusBadRequest,
	QueueIsNotClosed:             http.StatusBadRequest,
	QueueIsInUse:                 http.StatusBadRequest,
	QueueInvalidField:            http.StatusBadRequest,
	QueueUpdateFailed:            http.StatusBadRequest,

	RunNameDuplicated:     http.StatusBadRequest,
	RunNotFound:           http.StatusNotFound,
	PipelineNotFound:      http.StatusBadRequest,
	RunCacheNotFound:      http.StatusBadRequest,
	ArtifactEventNotFound: http.StatusBadRequest,

	GrantResourceTypeNotFound: http.StatusBadRequest,
	GrantNotFound:             http.StatusBadRequest,
	GrantAlreadyExist:         http.StatusBadRequest,
	GrantRootActionNotSupport: http.StatusBadRequest,

	FlavourNotFound:     http.StatusNotFound,
	FlavourNameEmpty:    http.StatusBadRequest,
	FlavourInvalidField: http.StatusBadRequest,

	JobInvalidField: http.StatusBadRequest,
	JobCreateFailed: http.StatusBadRequest,
	JobNotFound:     http.StatusNotFound,

	ClusterNameNotFound:      http.StatusBadRequest,
	ClusterIdNotFound:        http.StatusBadRequest,
	ClusterNotFound:          http.StatusBadRequest,
	InvalidClusterProperties: http.StatusBadRequest,
	InvalidCredential:        http.StatusBadRequest,
	InvalidClusterStatus:     http.StatusBadRequest,

	InvalidFileSystemURL:        http.StatusBadRequest,
	InvalidFileSystemProperties: http.StatusBadRequest,
	FileSystemDataBaseError:     http.StatusInternalServerError,
	FileSystemClientBusy:        http.StatusForbidden,
	FileSystemNotExist:          http.StatusForbidden,
	K8sOperatorError:            http.StatusInternalServerError,
	InvalidState:                http.StatusBadRequest,
	InvalidFileSystemMaxKeys:    http.StatusBadRequest,
	InvalidFileSystemFsName:     http.StatusBadRequest,
	InvalidLinkURL:              http.StatusBadRequest,
	InvalidLinkProperties:       http.StatusBadRequest,
	GrantUserNameAndFs:          http.StatusInternalServerError,
	LinkModelError:              http.StatusInternalServerError,
	FileSystemNameFormatError:   http.StatusBadRequest,
	LinkPathExist:               http.StatusBadRequest,
	LinkFileSystemNotExist:      http.StatusBadRequest,
	FuseClientError:             http.StatusInternalServerError,
	LinkFileSystemPathNotExist:  http.StatusBadRequest,
	LinkNotExist:                http.StatusBadRequest,
	LinkPathMustBeEmpty:         http.StatusBadRequest,
	ConnectivityFailed:          http.StatusBadRequest,
	InvalidPVClaimsParams:       http.StatusBadRequest,
	GetNamespaceFail:            http.StatusInternalServerError,
	LinkMetaPersistError:        http.StatusBadRequest,
}

var errorMessage = map[string]string{
	AccessDenied:         "Access denied",
	ActionNotAllowed:     "The operation cannot be executed at this phase/stage",
	InappropriateJSON:    "The JSON provided was well-formed and valid, but not appropriate for this operation.",
	InternalError:        "We encountered an internal error. Please try again.",
	InvalidHTTPRequest:   "One or more errors in HTTP request body",
	InvalidURI:           "Could not parse the specified URI.",
	MalformedJSON:        "The JSON provided was not well-formatted",
	MalformedYaml:        "The yaml provided was not well-formatted",
	FileTypeNotSupported: "File type not supported",
	InvalidVersion:       "The API version specified was invalid",
	InvalidNamePattern:   "Name pattern does not match regex rule",
	RequestExpired:       "Request has been expired",
	OnlyRootAllowed:      "This operation is only allowed for root user",
	InvalidMarker:        "The marker is invalid",
	InvalidScaleResource: "The scale resource is invalid",
	IOOperationFailure:   "I/O operation failed",
	NamespaceNotFound:    "Namespace is not set",
	CPUNotFound:          "CPU is not set",
	MemoryNotFound:       "Memory is not set",
	DuplicatedName:       "Name has existed. Duplicated name is not allowed",
	DuplicatedContent:    "content(md5) has existed. Please use existing one",
	InvalidArguments:     "invalid arguments",
	RecordNotFound:       "record not found",
	RequiredFieldEmpty:   "Field is not set",

	UserNameDuplicated: "The user name already exists",
	UserNotExist:       "User not exist",
	UserPasswordWeak:   "Password must consist of at least one number and one letter, and length must be greater than 6",

	AuthWithoutToken: "Request should login first",
	AuthInvalidToken: "Invalid token. Please re-login",
	AuthFailed:       "Username or password not correct",
	AuthIllegalUser:  "The user does not have permission to operate other users",

	QueueNameDuplicated:          "The queue name already exists",
	QueueActionIsNotSupported:    "Queue action not supported",
	QueueQuotaTypeIsNotSupported: "Queue quota type not supported",
	QueueNameNotFound:            "QueueName does not exist",
	QueueResourceNotMatch:        "Queue resource is not match",
	QueueIsNotClosed:             "Queue should be closed before delete",

	FlavourNameEmpty: "flavour name should not be empty",

	JobInvalidField: "job field invalid",
	JobCreateFailed: "job create failed",

	RunNameDuplicated:     "Run name already exists",
	RunNotFound:           "RunID not found",
	PipelineNotFound:      "Pipeline not found",
	RunCacheNotFound:      "RunCache not found",
	ArtifactEventNotFound: "ArtifactEvent not found",

	GrantResourceTypeNotFound: "This kind of resource is not exist",
	GrantNotFound:             "Grant not found. check the user and resource",
	GrantAlreadyExist:         "This user already have the grant of the resource",
	GrantRootActionNotSupport: "Can not delete or create root's grant",

	ClusterNameNotFound:      "ClusterName does not exist",
	ClusterIdNotFound:        "ClusterId does not exist",
	ClusterNotFound:          "Cluster not found",
	InvalidClusterProperties: "Cluster properties wrong",
	InvalidCredential:        "Cluster credential wrong",
	InvalidClusterStatus:     "Cluster not in online status, operator not permit",

	InvalidFileSystemURL:        "File system url wrong.",
	InvalidFileSystemProperties: "File system properties wrong.",
	InvalidFileSystemFsName:     "File system fsName wrong.",
	InvalidLinkURL:              "Link url wrong.",
	InvalidLinkProperties:       "Link properties wrong.",
	InvalidFileSystemMaxKeys:    "MaxKeys wrong",
	FileSystemDataBaseError:     "FileSystem DB is wrong.",
	LinkModelError:              "Link db is wrong.",
	FileSystemClientBusy:        "File system is busy",
	K8sOperatorError:            "K8s operator err",

	GrantUserNameAndFs:         "Grant fs and userName err",
	InvalidState:               "Heart state must active or inactive",
	FileSystemNotExist:         "File system not exist",
	FileSystemNameFormatError:  "File system name must be letters and numbers and name length limit 8",
	LinkPathExist:              "Link path has exist",
	LinkFileSystemNotExist:     "Link file system not exist",
	FuseClientError:            "Fuse client Error",
	LinkFileSystemPathNotExist: "Link File system path not exist",
	LinkNotExist:               "Link is not exist",
	LinkPathMustBeEmpty:        "Link path must be empty",
	ConnectivityFailed:         "Connectivity failed",
	InvalidPVClaimsParams:      "Invalid persistent volume claims params",
	GetNamespaceFail:           "Get namespace fail",
}

type ErrorResponse struct {
	RequestID    string `json:"requestID"`
	ErrorCode    string `json:"code"`
	ErrorMessage string `json:"message"`
}

func GetMessageByCode(code string) string {
	return errorMessage[code]
}

func GetHttpStatusByCode(code string) int {
	return errorHTTPStatus[code]
}

func NoAccessError(user, resourceType, resourceID string) error {
	return fmt.Errorf("user[%s] has no access to resource[%s] with Name[%s]", user, resourceType, resourceID)
}

func NotFoundError(resourceType, ID string) error {
	return fmt.Errorf("resouceType[%s] with Name[%s] not found", resourceType, ID)
}

func InvalidMaxKeysError(maxKeys string) error {
	return fmt.Errorf("maxKeys[%s] is invalid", maxKeys)
}

func DuplicatedNameError(resourceType, name, fsID string) error {
	return fmt.Errorf("resouceType[%s]'s name[%s] in fs[%s] already exists", resourceType, name, fsID)
}

func DuplicatedContentError(resourceType, md5, fsID string) error {
	return fmt.Errorf("resouceType[%s]'s md5[%s] in fs[%s] already exists", resourceType, md5, fsID)
}

func InvalidNamePatternError(name, resourceType, reg string) error {
	return fmt.Errorf("name[%s] for [%s] does not compile with regex rule[%s]", name, resourceType, reg)
}

func FileTypeNotSupportedError(fileType, resourceType string) error {
	return fmt.Errorf("fileType[%s] for [%s] is not supported", fileType, resourceType)
}

func New(text string) error {
	return errors.New(text)
}

func InvalidField(field string, info string) error {
	return fmt.Errorf("Field[%s] is invalid, %s", field, info)
}

func SubPathError(subPath string) error {
	return fmt.Errorf("Can not use subpath[%s], subpath used or conflict", subPath)
}

func LinkPathError(fsPath string) error {
	return fmt.Errorf("Can not use fsPath[%s], fsPath used or conflict by other link path", fsPath)
}

func DbDataNotExitError(errMsg string) error {
	return errors.New(errMsg)
}

func PVCNotFountError(pvc, namespace string) error {
	return fmt.Errorf("The pvc[%s] in the namespace[%s] does not exist", pvc, namespace)
}

func LogPageSizeOverMaxError() error {
	return fmt.Errorf("LogPageSize over max value")
}

func LogFilePositionInvalidValueError() error {
	return fmt.Errorf("LogFilePosition has wrong value")
}

func ConnectionClosedError() error {
	return fmt.Errorf("Connection closed")
}

func FsBeingUsedError(fsID string) error {
	return fmt.Errorf("fs[%s] is being used and cannot be deleted/modified", fsID)
}

func InvalidStatisticsParams(param string) error {
	return fmt.Errorf("Invalid params, %s should to be greater than 0", param)
}

func InvalidStartEndParams() error {
	return fmt.Errorf("Invalid params, start should not to be greater than end")
}
