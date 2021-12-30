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
	MalformedJSON               = "MalformedJSON" // JSON格式不合法
	InternalError               = "InternalError" // 所有未定义的其他错误
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
	AuthWithoutToken            = "AuthWithoutToken" // 请求没有携带token
	AuthInvalidToken            = "AuthInvalidToken" // 无效token
	AuthFailed                  = "AuthFailed"       // 用户名或者密码错误
	InvalidState                = "InvalidState"
	GrantUserNameAndFsID        = "GrantUserNameAndFsID"
	FileSystemNotExist          = "FileSystemNotExist"
	FileSystemNameFormatError   = "FileSystemNameFormatError"
	LinkFileSystemNotExist      = "LinkFileSystemNotExist"
	FuseClientError             = "FuseClientError"
	LinkFileSystemPathNotExist  = "LinkFileSystemPathNotExist"
	LinkNotExist                = "LinkNotExist"
	LinkPathMustBeEmpty         = "LinkPathMustBeEmpty"
	ConnectivityFailed          = "ConnectivityFailed"
	InvalidPVClaimsParams       = "InvalidPVClaimsParams"
	NamespaceNotFound           = "NamespaceNotFound"
	GetNamespaceFail            = "GetNamespaceFail"
	LinkMetaPersistError        = "LinkMetaPersistError"
)

var errorHTTPStatus = map[string]int{
	InternalError: http.StatusInternalServerError,
	MalformedJSON: http.StatusBadRequest,

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
	GrantUserNameAndFsID:        http.StatusInternalServerError,
	LinkModelError:              http.StatusInternalServerError,
	AuthWithoutToken:            http.StatusBadRequest,
	AuthInvalidToken:            http.StatusBadRequest,
	AuthFailed:                  http.StatusBadRequest,
	FileSystemNameFormatError:   http.StatusBadRequest,
	LinkPathExist:               http.StatusBadRequest,
	LinkFileSystemNotExist:      http.StatusBadRequest,
	FuseClientError:             http.StatusInternalServerError,
	LinkFileSystemPathNotExist:  http.StatusBadRequest,
	LinkNotExist:                http.StatusBadRequest,
	LinkPathMustBeEmpty:         http.StatusBadRequest,
	ConnectivityFailed:          http.StatusBadRequest,
	InvalidPVClaimsParams:       http.StatusBadRequest,
	NamespaceNotFound:           http.StatusBadRequest,
	GetNamespaceFail:            http.StatusInternalServerError,
	LinkMetaPersistError:        http.StatusBadRequest,
}

var errorMessage = map[string]string{
	InternalError: "We encountered an internal error. Please try again.",
	MalformedJSON: "The JSON provided was not well-formatted",

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

	GrantUserNameAndFsID:       "Grant fsID and userName err",
	AuthWithoutToken:           "Request should login first",
	AuthInvalidToken:           "Invalid token. Please relogin",
	AuthFailed:                 "UserName or password not correct",
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
	NamespaceNotFound:          "Namespace not found",
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
