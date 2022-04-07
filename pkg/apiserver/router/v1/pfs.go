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

package v1

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-chi/chi"
	log "github.com/sirupsen/logrus"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"paddleflow/pkg/apiserver/common"
	api "paddleflow/pkg/apiserver/controller/fs"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/database"
	"paddleflow/pkg/common/logger"
	fuse "paddleflow/pkg/fs/client/fs"
	fsCommon "paddleflow/pkg/fs/common"
	"paddleflow/pkg/fs/utils/k8s"
)

type PFSRouter struct{}

func (pr *PFSRouter) Name() string {
	return "PFSRouter"
}

func (pr *PFSRouter) AddRouter(r chi.Router) {
	log.Info("add PFS router")
	r.Post("/fs", pr.CreateFileSystem)
	r.Get("/fs", pr.ListFileSystem)
	r.Get("/fs/{fsName}", pr.GetFileSystem)
	r.Delete("/fs/{fsName}", pr.DeleteFileSystem)
	r.Post("/fs/claims", pr.CreateFileSystemClaims)
}

var URLPrefix = map[string]bool{
	common.HDFS:  true,
	common.Local: true,
	common.S3:    true,
	common.SFTP:  true,
	common.Mock:  true,
	common.CFS:   true,
}

const (
	userGroupField = "userGroup"
	userNameField  = "userName"
	Password       = "password"
	DefaultMaxKeys = 50
	MaxAllowKeys   = 1000
	FsNameMaxLen   = 8
	retryNum       = 3
)

// CreateFileSystem the function that handle the create file system request
// @Summary CreateFileSystem
// @Description 创建文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body request.CreateFileSystemRequest true "request body"
// @Success 200 {object} response.CreateFileSystemResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /api/paddleflow/v1/fs [post]
func (pr *PFSRouter) CreateFileSystem(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var createRequest api.CreateFileSystemRequest
	err := common.BindJSON(r, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("CreateFileSystem bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	ctx.Logging().Debugf("create file system with req[%v]", createRequest)

	fileSystemService := api.GetFileSystemService()

	if createRequest.Username == "" {
		createRequest.Username = ctx.UserName
	}
	err = validateCreateFileSystem(&ctx, &createRequest)

	if err != nil {
		ctx.Logging().Errorf("create file system params error: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	_, err = fileSystemService.CreateFileSystem(&ctx, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("create file system with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	response := api.CreateFileSystemResponse{FsName: createRequest.Name, FsID: common.ID(createRequest.Username, createRequest.Name)}

	ctx.Logging().Debugf("CreateFileSystem Fs:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateCreateFileSystem(ctx *logger.RequestContext, req *api.CreateFileSystemRequest) error {
	if req.Username == "" {
		ctx.Logging().Error("userName is empty")
		ctx.ErrorCode = common.AuthFailed
		return fmt.Errorf("userName is empty")
	}
	matchBool, err := regexp.MatchString(fmt.Sprintf("^[a-zA-Z0-9]{1,%d}$", FsNameMaxLen), req.Name)
	if err != nil {
		ctx.Logging().Errorf("regexp err[%v]", err)
		ctx.ErrorCode = common.FileSystemNameFormatError
		return err
	}
	if !matchBool {
		ctx.Logging().Errorf("regexp match failed with fsName[%s]", req.Name)
		ctx.ErrorCode = common.FileSystemNameFormatError
		return common.InvalidField("name", fmt.Sprintf("fsName[%s] must be letters or numbers and fsName maximum length is %d", req.Name, FsNameMaxLen))
	}

	urlArr := strings.Split(req.Url, ":")
	if len(urlArr) < 2 {
		ctx.Logging().Errorf("[%s] is not a correct file-system url", req.Url)
		ctx.ErrorCode = common.InvalidFileSystemURL
		return common.InvalidField("url", "is not a correct file-system url")
	}

	fileSystemType := urlArr[0]
	if !URLPrefix[fileSystemType] {
		ctx.Logging().Errorf("url[%s] can not support [%s] file system", req.Url, fileSystemType)
		ctx.ErrorCode = common.InvalidFileSystemURL
		return common.InvalidField("url", fmt.Sprintf("can not support [%s] file system", fileSystemType))
	}
	err = checkURLFormat(fileSystemType, req.Url, req.Properties)
	if err != nil {
		ctx.Logging().Errorf("check url format err[%v] with url[%s]", err, req.Url)
		ctx.ErrorCode = common.InvalidFileSystemURL
		return err
	}
	err = checkProperties(fileSystemType, req)
	if err != nil {
		ctx.Logging().Errorf("check properties err[%v] with properties[%v]", err, req.Properties)
		ctx.ErrorCode = common.InvalidFileSystemProperties
		return err
	}
	err = checkFsDir(fileSystemType, req.Url, req.Properties)
	if err != nil {
		ctx.Logging().Errorf("check fs dir err[%v] with url[%s]", err, req.Url)
		ctx.ErrorCode = common.InvalidFileSystemURL
		return err
	}
	if fileSystemType == common.Mock {
		return nil
	}
	fsType, serverAddress, subPath := common.InformationFromURL(req.Url, req.Properties)
	fsMeta := fsCommon.FSMeta{
		ID:            common.ID(req.Username, req.Name),
		Name:          req.Name,
		UfsType:       fsType,
		ServerAddress: serverAddress,
		SubPath:       subPath,
		Properties:    req.Properties,
		Type:          fsCommon.FSType,
	}
	err = checkStorageConnectivity(fsMeta)
	if err != nil {
		ctx.Logging().Errorf("check fs[%s] connectivity failed with req[%v] and err[%v]", req.Name, req, err)
		ctx.ErrorCode = common.ConnectivityFailed
		return err
	}
	return nil
}

func checkStorageConnectivity(fsMeta fsCommon.FSMeta) error {
	_, err := fuse.NewFileSystem(fsMeta, nil, true, false, config.GlobalServerConfig.Fs.LinkMetaDirPrefix, nil)
	if err != nil {
		log.Errorf("new a fileSystem with fsMeta [%+v] failed: %v", fsMeta, err)
		return err
	}
	return nil
}

func checkProperties(fsType string, req *api.CreateFileSystemRequest) error {
	switch fsType {
	case common.HDFS:
		if req.Properties[fsCommon.KeyTabData] != "" {
			err := common.CheckKerberosProperties(req.Properties)
			if err != nil {
				log.Errorf("check kerberos properties err[%v]", err)
				return err
			}
		} else if req.Properties[fsCommon.UserKey] != "" {
			if req.Properties[fsCommon.UserKey] == "" {
				return common.InvalidField("properties", "key[user] cannot be empty")
			}
			if req.Properties[fsCommon.Group] == "" {
				return common.InvalidField("properties", "key[group] cannot be empty")
			}
		} else {
			return common.InvalidField("properties", "not correct hdfs properties")
		}
		return nil
	case common.Local:
		if req.Properties["debug"] != "true" {
			return common.InvalidField("debug", "properties key[debug] must true")
		}
		return nil
	case common.S3:
		if req.Properties[fsCommon.AccessKey] == "" || req.Properties[fsCommon.SecretKey] == "" {
			log.Error("s3 ak or sk is empty")
			return common.InvalidField("properties", fmt.Sprintf("key %s or %s is empty", fsCommon.AccessKey, fsCommon.SecretKey))
		}
		if req.Properties[fsCommon.Endpoint] == "" {
			log.Error("endpoint is empty")
			return common.InvalidField("properties", "key[endpoint] is empty")
		}
		if req.Properties[fsCommon.Bucket] == "" {
			log.Error("bucket is empty")
			return common.InvalidField("properties", "url bucket is empty")
		}
		if req.Properties[fsCommon.Region] == "" {
			req.Properties[fsCommon.Region] = ""
		}
		encodedSk, err := common.AesEncrypt(req.Properties[fsCommon.SecretKey], common.AESEncryptKey)
		if err != nil {
			log.Errorf("encrypt s3 sk failed: %v", err)
			return err
		}
		req.Properties[fsCommon.SecretKey] = encodedSk
		return nil
	case common.SFTP:
		if req.Properties[fsCommon.UserKey] == "" {
			return common.InvalidField(fsCommon.UserKey, "key[user] cannot be empty")
		}
		if req.Properties[fsCommon.Password] == "" {
			return common.InvalidField("properties", "key[password] cannot be empty")
		}
		encodePassword, err := common.AesEncrypt(req.Properties[fsCommon.Password], common.AESEncryptKey)
		if err != nil {
			log.Errorf("encrypt sftp password failed: %v", err)
			return err
		}
		req.Properties[fsCommon.Password] = encodePassword
		return nil
	case common.Mock:
		pvc := req.Properties[fsCommon.PVC]
		if pvc == "" {
			return common.InvalidField(fsCommon.PVC, "key[pvc] cannot be empty")
		}
		namespace := req.Properties[fsCommon.Namespace]
		if namespace == "" {
			return common.InvalidField(fsCommon.Namespace, "key[namespace] cannot be empty")
		}
		if checkPVCExist(pvc, namespace) {
			return nil
		}
		return common.PVCNotFountError(pvc, namespace)
	default:
		return nil
	}
}

func checkPVCExist(pvc, namespace string) bool {
	_, errK8sOperator := k8s.GetK8sOperator().GetPersistentVolumeClaim(namespace, pvc, metav1.GetOptions{})
	if errK8sOperator != nil {
		log.Errorf("check namespace[%s] pvc[%s] exist failed: %v", namespace, pvc, errK8sOperator)
		return false
	}
	return true
}

func checkURLFormat(fsType, url string, properties map[string]string) error {
	urlSplit := strings.Split(url, "/")
	// check fs url correct
	switch fsType {
	case common.HDFS, common.SFTP, common.CFS:
		if len(urlSplit) < 4 {
			log.Errorf("%s url split error", fsType)
			return common.InvalidField("url", fmt.Sprintf("%s url format is wrong", fsType))
		}
	case common.Local, common.Mock:
		if len(urlSplit) < 3 {
			log.Errorf("%s url split error", fsType)
			return common.InvalidField("url", fmt.Sprintf("%s address format is wrong", fsType))
		}
		if urlSplit[2] == "" || urlSplit[2] == "root" {
			log.Errorf("%s path can not be empty or use root path", fsType)
			return common.InvalidField("url", fmt.Sprintf("%s path can not be empty or use root path", fsType))
		}
	case common.S3:
		if len(urlSplit) < common.S3SplitLen {
			log.Errorf("%s url split error", fsType)
			return common.InvalidField("url", fmt.Sprintf("%s url format is wrong", fsType))
		}
		if urlSplit[common.S3EndpointSplit] == "" {
			log.Errorf("%s path must appoint bucket", fsType)
			return common.InvalidField("url", fmt.Sprintf("%s must appoint bucket", fsType))
		}
		if properties == nil {
			log.Error("properties is empty")
			return common.InvalidField("properties", "must not be empty")
		}
		properties[fsCommon.Bucket] = urlSplit[common.S3EndpointSplit]
	}
	return nil
}

// checkFsDir duplicate and nesting of the same storage source directory is not supported
func checkFsDir(fsType, url string, properties map[string]string) error {
	var inputIPs []string
	subPath := ""
	switch fsType {
	case common.Local, common.Mock:
		subPath = strings.SplitAfterN(url, "/", 2)[1]
	case common.HDFS, common.SFTP, common.CFS:
		urlSplit := strings.Split(url, "/")
		urlRaw := urlSplit[2]
		inputIPs = strings.Split(urlRaw, ",")
		subPath = "/" + strings.SplitAfterN(url, "/", 4)[3]
	case common.S3:
		inputIPs = strings.Split(properties[fsCommon.Endpoint], ",")
		subPath = "/" + strings.SplitAfterN(url, "/", 4)[3]
	}
	fsList, err := models.GetSimilarityAddressList(database.DB, fsType, inputIPs)
	if err != nil {
		return err
	}
	for _, data := range fsList {
		if common.CheckFsNested(subPath, data.SubPath) {
			log.Errorf("%s and %s subpath is not allowed up nesting or duplication", subPath, data.SubPath)
			return common.SubPathError(subPath)
		}
	}

	return nil
}

// ListFileSystem the function that handle the list file systems request
// @Summary ListFileSystem
// @Description 批量获取文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body request.ListFileSystemRequest true "request body"
// @Success 200 {object} response.ListFileSystemResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /api/paddleflow/v1/fs [get]
func (pr *PFSRouter) ListFileSystem(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var maxKeys int
	if r.URL.Query().Get(util.QueryKeyMaxKeys) == "" {
		maxKeys = util.DefaultMaxKeys
	} else {
		maxKeys, _ = strconv.Atoi(r.URL.Query().Get(util.QueryKeyMaxKeys))
	}
	listRequest := &api.ListFileSystemRequest{
		FsName:   r.URL.Query().Get(util.QueryFsName),
		Marker:   r.URL.Query().Get(util.QueryKeyMarker),
		MaxKeys:  int32(maxKeys),
		Username: r.URL.Query().Get(util.QueryKeyUserName),
	}
	log.Debugf("list file system with req[%v]", listRequest)

	fileSystemService := api.GetFileSystemService()
	isRoot := false
	if listRequest.Username == "" {
		listRequest.Username = ctx.UserName
		if listRequest.Username == common.UserRoot {
			isRoot = true
		}
	}
	if listRequest.Username == "" {
		ctx.Logging().Error("userName is empty")
		common.RenderErrWithMessage(w, ctx.RequestID, common.AuthFailed, "userName is empty")
		return
	}

	if listRequest.MaxKeys == 0 {
		listRequest.MaxKeys = DefaultMaxKeys
	}
	if listRequest.MaxKeys > MaxAllowKeys {
		ctx.Logging().Error("too many max keys")
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidFileSystemMaxKeys, fmt.Sprintf("maxKeys limit %d", MaxAllowKeys))
		return
	}

	listFileSystems, nextMarker, err := fileSystemService.ListFileSystem(&ctx, listRequest, isRoot)
	if err != nil {
		ctx.Logging().Errorf("list file system with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	response := *getListResult(listFileSystems, nextMarker, listRequest.Marker)
	ctx.Logging().Debugf("ListFileSystem Fs:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func getListResult(fsModel []models.FileSystem, nextMarker, marker string) *api.ListFileSystemResponse {
	var FsLists []*api.FileSystemResponse
	for _, FSData := range fsModel {
		FsList := fsResponseFromModel(FSData)
		FsLists = append(FsLists, FsList)
	}
	ListFsResponse := &api.ListFileSystemResponse{
		Marker:    marker,
		FsList:    FsLists,
		Truncated: false,
	}
	if nextMarker != "" {
		ListFsResponse.Truncated = true
		ListFsResponse.NextMarker = nextMarker
	}
	return ListFsResponse
}

// GetFileSystem the function that handle the get file system request
// @Summary GetFileSystem
// @Description 获取指定文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param id path string true "文件系统ID"
// @Success 200 {object} models.FileSystem
// @Router /api/paddleflow/v1/fs/{fsName} [get]
func (pr *PFSRouter) GetFileSystem(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	fsName := chi.URLParam(r, util.QueryFsName)
	getRequest := api.GetFileSystemRequest{
		Username: r.URL.Query().Get(util.QueryKeyUserName),
	}
	log.Debugf("get file system with req[%v] and fileSystemID[%s]", getRequest, fsName)

	fileSystemService := api.GetFileSystemService()

	if getRequest.Username == "" {
		getRequest.Username = ctx.UserName
	}
	err := validateGetFs(&ctx, &getRequest, &fsName)
	if err != nil {
		ctx.Logging().Errorf("validateGetFs error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	fsModel, err := fileSystemService.GetFileSystem(&getRequest, fsName)
	if err != nil {
		ctx.Logging().Errorf("get file system with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response := *fsResponseFromModel(fsModel)
	ctx.Logging().Debugf("GetFileSystem Fs:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func fsResponseFromModel(fsModel models.FileSystem) *api.FileSystemResponse {
	return &api.FileSystemResponse{
		Id:            fsModel.ID,
		Name:          fsModel.Name,
		ServerAddress: fsModel.ServerAddress,
		Type:          fsModel.Type,
		SubPath:       fsModel.SubPath,
		Username:      fsModel.UserName,
		Properties:    fsModel.PropertiesMap,
	}
}

func validateGetFs(ctx *logger.RequestContext, req *api.GetFileSystemRequest, fsID *string) error {
	if req.Username == "" {
		req.Username = ctx.UserName
	}

	if req.Username == "" {
		ctx.Logging().Error("UserName is empty")
		ctx.ErrorCode = common.AuthFailed
		return common.InvalidField("userName", "userName is empty")
	}
	if *fsID == "" {
		ctx.Logging().Error("FsID or FsName is empty")
		// response to user use fsName not fsID
		return common.InvalidField("fsName", "fsName is empty")
	}
	// trans fsName to real fsID, for user they only use fsName，grpc client may be use fsID
	*fsID = common.NameToFsID(*fsID, req.Username)

	return nil
}

// DeleteFileSystem the function that handle the delete file system request
// @Summary DeleteFileSystem
// @Description 删除指定文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param id path string true "文件系统ID"
// @Success 200
// @Router /api/paddleflow/v1/fs/{fsName} [delete]
func (pr *PFSRouter) DeleteFileSystem(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	fsName := chi.URLParam(r, util.QueryFsName)
	deleteRequest := &api.DeleteFileSystemRequest{
		FsName:   fsName,
		Username: r.URL.Query().Get(util.QueryKeyUserName),
	}
	log.Debugf("delete file system with req[%v] and FileSystemID[%s]", deleteRequest, fsName)

	fileSystemService := api.GetFileSystemService()

	err := validateDeleteFs(&ctx, deleteRequest, &fsName)
	if err != nil {
		ctx.Logging().Errorf("validateDeleteFs error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = fileSystemService.DeleteFileSystem(&ctx, fsName)
	if err != nil {
		ctx.Logging().Errorf("delete file system with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusOK)
}

func validateDeleteFs(ctx *logger.RequestContext, req *api.DeleteFileSystemRequest, fsID *string) error {
	if req.Username == "" {
		req.Username = ctx.UserName
	}

	if req.Username == "" {
		ctx.Logging().Error("UserName is empty")
		ctx.ErrorCode = common.AuthFailed
		return common.InvalidField("userName", "userName is empty")
	}

	if *fsID == "" {
		ctx.Logging().Error("FsID or FsName is empty")
		// response to user use fsName not fsID
		return common.InvalidField("fsName", "fsName is empty")
	}
	req.FsName = *fsID
	// trans fsName to real fsID, for user they only use fsName，grpc client may be use fsID
	fsTemp := strings.Split(*fsID, "-")
	if len(fsTemp) < common.IDSliceLen || fsTemp[0] != "fs" || (fsTemp[1] != req.Username && req.Username != common.UserRoot) {
		*fsID = common.ID(req.Username, *fsID)
	}
	ctx.Logging().Debugf("delete fs id is %s", *fsID)

	fsModel, err := models.GetFileSystemWithFsID(database.DB, *fsID)
	if err != nil {
		ctx.Logging().Errorf("delete failed by getting file system error[%v]", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}

	if fsModel.Name == "" {
		ctx.Logging().Errorf("file system not exit %s", *fsID)
		ctx.ErrorCode = common.FileSystemNotExist
		return common.DbDataNotExitError(fmt.Sprintf("userName[%s] not created file system[%s]", req.Username, req.FsName))
	}

	if req.Username != common.UserRoot && req.Username != fsModel.UserName {
		ctx.ErrorCode = common.AuthFailed
		return fmt.Errorf("user[%s] is not admin user, can not delete fs[%s]", req.Username, req.FsName)
	}
	return nil
}

// CreateFileSystemClaims the function that handle the create file system claims request
// @Summary CreateFileSystemClaims
// @Description
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body request.CreateFileSystemClaimsRequest true "request body"
// @Success 200 {object} response.CreateFileSystemClaimsResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /api/paddleflow/v1/fs/claims [post]
func (pr *PFSRouter) CreateFileSystemClaims(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	var createRequest api.CreateFileSystemClaimsRequest
	err := common.BindJSON(r, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("CreateFileSystemClaims bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	log.Debugf("create file system claims with req[%v]", config.PrettyFormat(createRequest))

	fileSystemService := api.GetFileSystemService()

	err = validateCreateFileSystemClaims(&ctx, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("create file system claims params error: %v", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = fileSystemService.CreateFileSystemClaims(&ctx, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("create file system claims with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	response := api.CreateFileSystemClaimsResponse{Message: common.ClaimsSuccessMessage}
	ctx.Logging().Debugf("CreateFileSystemClaims Fs:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateCreateFileSystemClaims(ctx *logger.RequestContext, req *api.CreateFileSystemClaimsRequest) error {
	if len(req.FsIDs) == 0 {
		ctx.ErrorCode = common.InvalidPVClaimsParams
		return common.InvalidField("fsIDs", "must not be empty")
	}
	if len(req.Namespaces) == 0 {
		ctx.ErrorCode = common.InvalidPVClaimsParams
		return common.InvalidField("namespaces", "must not be empty")
	}
	var notExistNamespaces []string
	for _, ns := range req.Namespaces {
		if _, err := k8s.GetK8sOperator().GetNamespace(ns, metav1.GetOptions{}); err != nil {
			if k8serrors.IsNotFound(err) {
				notExistNamespaces = append(notExistNamespaces, ns)
				continue
			}
			ctx.Logging().Errorf("get namespace[%s] failed: %v", ns, err)
			ctx.ErrorCode = common.GetNamespaceFail
			return err
		}
	}
	if len(notExistNamespaces) != 0 {
		ctx.Logging().Errorf("namespaces[%v] to create pvc is not found", notExistNamespaces)
		ctx.ErrorCode = common.NamespaceNotFound
		return common.InvalidField("namespaces", fmt.Sprintf("namespaces %v not found", notExistNamespaces))
	}
	return nil
}
