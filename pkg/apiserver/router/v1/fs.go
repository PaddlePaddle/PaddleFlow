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
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-chi/chi"
	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"paddleflow/pkg/apiserver/common"
	api "paddleflow/pkg/apiserver/controller/fs"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/apiserver/router/util"
	"paddleflow/pkg/common/config"
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
	// fs
	r.Post("/fs", pr.createFileSystem)
	r.Get("/fs", pr.listFileSystem)
	r.Get("/fs/{fsName}", pr.getFileSystem)
	r.Delete("/fs/{fsName}", pr.deleteFileSystem)
	r.Post("/fs/claims", pr.createFileSystemClaims)
	// fs cache config
	r.Post("/fsCache", pr.createFSCacheConfig)
	r.Put("/fsCache/{fsName}", pr.updateFSCacheConfig)
	r.Get("/fsCache/{fsName}", pr.getFSCacheConfig)
	r.Post("/fsCache/report", pr.fsCacheReport)
	r.Post("/fsMount", pr.createFsMount)
	r.Delete("/fsMount/{fsName}", pr.deleteFsMount)
	r.Get("/fsMount", pr.listFsMount)
}

var URLPrefix = map[string]bool{
	common.HDFS:  true,
	common.Local: true,
	common.S3:    true,
	common.SFTP:  true,
	common.Mock:  true,
	common.CFS:   true,
}

const FsNameMaxLen = 8

// createFileSystem the function that handle the create file system request
// @Summary createFileSystem
// @Description 创建文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body fs.CreateFileSystemRequest true "request body"
// @Success 201 {object} fs.CreateFileSystemResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fs [post]
func (pr *PFSRouter) createFileSystem(w http.ResponseWriter, r *http.Request) {
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

	fs, err := fileSystemService.CreateFileSystem(&ctx, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("create file system with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	response := api.CreateFileSystemResponse{FsName: fs.Name, FsID: fs.ID}

	ctx.Logging().Debugf("CreateFileSystem Fs:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusCreated, response)
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
	fsList, err := models.GetSimilarityAddressList(fsType, inputIPs)
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

// listFileSystem the function that handle the list file systems request
// @Summary listFileSystem
// @Description 批量获取文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body fs.ListFileSystemRequest true "request body"
// @Success 200 {object} fs.ListFileSystemResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fs [get]
func (pr *PFSRouter) listFileSystem(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	listRequest := &api.ListFileSystemRequest{
		FsName:   r.URL.Query().Get(util.QueryFsName),
		Marker:   r.URL.Query().Get(util.QueryKeyMarker),
		MaxKeys:  int32(maxKeys),
		Username: r.URL.Query().Get(util.QueryKeyUserName),
	}
	log.Debugf("list file system with req[%v]", listRequest)

	fileSystemService := api.GetFileSystemService()
	realUserName := getRealUserName(&ctx, listRequest.Username)

	listRequest.Username = realUserName

	listFileSystems, nextMarker, err := fileSystemService.ListFileSystem(&ctx, listRequest)
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

// getFileSystem the function that handle the get file system request
// @Summary getFileSystem
// @Description 获取指定文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param id path string true "文件系统ID"
// @Success 200 {object} models.FileSystem
// @Router /fs/{fsName} [get]
func (pr *PFSRouter) getFileSystem(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	fsName := chi.URLParam(r, util.QueryFsName)
	getRequest := api.GetFileSystemRequest{
		Username: r.URL.Query().Get(util.QueryKeyUserName),
	}
	log.Infof("get file system with req[%v] and fileSystemID[%s]", getRequest, fsName)

	fileSystemService := api.GetFileSystemService()
	realUserName := getRealUserName(&ctx, getRequest.Username)
	fsID := common.ID(realUserName, fsName)
	fsModel, err := fileSystemService.GetFileSystem(fsID)
	if err != nil {
		ctx.Logging().Errorf("get file system username[%s] fsname[%s] with error[%v]", getRequest.Username, fsName, err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RecordNotFound
			ctx.ErrorMessage = fmt.Sprintf("username[%s] not create fsName[%s]", realUserName, fsName)
		} else {
			ctx.ErrorCode = common.FileSystemDataBaseError
			ctx.ErrorMessage = err.Error()
		}
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, ctx.ErrorMessage)
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

// deleteFileSystem the function that handle the delete file system request
// @Summary deleteFileSystem
// @Description 删除指定文件系统
// @tag fs
// @Accept   json
// @Produce  json
// @Param fsName path string true "文件系统名称"
// @Param username query string false "用户名"
// @Success 200
// @Router /fs/{fsName} [delete]
func (pr *PFSRouter) deleteFileSystem(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)

	log.Debugf("delete file system with fsName[%s] username[%s]", fsName, username)

	fileSystemService := api.GetFileSystemService()

	realUserName := getRealUserName(&ctx, username)
	fsID := common.ID(realUserName, fsName)

	_, err := models.GetFileSystemWithFsID(fsID)
	if err != nil {
		ctx.Logging().Errorf("delete fsID[%s] failed by getting file system error[%v]", fsID, err)
		ctx.ErrorMessage = fmt.Sprintf("username[%s] not create fsName[%s]", username, fsName)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.RenderErrWithMessage(w, ctx.RequestID, common.RecordNotFound, ctx.ErrorMessage)
		} else {
			common.RenderErrWithMessage(w, ctx.RequestID, common.FileSystemDataBaseError, err.Error())
		}
		return
	}

	err = fileSystemService.DeleteFileSystem(&ctx, fsID)
	if err != nil {
		ctx.Logging().Errorf("delete file system with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// createFileSystemClaims the function that handle the create file system claims request
// @Summary createFileSystemClaims
// @Description
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body request.CreateFileSystemClaimsRequest true "request body"
// @Success 200 {object} response.CreateFileSystemClaimsResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fs/claims [post]
func (pr *PFSRouter) createFileSystemClaims(w http.ResponseWriter, r *http.Request) {
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

func getRealUserName(ctx *logger.RequestContext,
	username string) string {
	if common.IsRootUser(ctx.UserName) && username != "" {
		return username
	}
	return ctx.UserName
}

func getFsIDAndCheckPermission(ctx *logger.RequestContext,
	username, fsName string) (string, error) {
	// check permission
	var fsID string
	// concatenate fsID
	if common.IsRootUser(ctx.UserName) && username != "" {
		// root user can select fs under other users
		fsID = common.ID(username, fsName)
	} else {
		fsID = common.ID(ctx.UserName, fsName)
	}
	return fsID, nil
}

// createFSCacheConfig handles requests of creating filesystem cache config
// @Summary createFSCacheConfig
// @Description
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body fs.CreateFileSystemCacheRequest true "request body"
// @Success 201 {string} string Created
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsCache [post]
func (pr *PFSRouter) createFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var createRequest api.CreateFileSystemCacheRequest
	err := common.BindJSON(r, &createRequest)
	if err != nil {
		ctx.Logging().Errorf("CreateFSCacheConfig bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	realUserName := getRealUserName(&ctx, createRequest.Username)
	createRequest.FsID = common.ID(realUserName, createRequest.FsName)

	ctx.Logging().Debugf("create file system cache with req[%v]", createRequest)

	err = validateCreateFSCacheConfig(&ctx, &createRequest)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = api.CreateFileSystemCacheConfig(&ctx, createRequest)
	if err != nil {
		ctx.Logging().Errorf("create file system cache with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusCreated)
}

func validateCreateFSCacheConfig(ctx *logger.RequestContext, req *api.CreateFileSystemCacheRequest) error {
	// fs exists?
	_, err := models.GetFileSystemWithFsID(req.FsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.FileSystemNotExist
			ctx.Logging().Errorf("validateCreateFileSystemCache fsID[%s] not exist", req.FsID)
		} else {
			ctx.Logging().Errorf("validateCreateFileSystemCache fsID[%s] err:%v", req.FsID, err)
		}
		return err
	}
	// TODO param check rule

	return nil
}

// getFSCacheConfig
// @Summary 通过FsID获取缓存配置
// @Description  通过FsID获取缓存配置
// @Id getFSCacheConfig
// @tags FSCacheConfig
// @Accept  json
// @Produce json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Success 200 {object} models.FSCacheConfig "缓存配置结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /fsCache/{fsName} [GET]
func (pr *PFSRouter) getFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)
	ctx := common.GetRequestContext(r)

	realUserName := getRealUserName(&ctx, username)
	fsID := common.ID(realUserName, fsName)

	fsCacheConfigResp, err := api.GetFileSystemCacheConfig(&ctx, fsID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			ctx.ErrorCode = common.RecordNotFound
		} else {
			ctx.ErrorCode = common.InternalError
		}
		logger.LoggerForRequest(&ctx).Errorf("GetFSCacheConfig[%s] failed. error:%v", fsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.Render(w, http.StatusOK, fsCacheConfigResp)
}

// updateFSCacheConfig
// @Summary 更新FsID的缓存配置
// @Description  更新FsID的缓存配置
// @Id updateFSCacheConfig
// @tags FSCacheConfig
// @Accept  json
// @Produce json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Param request body fs.UpdateFileSystemCacheRequest true "request body"
// @Success 200 {object} models.FSCacheConfig "缓存配置结构体"
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /fsCache/{fsName} [PUT]
func (pr *PFSRouter) updateFSCacheConfig(w http.ResponseWriter, r *http.Request) {
	fsName := chi.URLParam(r, util.QueryFsName)
	username := r.URL.Query().Get(util.QueryKeyUserName)
	ctx := common.GetRequestContext(r)

	var req api.UpdateFileSystemCacheRequest
	err := common.BindJSON(r, &req)
	if err != nil {
		ctx.Logging().Errorf("UpdateFSCacheConfig[%s] bindjson failed. err:%s", fsName, err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}

	realUserName := getRealUserName(&ctx, username)
	req.FsID = common.ID(realUserName, fsName)

	// validate fs_cache_config existence
	_, err = models.GetFSCacheConfig(ctx.Logging(), req.FsID)
	if err != nil {
		ctx.Logging().Errorf("validateUpdateFileSystemCache err:%v", err)
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.RenderErr(w, ctx.RequestID, common.RecordNotFound)
		} else {
			common.RenderErr(w, ctx.RequestID, common.InternalError)
		}
		return
	}

	err = api.UpdateFileSystemCacheConfig(&ctx, req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			common.RenderErr(w, ctx.RequestID, common.RecordNotFound)
		} else {
			common.RenderErr(w, ctx.RequestID, common.InternalError)
		}
		logger.LoggerForRequest(&ctx).Errorf(
			"GetFSCacheConfig[%s] failed. error:%v", req.FsID, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	common.RenderStatus(w, http.StatusOK)
}

// FSCacheReport
// @Summary 上报FsID的缓存信息
// @Description  上报FsID的缓存信息
// @Id FSCacheReport
// @tags FSCacheConfig
// @Accept  json
// @Produce json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Param request body fs.CacheReportRequest true "request body"
// @Success 200 {object}
// @Failure 400 {object} common.ErrorResponse "400"
// @Failure 500 {object} common.ErrorResponse "500"
// @Router /fsCache/report [POST]
func (pr *PFSRouter) fsCacheReport(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var request api.CacheReportRequest
	err := common.BindJSON(r, &request)
	if err != nil {
		ctx.Logging().Errorf("FSCachReport bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	if request.Username == "" {
		request.Username = ctx.UserName
	}

	err = validateFsCacheReport(&ctx, &request)
	if err != nil {
		ctx.Logging().Errorf("validateFsCacheReport request[%v] failed: [%v]", request, err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	ctx.Logging().Debugf("report cache with req[%v]", request)

	err = api.ReportCache(&ctx, request)
	if err != nil {
		ctx.Logging().Errorf("report cache with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusOK)
}

func validateFsCacheReport(ctx *logger.RequestContext, req *api.CacheReportRequest) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		for _, err = range err.(validator.ValidationErrors) {
			ctx.ErrorCode = common.InappropriateJSON
			return err
		}
	}
	return nil
}

// createFsMount handles requests of creating filesystem mount record
// @Summary createFsMount
// @Description 创建mount记录
// @tag fs
// @Accept   json
// @Produce  json
// @Param request body fs.CreateMountRequest true "request body"
// @Success 201 {string} string Created
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsMount [post]
func (pr *PFSRouter) createFsMount(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	var req api.CreateMountRequest
	err := common.BindJSON(r, &req)
	if err != nil {
		ctx.Logging().Errorf("createFSMount bindjson failed. err:%s", err.Error())
		common.RenderErr(w, ctx.RequestID, common.MalformedJSON)
		return
	}
	if req.Username == "" {
		req.Username = ctx.UserName
	}

	ctx.Logging().Debugf("create file system cache with req[%v]", req)

	err = validateCreateMount(&ctx, &req)
	if err != nil {
		ctx.Logging().Errorf("validateCreateMount failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = api.CreateMount(&ctx, req)
	if err != nil {
		ctx.Logging().Errorf("create mount with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusCreated)
}

func validateCreateMount(ctx *logger.RequestContext, req *api.CreateMountRequest) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		for _, err = range err.(validator.ValidationErrors) {
			ctx.ErrorCode = common.InappropriateJSON
			return err
		}
	}
	return nil
}

// listFsMount the function that handle the list mount by fsID and nodename
// @Summary listFsMount
// @Description 获取mount列表
// @tag fs
// @Accept   json
// @Produce  json
// @Param fsName query string true "存储名称"
// @Param username query string false "用户名"
// @Param clusterID query string true "集群ID"
// @Param nodename query string true "结点名称"
// @Param marker query string false "查询起始条目加密条码"
// @Param maxKeys query string false "每页条数"
// @Success 200 {object} fs.ListFileSystemResponse
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsMount [get]
func (pr *PFSRouter) listFsMount(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)

	maxKeys, err := util.GetQueryMaxKeys(&ctx, r)
	if err != nil {
		common.RenderErrWithMessage(w, ctx.RequestID, common.InvalidURI, err.Error())
		return
	}
	req := api.ListMountRequest{
		FsName:    r.URL.Query().Get(util.QueryFsName),
		Username:  r.URL.Query().Get(util.QueryKeyUserName),
		Marker:    r.URL.Query().Get(util.QueryKeyMarker),
		MaxKeys:   int32(maxKeys),
		ClusterID: r.URL.Query().Get(util.QueryClusterID),
		NodeName:  r.URL.Query().Get(util.QueryNodeName),
	}
	log.Debugf("list file mount with req[%v]", req)
	if req.Username == "" {
		req.Username = ctx.UserName
	}

	err = validateListMount(&ctx, &req)
	if err != nil {
		ctx.Logging().Errorf("validateListMount failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	listMounts, nextMarker, err := api.ListMount(&ctx, req)
	if err != nil {
		ctx.Logging().Errorf("list mount with error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}
	response := *getListMountResult(listMounts, nextMarker, req.Marker)
	ctx.Logging().Debugf("List mount:%v", string(config.PrettyFormat(response)))
	common.Render(w, http.StatusOK, response)
}

func validateListMount(ctx *logger.RequestContext, req *api.ListMountRequest) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		for _, err = range err.(validator.ValidationErrors) {
			ctx.ErrorCode = common.InappropriateJSON
			return err
		}
	}
	return nil
}

func getListMountResult(fsMounts []models.FsMount, nextMarker, marker string) *api.ListMountResponse {
	var fsMountLists []*api.MountResponse
	for _, fsMount := range fsMounts {
		FsList := &api.MountResponse{
			MountID:    fsMount.MountID,
			FsID:       fsMount.FsID,
			MountPoint: fsMount.MountPoint,
			NodeName:   fsMount.NodeName,
			ClusterID:  fsMount.NodeName,
		}
		fsMountLists = append(fsMountLists, FsList)
	}
	ListFsResponse := &api.ListMountResponse{
		Marker:    marker,
		MountList: fsMountLists,
		Truncated: false,
	}
	if nextMarker != "" {
		ListFsResponse.Truncated = true
		ListFsResponse.NextMarker = nextMarker
	}
	return ListFsResponse
}

// deleteFsMount handles requests of deleting filesystem mount record
// @Summary deleteFsMount
// @Description 删除mount记录
// @tag fs
// @Accept   json
// @Produce  json
// @Param fsName path string true "存储名称"
// @Param username query string false "用户名"
// @Param mountpoint query string true "挂载点"
// @Param clusterID query string true "集群ID"
// @Param nodename query string true "结点名称"
// @Success 200
// @Failure 400 {object} common.ErrorResponse
// @Failure 404 {object} common.ErrorResponse
// @Failure 500 {object} common.ErrorResponse
// @Router /fsMount/{fsName} [delete]
func (pr *PFSRouter) deleteFsMount(w http.ResponseWriter, r *http.Request) {
	ctx := common.GetRequestContext(r)
	fsName := chi.URLParam(r, util.QueryFsName)
	req := api.DeleteMountRequest{
		Username:   r.URL.Query().Get(util.QueryKeyUserName),
		FsName:     fsName,
		MountPoint: r.URL.Query().Get(util.QueryMountPoint),
		ClusterID:  r.URL.Query().Get(util.QueryClusterID),
		NodeName:   r.URL.Query().Get(util.QueryNodeName),
	}

	if req.Username == "" {
		req.Username = ctx.UserName
	}

	ctx.Logging().Debugf("delete fs mount with req[%v]", req)

	err := validateDeleteMount(&ctx, &req)
	if err != nil {
		ctx.Logging().Errorf("validateDeleteMount failed: [%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	err = api.DeleteMount(&ctx, req)
	if err != nil {
		ctx.Logging().Errorf("delete fs mount with service error[%v]", err)
		common.RenderErrWithMessage(w, ctx.RequestID, ctx.ErrorCode, err.Error())
		return
	}

	common.RenderStatus(w, http.StatusOK)
}

func validateDeleteMount(ctx *logger.RequestContext, req *api.DeleteMountRequest) error {
	validate := validator.New()
	err := validate.Struct(req)
	if err != nil {
		for _, err = range err.(validator.ValidationErrors) {
			ctx.ErrorCode = common.InappropriateJSON
			return err
		}
	}
	return nil
}
