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
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	api "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/router/util"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	fuse "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fs"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils/k8s"
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
	r.Delete("/fsCache/{fsName}", pr.deleteFSCacheConfig)
	r.Post("/fsCache/report", pr.fsCacheReport)
	// fs mount
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
