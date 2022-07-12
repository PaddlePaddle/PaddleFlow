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

package handler

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	log "github.com/sirupsen/logrus"
	"github.com/smallnest/chanx"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

var PFImageHandler *ImageHandler

type ImageHandlerCallBackFunc func(imageInfo ImageInfo, err error) error

type imageHandleInfo struct {
	runID string
	fsID  string

	// docker_env in run.yaml
	dockerEnv string
	imageIDs  []string
	logEntry  *log.Entry
	cb        ImageHandlerCallBackFunc
}

type ImageInfo struct {
	RunID      string
	FsID       string
	Source     string // docker_env in run.yaml
	ImageID    string
	Url        string
	UrlUpdated bool
	PFImageID  string // 数据库中的唯一标识符
}

type ImageConfig struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

type ImageHandlerError struct {
	errMsg string
}

func CompsNeedHandleImage(comps map[string]schema.Component) bool {
	for _, comp := range comps {
		if dag, ok := comp.(*schema.WorkflowSourceDag); ok {
			if need := CompsNeedHandleImage(dag.EntryPoints); need {
				return true
			}
		} else {
			step, ok := comp.(*schema.WorkflowSourceStep)
			if !ok {
				logger.Logger().Errorf("comp is not dag or step")
				return false
			}
			envType := classifyEnvType(step.DockerEnv)
			switch envType {
			case TarFile:
				return true
			case RegistryUrl:
				continue
			default:
				continue
			}
		}
	}
	return false
}

func NeedHandleImage(wfs schema.WorkflowSource) bool {
	if need := CompsNeedHandleImage(wfs.Components); need {
		return true
	}
	if need := CompsNeedHandleImage(wfs.EntryPoints.EntryPoints); need {
		return true
	}
	postMap := map[string]schema.Component{}
	for k, v := range wfs.PostProcess {
		postMap[k] = v
	}
	if need := CompsNeedHandleImage(postMap); need {
		return true
	}
	return false
}

func (e ImageHandlerError) Error() string {
	return e.errMsg
}

type dockerEnvType string

const (
	TarFile            dockerEnvType = "tar"
	RegistryUrl        dockerEnvType = "url"
	imagePrefix        string        = "pf"
	dockerManifestFile               = "manifest.json"
	defaultConcurrency               = 10
)

func setAuthStr(username, passwd string) (string, error) {
	authConfig := types.AuthConfig{
		Username: username,
		Password: passwd,
	}

	encodedJSON, err := json.Marshal(authConfig)
	if err != nil {
		return "", err
	}
	authStr := base64.URLEncoding.EncodeToString(encodedJSON)
	return authStr, nil
}

func generatePFImageID(handleInfo imageHandleInfo, imageID string) string {
	return handleInfo.fsID + "-" + imageID
}

type ImageHandler struct {
	// 处理中的 handleInfo 的映射，key 为 fsID + ImageID
	handleInfoMap sync.Map

	// 已经处理过的 HandleID 与镜像 Url 之间的映射关系
	UrlMap sync.Map

	// 同时 加载镜像协程的数目
	concurrency int

	// 用于存储待处理的 image 信息的 channel
	handleChan *chanx.UnboundedChan

	// 镜像仓库 server/namespace
	imageRepo string

	// 镜像仓库的认证字符串
	imageAuthStr string

	// docker client
	imageClient *client.Client
	ctx         context.Context

	// 镜像加载完成后是否需要删除dockerd 机器上的镜像
	removeLocalImage bool

	// 是否已经停止加载镜像
	isStopped bool
}

func InitPFImageHandler() (*ImageHandler, error) {
	logger.LoggerForRun("initiation").Debugf("begin to new a ImageHandler")

	handleChan := chanx.NewUnboundedChan(100)

	ctx := context.Background()
	cli, err := client.NewEnvClient()
	if err != nil {
		logger.LoggerForRun("initiation").Debugf("new a imageClient failed")
		return nil, err
	}

	var authStr string
	if config.GlobalServerConfig.ImageConf.Server != "" {
		authStr, err = setAuthStr(config.GlobalServerConfig.ImageConf.Username,
			config.GlobalServerConfig.ImageConf.Password)

		if err != nil {
			logger.LoggerForRun("initiation").Errorf("set the authstr failed")
			return nil, err
		}
	} else {
		authStr = ""
	}

	var imageRepo string
	if config.GlobalServerConfig.ImageConf.Server == "" {
		imageRepo = config.GlobalServerConfig.ImageConf.Namespace
	} else if config.GlobalServerConfig.ImageConf.Namespace == "" {
		imageRepo = config.GlobalServerConfig.ImageConf.Server
	} else {
		imageRepo = config.GlobalServerConfig.ImageConf.Server + "/" + config.GlobalServerConfig.ImageConf.Namespace
	}

	concurrency := defaultConcurrency
	if config.GlobalServerConfig.ImageConf.Concurrency > 0 {
		concurrency = config.GlobalServerConfig.ImageConf.Concurrency
	}
	PFImageHandler = &ImageHandler{
		concurrency:      concurrency,
		handleChan:       handleChan,
		imageClient:      cli,
		imageRepo:        imageRepo,
		imageAuthStr:     authStr,
		ctx:              ctx,
		removeLocalImage: config.GlobalServerConfig.ImageConf.RemoveLocalImage,
		handleInfoMap:    sync.Map{},
		UrlMap:           sync.Map{},
	}

	return PFImageHandler, nil
}

func (handler *ImageHandler) HandleImage(dockerEnv, runID, fsID string, imageIDs []string, logEntry *log.Entry, cb ImageHandlerCallBackFunc) error {
	logEntry.Infof("handle image[%s] with run[%s].", dockerEnv, runID)
	envType := classifyEnvType(dockerEnv)
	switch envType {
	case TarFile:
		return handler.HandleTarImage(dockerEnv, runID, fsID, imageIDs, logEntry, cb)
	case RegistryUrl:
		return handler.HandleUrlImage(dockerEnv, runID, fsID, logEntry, cb)
	default:
		err := common.FileTypeNotSupportedError(string(envType), common.ResourceTypeImage)
		logEntry.Errorf("handle image[%s] failed for run[%s]. err:%v", dockerEnv, runID, err)
		return err
	}
}

func classifyEnvType(dockerEnv string) dockerEnvType {
	if strings.HasSuffix(dockerEnv, ".tar") {
		return TarFile
	} else {
		return RegistryUrl
	}
}

func (handler *ImageHandler) HandleUrlImage(dockerEnv, runID, fsID string, logEntry *log.Entry, cb ImageHandlerCallBackFunc) error {
	// 没有耗时操作，直接调用 cb, 无需入队
	logEntry.Infof("handle image[%s] as url for run[%s]", dockerEnv, runID)
	imageInfo := ImageInfo{
		RunID:      runID,
		FsID:       fsID,
		Source:     dockerEnv,
		Url:        dockerEnv,
		UrlUpdated: false,
	}
	go cb(imageInfo, nil)
	return nil
}

func (handler *ImageHandler) HandleTarImage(dockerEnv, runID, fsID string, imageIDs []string,
	logEntry *log.Entry, cb ImageHandlerCallBackFunc) (err error) {
	logEntry.Infof("handle image[%s] as tar pkg for run[%s]", dockerEnv, runID)
	err = nil

	handleInfo := imageHandleInfo{
		runID:     runID,
		fsID:      fsID,
		dockerEnv: dockerEnv,
		imageIDs:  imageIDs,
		logEntry:  logEntry,
		cb:        cb,
	}
	defer func() {
		if r := recover(); r != nil {
			errMsg := "handler tar image failed: " + fmt.Sprint(r)
			err := ImageHandlerError{errMsg: errMsg}
			logEntry.Infof("handle image[%s] as tar pkg for run[%s] failed:%v", dockerEnv, runID, err)

			imageInfo := ImageInfo{
				RunID:  runID,
				FsID:   fsID,
				Source: dockerEnv,
			}
			go cb(imageInfo, err)
		}
	}()

	handler.handleChan.In <- handleInfo
	return err
}

func (handler *ImageHandler) Run() {
	wg := sync.WaitGroup{}
	for i := 0; i < handler.concurrency; i++ {
		logger.LoggerForRun("imageHandler").Infof("begin to run process goroutine [%d]", i)
		wg.Add(1)
		go handler.process(&wg)
	}
	wg.Wait()
}

func (handler *ImageHandler) Stop() {
	logger.LoggerForRun("imageHandler").Infof("begin to stopping image handler")
	handler.isStopped = true
	close(handler.handleChan.In)
}

func (handler *ImageHandler) process(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		if handler.isStopped {
			return
		}
		info, ok := <-handler.handleChan.Out
		handleInfo := info.(imageHandleInfo)
		if !ok {
			handleInfo.logEntry.Errorf("get the handleInfo from UChan failed, maybe it has been closed")
			return
		}
		handleInfo.logEntry.Infof("begin to handle image with runID[%s]", handleInfo.runID)
		handler.processHandleInfo(handleInfo)
	}
}

func (handler *ImageHandler) handleImageConfig(handleInfo imageHandleInfo) (ImageConfig, error) {
	fsHandler, err := NewFsHandlerWithServer(handleInfo.fsID, handleInfo.logEntry)
	if err != nil {
		handleInfo.logEntry.Errorf("NewFsHandlerWithServer failed. err: %v", err)
		return ImageConfig{}, err
	}

	file, err := fsHandler.fsClient.Open(handleInfo.dockerEnv)
	if err != nil {
		handleInfo.logEntry.Errorf("open file [%s] failed. err: %v", handleInfo.dockerEnv, err)
		return ImageConfig{}, err
	}
	defer file.Close()

	imageConfs := make([]ImageConfig, 0)
	tr := tar.NewReader(file)
	for {
		header, err := tr.Next()
		if err != nil {
			// 如果在 tar 包没有找到 manifest.json 文件, err = io.EOF
			handleInfo.logEntry.Errorf("cannot get the ImageID with RunID[%s], fsID[%s], dockerEnv[%s]:%v",
				handleInfo.runID, handleInfo.fsID, handleInfo.dockerEnv, err)
			return ImageConfig{}, err
		}

		if header.Name != dockerManifestFile {
			continue
		} else {
			content, err := ioutil.ReadAll(tr)
			if err != nil {
				handleInfo.logEntry.Errorf("cannot get the ImageID with RunID[%s], fsID[%s], dockerEnv[%s]:%v",
					handleInfo.runID, handleInfo.fsID, handleInfo.dockerEnv, err)
				return ImageConfig{}, err
			}

			err = json.Unmarshal(content, &imageConfs)
			if err != nil {
				handleInfo.logEntry.Errorf("cannot get the ImageID with RunID[%s], fsID[%s], dockerEnv[%s]:%v",
					handleInfo.runID, handleInfo.fsID, handleInfo.dockerEnv, err)
				return ImageConfig{}, err
			}

			if len(imageConfs) != 1 {
				handleInfo.logEntry.Errorf("find more than one image with RunID[%s], fsID[%s], dockerEnv[%s]:%v",
					handleInfo.runID, handleInfo.fsID, handleInfo.dockerEnv, err)
				return ImageConfig{}, ImageHandlerError{errMsg: "more than one image in tar pkg is not support now"}
			}
			return imageConfs[0], nil
		}
	}
}

func (handler *ImageHandler) IsImageExistInRepo(handleInfo imageHandleInfo, imageID string) bool {
	urlPrefix := handler.generateImageUrlWithoutTag(handleInfo, imageID)

	// TODO: 对于远端的场景也需要进行判断是否存在，
	// 本期暂时假定云端是存在的
	if handler.imageRepo != "" {
		return true
	}

	images, err := handler.imageClient.ImageList(handler.ctx, types.ImageListOptions{})
	if err != nil {
		handleInfo.logEntry.Infof("cannot judge image[%s] exists on docker host or not, "+
			"due to getting the image list failed: %v", imageID, err)
		return false
	}

	for _, image := range images {
		if strings.Contains(image.ID, imageID) {
			for _, tag := range image.RepoTags {
				// 在数据库中，不会存在多个 urlPrefix 相同的记录
				if strings.HasPrefix(tag, urlPrefix) {
					// TODO: 这种情况应该只需要给镜像打 tag, 无需重新加载
					return true
				}
			}
		}
	}
	return false
}

func (handler *ImageHandler) processHandleInfo(handleInfo imageHandleInfo) {
	handleInfo.logEntry.Debugf("processHandleInfo with RunID[%s] starts", handleInfo.runID)
	if handler.isStopped {
		return
	}
	defer func() {
		if info := recover(); info != nil {
			runID := handleInfo.runID
			errmsg := fmt.Sprintf("imageHandler process failed, %v", info)
			logger.LoggerForRun(runID).Errorf(errmsg)
			if err := handleInfo.cb(ImageInfo{RunID: runID}, fmt.Errorf("%v", info)); err != nil {
				logger.LoggerForRun(runID).Errorf("update run status by cb after imageHandler panic, error: %v", err)
			}
		}
	}()
	imageConfig, err := handler.handleImageConfig(handleInfo)
	if err != nil {
		imageInfo := ImageInfo{
			RunID:  handleInfo.runID,
			FsID:   handleInfo.fsID,
			Source: handleInfo.dockerEnv,
		}
		go handleInfo.cb(imageInfo, err)
		return
	}

	imageID := imageConfig.Config[:64]
	isExistInRepo := handler.IsImageExistInRepo(handleInfo, imageID)

	isExistInDB := false
	for _, id := range handleInfo.imageIDs {
		if imageID == id {
			isExistInDB = true
		}
	}

	// 如果 isExistInDB 和 isExistInRepo 为True, 则说对应的 ImageID 在 数据库已经有记录，且在镜像仓库中存在对应的镜像, 此时直接调用 cb
	if isExistInDB && isExistInRepo {
		handleInfo.logEntry.Infof("The imageInfo of RunID[%s] already exists in DB", handleInfo.runID)
		imageInfo := ImageInfo{
			RunID:      handleInfo.runID,
			FsID:       handleInfo.fsID,
			Source:     handleInfo.dockerEnv,
			ImageID:    imageID,
			UrlUpdated: false,
			PFImageID:  generatePFImageID(handleInfo, imageID),
		}
		go handleInfo.cb(imageInfo, nil)
		return
	}

	handleID := handleInfo.fsID + imageID
	urlInterface, ok := handler.UrlMap.Load(handleID)
	if ok && isExistInRepo {
		handleInfo.logEntry.Infof("The imageInfo of RunID[%s] was handled before", handleInfo.runID)
		url := urlInterface.(string)

		imageInfo := ImageInfo{
			RunID:      handleInfo.runID,
			FsID:       handleInfo.fsID,
			Source:     handleInfo.dockerEnv,
			ImageID:    imageID,
			Url:        url,
			UrlUpdated: false,
			PFImageID:  generatePFImageID(handleInfo, imageID),
		}
		go handleInfo.cb(imageInfo, nil)
		return
	} else {
		handler.processImage(handleInfo, imageConfig)
	}

	return
}

func (handler *ImageHandler) ExecCBAndRemoveHandleID(logEntry *log.Entry, handleID, imageID, imageUrl string,
	err error) {
	logEntry.Debugf("ExecCBAndRemoveHandleID starts. handleID[%s], imageID[%s], imageUrl[%s]", handleID, imageID, imageUrl)

	UchanInterface, loaded := handler.handleInfoMap.LoadAndDelete(handleID)
	if !loaded {
		logEntry.Debugf("ExecCBAndRemoveHandleID failed. handleID[%s], imageID[%s], imageUrl[%s]", handleID, imageID, imageUrl)
		return
	}

	Uchan := UchanInterface.(chanx.UnboundedChan)
	close(Uchan.In)

	for {
		if handler.isStopped {
			return
		}

		infoInterface, ok := <-Uchan.Out
		if !ok {
			break
		}

		info := infoInterface.(imageHandleInfo)

		imageInfo := ImageInfo{
			RunID:      info.runID,
			FsID:       info.fsID,
			Source:     info.dockerEnv,
			ImageID:    imageID,
			Url:        imageUrl,
			UrlUpdated: true,
			PFImageID:  generatePFImageID(info, imageID),
		}
		go info.cb(imageInfo, err)
	}
}

func (handler *ImageHandler) processImage(handleInfo imageHandleInfo, imageConfig ImageConfig) {
	if handler.isStopped {
		return
	}

	imageID := imageConfig.Config[:64]
	handleID := handleInfo.fsID + imageID

	NewUchan := chanx.NewUnboundedChan(20)
	OldUchanInterface, loaded := handler.handleInfoMap.LoadOrStore(handleID, NewUchan)
	OldUchan := OldUchanInterface.(chanx.UnboundedChan)
	if loaded {
		defer func() {
			// 如果有 panic, 则是 OldUchan.In 已经 close, 此时的 handleIno 需要重新处理
			if r := recover(); r != nil {
				handler.processHandleInfo(handleInfo)
			}
		}()

		OldUchan.In <- handleInfo
	} else {
		NewUchan.In <- handleInfo
		RemoveTags := handler.generateRemoveTags(handleInfo.logEntry, imageConfig)
		imageUrl, err := handler.loadAndTagImage(handleInfo, imageID)
		if err != nil {
			handler.ExecCBAndRemoveHandleID(handleInfo.logEntry, handleID, imageID, imageUrl, err)
			return
		}

		// 删除镜像时，新生成的 tag 也需要删除
		RemoveTags = append(RemoveTags, imageUrl)

		err = handler.pushImage(handleInfo.logEntry, imageUrl)
		if err != nil {
			handler.ExecCBAndRemoveHandleID(handleInfo.logEntry, handleID, imageID, imageUrl, err)
			return
		}

		handler.UrlMap.Store(handleID, imageUrl)

		// 如果 imageConfigID 中有 RepoTag 的信息，则无法删除进行，因为此时的镜像会有多个 tag
		// docker cilent 未提供 UnTag 相关接口
		handler.RemoveImage(handleInfo.logEntry, RemoveTags)
		handler.ExecCBAndRemoveHandleID(handleInfo.logEntry, handleID, imageID, imageUrl, nil)
	}
	return
}

func (handler *ImageHandler) generateRemoveTags(logEntry *log.Entry, imageConfig ImageConfig) []string {
	logEntry.Debugln("generateRemoveTags starts")
	// 只删除在 tar 中存在且本地不存在的tag
	if len(imageConfig.RepoTags) == 0 {
		return []string{}
	}

	images, err := handler.imageClient.ImageList(handler.ctx, types.ImageListOptions{})
	if err != nil {
		logEntry.Infof("get the image list failed: %v", err)
		return []string{}
	}

	existTags := make([]string, 0)
	for _, image := range images {
		if strings.Contains(image.ID, imageConfig.Config[0:64]) {
			existTags = image.RepoTags
			break
		}
	}

	if len(existTags) == 0 {
		return imageConfig.RepoTags
	}

	tagMap := make(map[string]int)
	for _, tag := range existTags {
		tagMap[tag] = 1
	}

	removeTags := make([]string, 0)
	for _, tag := range imageConfig.RepoTags {
		_, ok := tagMap[tag]
		if !ok {
			removeTags = append(removeTags, tag)
		}
	}
	return removeTags
}

func (handler *ImageHandler) generateImageUrlWithoutTag(handleInfo imageHandleInfo, imageID string) string {
	re := regexp.MustCompile(`[^a-z0-9]`)

	// '/' 后不能紧跟 '_'
	fsIDInUrl := strings.Trim(re.ReplaceAllString(handleInfo.fsID, "_"), "_")
	DockerEnvInUrl := strings.Trim(re.ReplaceAllString(handleInfo.dockerEnv, "_"), "_")
	url := imagePrefix + "/" + fsIDInUrl + "/" + imageID[0:8] + "/" + DockerEnvInUrl

	if handler.imageRepo != "" {
		url = handler.imageRepo + "/" + url
		urlRegex := regexp.MustCompile(`[/]{2,}`)
		url = urlRegex.ReplaceAllString(url, "/")
	}
	urlRegex := regexp.MustCompile(`[__]{2,}`)
	url = urlRegex.ReplaceAllString(url, "_")
	return url
}

func (handler *ImageHandler) generateImageUrl(handleInfo imageHandleInfo, imageID string) string {
	handleInfo.logEntry.Debugf("generateImageUrl for imageID[%s] and runID[%s]", imageID, handleInfo.runID)

	url := handler.generateImageUrlWithoutTag(handleInfo, imageID)

	curTime := time.Now().UnixNano()
	tag := strconv.FormatInt(curTime, 10)[0:13]
	url += ":" + tag

	return url
}

func (handler *ImageHandler) loadAndTagImage(handleInfo imageHandleInfo, imageID string) (string, error) {
	handleInfo.logEntry.Debugln("loadAndTagImage starts")
	if handler.isStopped {
		handleInfo.logEntry.Debugln("imageHandler has been stopped")
		return "", ImageHandlerError{errMsg: "imageHandler has been Stopped"}
	}

	handleInfo.logEntry.Infof("begin to loading image with imageID[%s]", imageID)
	fsHandler, err := NewFsHandlerWithServer(handleInfo.fsID, handleInfo.logEntry)
	if err != nil {
		return "", err
	}

	file, err := fsHandler.fsClient.Open(handleInfo.dockerEnv)
	if err != nil {
		return "", err
	}
	defer file.Close()

	res, err := handler.imageClient.ImageLoad(handler.ctx, file, false)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	loadResByte, err := ioutil.ReadAll(res.Body)
	loadResStr := string(loadResByte)

	// 镜像加载失败时，err 也有可能为 nil. 但是 输出中会有 errorDetail 字符串
	if strings.Contains(loadResStr, "errorDetail") {
		return "", ImageHandlerError{errMsg: "load image failed: cannot find imageID"}
	}

	imageUrl := handler.generateImageUrl(handleInfo, imageID)
	err = handler.imageClient.ImageTag(handler.ctx, imageID, imageUrl)
	if err != nil {
		handleInfo.logEntry.Errorf("tag image[%s] failed with runID[%s], dockerEnv[%s]:%v",
			imageUrl, handleInfo.runID, handleInfo.dockerEnv, err)
	}
	return imageUrl, err
}

func (handler *ImageHandler) RemoveImage(logEntry *log.Entry, imageID []string) {
	if handler.isStopped {
		return
	}

	if !handler.removeLocalImage {
		return
	}

	if handler.imageRepo == "" {
		return
	}

	for _, imageID := range imageID {
		logEntry.Infof("begin to removing image[%s]", imageID)
		_, err := handler.imageClient.ImageRemove(handler.ctx, imageID, types.ImageRemoveOptions{Force: true})
		if err != nil {
			logEntry.Errorf("remove image[%s] failed:%v",
				imageID, err)
		}
	}
	return
}

func (handler *ImageHandler) pushImage(logEntry *log.Entry, imageUrl string) error {
	if handler.isStopped {
		logEntry.Infoln("imageHandler has been stopped")
		return ImageHandlerError{errMsg: "imageHandler has been stopped"}
	}

	if handler.imageRepo == "" {
		return nil
	}

	pushOpt := types.ImagePushOptions{
		RegistryAuth: handler.imageAuthStr,
	}

	logEntry.Infof("begin to pushing image[%s]", imageUrl)
	push_read, err := handler.imageClient.ImagePush(handler.ctx, imageUrl, pushOpt)
	if err != nil {
		logEntry.Errorf("push image[%s] failed:%v", imageUrl, err)

		return err
	}
	defer push_read.Close()

	if err != nil {
		return err
	}

	errByte, err := ioutil.ReadAll(push_read)
	errMsg := string(errByte)
	if strings.Contains(errMsg, "errorDetail") {
		logEntry.Errorf("push image[%s] failed: %s",
			imageUrl, errMsg)
		return ImageHandlerError{errMsg: errMsg}
	}
	return nil
}
