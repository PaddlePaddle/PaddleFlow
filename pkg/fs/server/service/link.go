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

package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	apicommon "paddleflow/pkg/apiserver/common"
	"paddleflow/pkg/apiserver/models"
	"paddleflow/pkg/common/config"
	"paddleflow/pkg/common/logger"
	"paddleflow/pkg/fs/client/base"
	fuse "paddleflow/pkg/fs/client/fs"
	"paddleflow/pkg/fs/server/api/common"
	"paddleflow/pkg/fs/server/api/request"
	utils "paddleflow/pkg/fs/server/utils/fs"
)

// LinkService the service which contains the operation of link
type LinkService struct {
	mutexes sync.Map
}

var linkService *LinkService

// GetLinkService returns the instance of link service
func GetLinkService() *LinkService {
	if linkService == nil {
		linkService = &LinkService{}
	}
	return linkService
}

// CreateLink the function which performs the operation of creating Link
func (s *LinkService) CreateLink(ctx *logger.RequestContext, req *request.CreateLinkRequest) (models.Link, error) {
	fsType, serverAddress, subPath := utils.InformationFromURL(req.Url, req.Properties)
	fsID := utils.ID(req.Username, req.FsName)
	link := models.Link{
		FsID:          fsID,
		FsPath:        req.FsPath,
		PropertiesMap: req.Properties,
		ServerAddress: serverAddress,
		Type:          fsType,
		SubPath:       subPath,
		UserName:      req.Username,
	}

	err := models.CreateLink(&link)
	if err != nil {
		ctx.Logging().Errorf("create link[%v] in db failed: %v", link, err)
		ctx.ErrorCode = common.LinkModelError
		return models.Link{}, err
	}
	return link, nil
}

// DeleteLink the function which performs the operation of delete file system link
func (s *LinkService) DeleteLink(ctx *logger.RequestContext, req *request.DeleteLinkRequest) error {
	err := models.DeleteLinkWithFsIDAndFsPath(utils.ID(req.Username, req.FsName), req.FsPath)
	if err != nil {
		ctx.Logging().Errorf("delete link failed error[%v]", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}

	return err
}

// GetLink the function which performs the operation of list file system links
func (s *LinkService) GetLink(req *request.GetLinkRequest) ([]models.Link, string, error) {
	limit := req.MaxKeys + 1
	marker := req.Marker
	if req.Marker == "" {
		marker = time.Now().Format(TimeFormat)
	}
	var items []models.Link
	var err error
	if req.FsPath == "" {
		items, err = models.ListLink(int(limit), marker, req.FsID)
		if err != nil {
			log.Errorf("list links models err[%v]", err)
			return nil, "", err
		}
	} else {
		if req.Username == utils.UserRoot {
			req.Username = ""
		}
		items, err = models.GetLinkWithFsIDFsPathAndUserName(req.FsID, req.FsPath, req.Username)
		if err != nil {
			log.Errorf("get link models err[%v]", err)
			return nil, "", err
		}
	}

	itemsLen := len(items)
	if itemsLen == 0 && req.FsPath != "" {
		log.Errorf("get link with username[%s] fsID[%s] fsPath[%s] failed", req.Username, req.FsID, req.FsPath)
		return []models.Link{}, "", common.New("Link not exist")
	}

	if itemsLen > int(req.MaxKeys) {
		return items[:len(items)-1], items[len(items)-1].UpdatedAt.Format(TimeFormat), err
	}

	return items, "", err
}

func (s *LinkService) PersistLinksMeta(fsID string) error {
	unlock := s.FsLock(fsID)
	defer unlock()

	links, err := models.FsNameLinks(fsID)
	if err != nil {
		log.Errorf("get links err[%v] with fsID[%s]", err, fsID)
		return err
	}

	linksMeta := make(map[string]base.FSMeta)
	for _, link := range links {
		linksMeta[link.FsPath] = base.FSMeta{
			ID:            link.ID,
			Name:          utils.FSIDToName(link.ID),
			UfsType:       link.Type,
			ServerAddress: link.ServerAddress,
			SubPath:       link.SubPath,
			Properties:    link.PropertiesMap,
			Type:          base.LinkType,
		}
	}

	linksMetaJson, err := json.Marshal(linksMeta)
	if err != nil {
		log.Errorf("json marshal links meta err[%v]", err)
		return err
	}

	encodedLinksMeta, err := apicommon.AesEncrypt(string(linksMetaJson), apicommon.AESEncryptKey)
	if err != nil {
		log.Errorf("aes encrypt links meta json string err[%v]", err)
		return err
	}

	if err = writeLinksMeta(encodedLinksMeta, fsID); err != nil {
		log.Errorf("write links meta err[%v] with fsID[%s]", err, fsID)
		return err
	}
	return nil
}

func writeLinksMeta(encodedLinksMeta string, fsID string) error {
	fs, err := models.GetFileSystemWithFsID(fsID)
	if err != nil {
		log.Errorf("GetFileSystemWithFsID error[%v]", err)
		return err
	}
	fsMeta := base.FSMeta{
		ID:            fsID,
		Name:          fs.Name,
		UfsType:       fs.Type,
		ServerAddress: fs.ServerAddress,
		SubPath:       fs.SubPath,
		Properties:    fs.PropertiesMap,
		Type:          base.FSType,
	}
	client, err := fuse.NewFSClient(fsMeta, nil)
	if err != nil {
		return err
	}

	dirPath := filepath.Join(config.GlobalServerConfig.Fs.LinkMetaDirPrefix, base.LinkMetaDir)
	if err := client.MkdirAll(dirPath, os.ModePerm); err != nil {
		log.Errorf("client mkdirAll file err:%v", err)
		return err
	}

	tempSrcFile := uuid.NewString()
	if err := client.SaveFile(strings.NewReader(encodedLinksMeta), dirPath, tempSrcFile); err != nil {
		log.Errorf("client save file err:%v", err)
		return err
	}

	srcPath := filepath.Join(dirPath, tempSrcFile)
	dstPath := filepath.Join(dirPath, base.LinkMetaFile)
	dstExit, _ := client.Exist(dstPath)
	var tempDstPath string
	if dstExit {
		tempDstPath = filepath.Join(dirPath, uuid.NewString())
		if err := client.Rename(dstPath, tempDstPath); err != nil {
			return err
		}
		defer client.Remove(tempDstPath)
	}

	err = client.Rename(srcPath, dstPath)
	if err != nil {
		if tempDstPath != "" {
			client.Rename(tempDstPath, dstPath)
		}
		return err
	}
	return nil
}

// todo 考虑多server并发控制
func (s *LinkService) FsLock(fsID string) func() {
	value, _ := s.mutexes.LoadOrStore(fsID, &sync.Mutex{})
	mtx := value.(*sync.Mutex)
	mtx.Lock()

	return func() { mtx.Unlock() }
}
