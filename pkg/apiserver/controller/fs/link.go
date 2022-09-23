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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	fuse "github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fs"
	fsCommon "github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

// LinkService the service which contains the operation of link
type LinkService struct {
	mutexes sync.Map
}

type CreateLinkRequest struct {
	FsName     string            `json:"fsName"`
	Url        string            `json:"url"`
	Properties map[string]string `json:"properties"`
	Username   string            `json:"username"`
	FsPath     string            `json:"fsPath"`
}

type DeleteLinkRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
	FsPath   string `json:"fsPath"`
}

type GetLinkRequest struct {
	Marker  string `json:"marker"`
	MaxKeys int32  `json:"maxKeys"`
	FsID    string `json:"fsID"`
	FsPath  string `json:"fsPath"`
}

type GetLinkResponse struct {
	Marker     string          `json:"marker"`
	Truncated  bool            `json:"truncated"`
	NextMarker string          `json:"nextMarker"`
	LinkList   []*LinkResponse `json:"linkList"`
}

type LinkResponse struct {
	FsName        string            `json:"fsName"`
	FsPath        string            `json:"fsPath"`
	ServerAddress string            `json:"serverAddress"`
	Type          string            `json:"type"`
	Username      string            `json:"username"`
	SubPath       string            `json:"subPath"`
	Properties    map[string]string `json:"properties"`
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
func (s *LinkService) CreateLink(ctx *logger.RequestContext, req *CreateLinkRequest) (model.Link, error) {
	fsType, serverAddress, subPath := common.InformationFromURL(req.Url, req.Properties)
	fsID := common.ID(req.Username, req.FsName)
	link := model.Link{
		FsID:          fsID,
		FsPath:        req.FsPath,
		PropertiesMap: req.Properties,
		ServerAddress: serverAddress,
		Type:          fsType,
		SubPath:       subPath,
		UserName:      req.Username,
	}

	err := storage.Filesystem.CreateLink(&link)
	if err != nil {
		ctx.Logging().Errorf("create link[%v] in db failed: %v", link, err)
		ctx.ErrorCode = common.LinkModelError
		return model.Link{}, err
	}
	return link, nil
}

// DeleteLink the function which performs the operation of delete file system link
func (s *LinkService) DeleteLink(ctx *logger.RequestContext, req *DeleteLinkRequest) error {
	err := storage.Filesystem.DeleteLinkWithFsIDAndFsPath(common.ID(req.Username, req.FsName), req.FsPath)
	if err != nil {
		ctx.Logging().Errorf("delete link failed error[%v]", err)
		ctx.ErrorCode = common.FileSystemDataBaseError
		return err
	}

	return err
}

// GetLink the function which performs the operation of list file system links
func (s *LinkService) GetLink(req *GetLinkRequest) ([]model.Link, string, error) {
	limit := req.MaxKeys + 1
	marker := req.Marker
	if req.Marker == "" {
		marker = time.Now().Format(TimeFormat)
	}
	var items []model.Link
	var err error
	if req.FsPath == "" {
		items, err = storage.Filesystem.ListLink(int(limit), marker, req.FsID)
		if err != nil {
			log.Errorf("list links models err[%v]", err)
			return nil, "", err
		}
	} else {
		items, err = storage.Filesystem.GetLinkWithFsIDAndPath(req.FsID, req.FsPath)
		if err != nil {
			log.Errorf("get link models err[%v]", err)
			return nil, "", err
		}
	}

	itemsLen := len(items)
	if itemsLen == 0 && req.FsPath != "" {
		log.Errorf("get link with fsID[%s] fsPath[%s] failed", req.FsID, req.FsPath)
		return []model.Link{}, "", common.New("Link not exist")
	}

	if itemsLen > int(req.MaxKeys) {
		return items[:len(items)-1], items[len(items)-1].UpdatedAt.Format(TimeFormat), err
	}

	return items, "", err
}

func (s *LinkService) PersistLinksMeta(fsID string) error {
	unlock := s.FsLock(fsID)
	defer unlock()

	links, err := storage.Filesystem.FsNameLinks(fsID)
	if err != nil {
		log.Errorf("get links err[%v] with fsID[%s]", err, fsID)
		return err
	}

	linksMeta := make(map[string]fsCommon.FSMeta)
	for _, link := range links {
		fsName, _, err := utils.GetFsNameAndUserNameByFsID(link.FsID)
		if err != nil {
			return err
		}
		linksMeta[link.FsPath] = fsCommon.FSMeta{
			ID:            link.ID,
			Name:          fsName,
			UfsType:       link.Type,
			ServerAddress: link.ServerAddress,
			SubPath:       link.SubPath,
			Properties:    link.PropertiesMap,
			Type:          fsCommon.LinkType,
		}
	}

	linksMetaJson, err := json.Marshal(linksMeta)
	if err != nil {
		log.Errorf("json marshal links meta err[%v]", err)
		return err
	}

	encodedLinksMeta, err := common.AesEncrypt(string(linksMetaJson), common.AESEncryptKey)
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
	fs, err := storage.Filesystem.GetFileSystemWithFsID(fsID)
	if err != nil {
		log.Errorf("GetFileSystemWithFsID error[%v]", err)
		return err
	}
	fsMeta := fsCommon.FSMeta{
		ID:            fsID,
		Name:          fs.Name,
		UfsType:       fs.Type,
		ServerAddress: fs.ServerAddress,
		SubPath:       fs.SubPath,
		Properties:    fs.PropertiesMap,
		Type:          fsCommon.FSType,
	}
	client, err := fuse.NewFSClient(fsMeta, nil)
	if err != nil {
		return err
	}

	dirPath := filepath.Join(config.GlobalServerConfig.Fs.LinkMetaDirPrefix, fsCommon.LinkMetaDir)
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
	dstPath := filepath.Join(dirPath, fsCommon.LinkMetaFile)
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
