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
	"crypto/md5"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
)

/*
*	resourceHandler
 */
type ResourceHandler struct {
	pplRunID  string
	fsHandler *handler.FsHandler
	logger    *log.Entry
}

func NewResourceHandler(runID string, fsID string, logger *log.Entry) (ResourceHandler, error) {
	fsHandler, err := handler.NewFsHandlerWithServer(fsID, logger)

	if err != nil {
		newErr := fmt.Errorf("init fsHandler failed: %s", err.Error())
		logger.Errorln(newErr)
		return ResourceHandler{}, newErr
	}

	resourceHandler := ResourceHandler{
		pplRunID:  runID,
		fsHandler: fsHandler,
		logger:    logger,
	}
	return resourceHandler, nil
}

func (resourceHandler *ResourceHandler) GenerateOutAtfPath(pplName, rootPath, stepName, runtimeName string,
	seq int, outatfName string, toInit bool) (string, error) {
	pipelineDir := ".pipeline"
	md5sum := md5.Sum([]byte(runtimeName))
	outatfDir := fmt.Sprintf("%s/%s/%s/%s-%d-%x", pipelineDir, resourceHandler.pplRunID, pplName, stepName,
		seq, md5sum)

	rootPath = strings.TrimRight(rootPath, "/")
	if rootPath != "" {
		outatfDir = fmt.Sprintf("%s/%s", rootPath, outatfDir)
	}

	outatfPath := fmt.Sprintf("%s/%s", outatfDir, outatfName)

	if toInit {
		isExist, err := resourceHandler.fsHandler.Exist(outatfPath)
		if err != nil {
			return "", err
		} else if isExist {
			resourceHandler.logger.Infof("path[%s] of outAtf[%s] already existed, clear first", outatfPath, outatfName)
			err = resourceHandler.fsHandler.RemoveAll(outatfPath)
			if err != nil {
				newErr := fmt.Errorf("clear generatePath[%s] for outAtf[%s] in step[%s] with pplname[%s] pplrunid[%s] failed: %s",
					outatfPath, outatfName, stepName, pplName, resourceHandler.pplRunID, err.Error())
				return "", newErr
			}
		}

		isExist, err = resourceHandler.fsHandler.Exist(outatfPath)
		if err != nil {
			return "", err
		} else if !isExist {
			resourceHandler.logger.Infof("prepare dir[%s] for path[%s] of outAtf[%s]", outatfDir, outatfPath, outatfName)
			err = resourceHandler.fsHandler.MkdirAll(outatfDir, os.ModePerm)
			if err != nil {
				newErr := fmt.Errorf("prepare dir[%s] for outAtf[%s] in step[%s] with pplname[%s] pplrunid[%s] failed: %s",
					outatfDir, outatfName, stepName, pplName, resourceHandler.pplRunID, err.Error())
				return "", newErr
			}
		}
	}

	return outatfPath, nil
}

func (resourceHandler *ResourceHandler) ClearResource() error {
	// 用于清理pplRunID对应的output artifact资源
	pipelineDir := "./.pipeline"
	runResourceDir := fmt.Sprintf("%s/%s", pipelineDir, resourceHandler.pplRunID)

	resourceHandler.logger.Infof("clear resource path[%s] of pplrunID[%s]", runResourceDir, resourceHandler.pplRunID)
	err := resourceHandler.fsHandler.RemoveAll(runResourceDir)
	if err != nil {
		newErr := fmt.Errorf("clear resource path[%s] of pplrunID[%s] failed: %s",
			runResourceDir, resourceHandler.pplRunID, err.Error())
		return newErr
	}

	return nil
}
