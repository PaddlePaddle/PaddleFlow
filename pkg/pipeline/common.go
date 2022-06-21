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

package pipeline

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
)

// 获取制定Artifact的内容
// TODO: 作为 ResourceHandler 的成员函数
func GetArtifactContent(artPath string, maxSize int, fsID string, logger *logrus.Entry) (string, error) {
	fsHandler, err := handler.NewFsHandlerWithServer(fsID, logger)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact by path[%s] : %v",
			artPath, err.Error())
		return "", err
	}

	stat, err := fsHandler.Stat(artPath)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact by path[%s] : %v",
			artPath, err.Error())
		return "", err
	}

	if stat.IsDir() || stat.Size() >= int64(maxSize) {
		err = fmt.Errorf("failed to get the content of artifact by path[%s]: "+
			"maybe it's an directory or it is too large[>= %d]",
			artPath, maxSize)
	}

	content, err := fsHandler.ReadFsFile(artPath)
	if err != nil {
		err = fmt.Errorf("failed to get the content of artifact by path[%s] : %v",
			artPath, err.Error())
		return "", err
	}

	contentString := string(content)
	return contentString, nil
}

func GetRandID(randNum int) string {
	b := make([]byte, randNum/2)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func GetInputArtifactEnvName(atfName string) string {
	return "PF_INPUT_ARTIFACT_" + strings.ToUpper(atfName)
}

func GetOutputArtifactEnvName(atfName string) string {
	return "PF_OUTPUT_ARTIFACT_" + strings.ToUpper(atfName)
}
