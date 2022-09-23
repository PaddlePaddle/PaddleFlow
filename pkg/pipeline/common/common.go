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
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

// 获取制定Artifact的内容
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
		return "", err
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

func TopologicalSort(components map[string]schema.Component) ([]string, error) {
	// unsorted: unsorted graph
	// if we have dag:
	//     1 -> 2 -> 3
	// then unsorted as follow will be get:
	//     1 -> [2]
	//     2 -> [3]
	sortedComponent := make([]string, 0)
	unsorted := map[string][]string{}
	for name, component := range components {
		depsList := component.GetDeps()

		if len(depsList) == 0 {
			unsorted[name] = nil
			continue
		}

		unsorted[name] = append(unsorted[name], depsList...)
	}

	// 通过判断入度，每一轮寻找入度为0（没有parent节点）的节点，从unsorted中移除，并添加到sortedSteps中
	// 如果unsorted长度被减少到0，说明无环。如果有一轮没有出现入度为0的节点，说明每个节点都有父节点，即有环。
	for len(unsorted) != 0 {
		acyclic := false
		for name, parents := range unsorted {
			parentExist := false
			for _, parent := range parents {
				if _, ok := unsorted[parent]; ok {
					parentExist = true
					break
				}
			}
			// if all the source nodes of this node has been removed,
			// consider it as sorted and remove this node from the unsorted graph
			if !parentExist {
				acyclic = true
				delete(unsorted, name)
				sortedComponent = append(sortedComponent, name)
			}
		}
		if !acyclic {
			// we have go through all the nodes and weren't able to resolve any of them
			// there must be cyclic edges
			return nil, fmt.Errorf("workflow is not acyclic")
		}
	}
	return sortedComponent, nil
}

func LatestTime(times []time.Time) time.Time {
	latestTime := time.Time{}

	for _, t := range times {
		if latestTime.Before(t) {
			latestTime = t
		}
	}

	return latestTime
}

func GetFSMountPath(FS *schema.FsMount) string {
	if FS.MountPath != "" {
		return FS.MountPath
	} else {
		return fmt.Sprintf("/home/paddleflow/storage/mnt/%s", FS.ID)
	}
}

func GetArtifactMountPath(mainFS *schema.FsMount, artifactPath string) string {
	mountPath := GetFSMountPath(mainFS)
	mountPath = strings.TrimRight(mountPath, "/")
	subPath := strings.TrimRight(mainFS.SubPath, "/")

	artMountPaths := []string{}

	for _, path := range strings.Split(artifactPath, ",") {
		if path == "" {
			continue
		}

		var artMountPath string
		if subPath != "" {
			artMountPath = strings.Replace(path, subPath, mountPath, 1)
		} else {
			artMountPath = fmt.Sprintf("%s/%s", mountPath, path)
		}

		if !strings.HasPrefix(artMountPath, "/") {
			artMountPath = "/" + artMountPath
		}

		artMountPaths = append(artMountPaths, artMountPath)
	}

	return strings.Join(artMountPaths, ",")
}

func GetSiblingAbsoluteName(curAbsName string, siblingRelativeName string) string {
	nameList := strings.Split(curAbsName, ".")
	parentAbsName := strings.Join(nameList[:len(nameList)-1], ".")
	var sibAbsName string
	if parentAbsName != "" {
		sibAbsName = parentAbsName + "." + siblingRelativeName
	} else {
		sibAbsName = siblingRelativeName
	}
	return sibAbsName
}

func CheckListParam(param []interface{}) error {
	for _, listItem := range param {
		switch listItem := listItem.(type) {
		case float32, float64, int, int64:
			// do nothing
		case string:
			checker := VariableChecker{}
			// list中的元素不能为模板，如果使用了模板，则报错
			if err := checker.CheckRefArgument(listItem); err == nil {
				return fmt.Errorf("list param item [%v] is invalid, each item must not be templete", listItem)
			}
		case []interface{}:
			if err := CheckListParam(listItem); err != nil {
				return err
			}
		default:
			return fmt.Errorf("list param item can only be int, float, string, list type")
		}
	}
	return nil
}
