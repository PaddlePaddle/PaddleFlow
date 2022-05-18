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

package location_awareness

import (
	"errors"

	log "github.com/sirupsen/logrus"

	"paddleflow/pkg/apiserver/models"
)

func ListMountNodesByFsID(fsIDs []string) ([]string, error) {
	if len(fsIDs) == 0 {
		log.Errorf("GetFsMountByID IDs empty")
		return nil, errors.New("fsIDS empty")
	}
	nodesMap := make(map[string]struct{}, 0)

	for _, fsID := range fsIDs {
		nodeNames, err := models.ListMountNodesByID(fsID)
		if err != nil {
			log.Errorf("ListMountNodesByID[%s] err: %v", fsID, err)
			return nil, err
		}
		for _, nodeName := range nodeNames {
			nodesMap[nodeName] = struct{}{}
		}
	}

	nodes := make([]string, 0)
	for nodeName, _ := range nodesMap {
		nodes = append(nodes, nodeName)
	}

	return nodes, nil
}
