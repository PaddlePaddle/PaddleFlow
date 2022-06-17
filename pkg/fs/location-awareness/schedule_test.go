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
	"crypto/md5"
	"encoding/hex"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/database"
	"github.com/PaddlePaddle/PaddleFlow/pkg/service/db_service"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getMountID(clusterID, nodeName, mountPoint string) string {
	hash := md5.Sum([]byte(clusterID + nodeName + mountPoint))
	return hex.EncodeToString(hash[:])
}

func TestListMountNodesByFsID(t *testing.T) {
	database.InitMockDB()

	fsID1, fsID2, mountPoint1, mountPoint2, nodeName1, nodeName2, clusterID :=
		"fs-root-1", "fs-root-2", "/mnt/fs-root-1/storage", "/mnt/fs-root-2/storage", "node1", "node2", ""
	fsMount := &db_service.FsMount{
		FsID:       fsID1,
		MountPoint: mountPoint1,
		MountID:    getMountID(clusterID, nodeName1, mountPoint1),
		NodeName:   nodeName1,
		ClusterID:  clusterID,
	}
	err := fsMount.Add(fsMount)
	assert.Nil(t, err)

	fsMount = &db_service.FsMount{
		FsID:       fsID1,
		MountPoint: mountPoint1,
		MountID:    getMountID(clusterID, nodeName2, mountPoint1),
		NodeName:   nodeName2,
		ClusterID:  clusterID,
	}
	err = fsMount.Add(fsMount)
	assert.Nil(t, err)

	fsMount = &db_service.FsMount{
		FsID:       fsID2,
		MountPoint: mountPoint2,
		MountID:    getMountID(clusterID, nodeName1, mountPoint2),
		NodeName:   nodeName1,
		ClusterID:  clusterID,
	}
	err = fsMount.Add(fsMount)
	assert.Nil(t, err)

	fsIDs := []string{fsID1, fsID2, "fs-non-exist"}
	nodeList, err := ListFsCacheLocation(fsIDs)
	assert.Nil(t, err)

	assert.Equal(t, 2, len(nodeList))
	cnt := 0
	for _, nodeName := range nodeList {
		if nodeName == nodeName1 || nodeName == nodeName2 {
			cnt++
		}
	}
	assert.Equal(t, 2, cnt)
}
