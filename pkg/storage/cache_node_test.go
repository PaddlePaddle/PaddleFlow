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

package storage

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

func initMockCache() {
	// github.com/mattn/go-sqlite3
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		// print sql
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Fatalf("InitMockDB open db error: %v", err)
	}

	if err = db.AutoMigrate(
		&model.NodeInfo{},
		&model.PodInfo{},
		&model.ResourceInfo{},
		&model.LabelInfo{},
	); err != nil {
		log.Fatalf("InitMockDB createDatabaseTables error[%s]", err.Error())
	}
	ClusterCache = db
	InitClusterCaches(db)
}

func TestNodeCache(t *testing.T) {
	initMockCache()
	t.Run("test node cache list", func(t *testing.T) {
		mockClusterName := "test-cluster-1"
		nodeInfos := []model.NodeInfo{
			{
				ID:          "test-cluster-ID-test-node-1",
				Name:        "test-instance-1",
				ClusterID:   mockClusterName,
				ClusterName: mockClusterName,
				Status:      "Ready",
				Capacity: map[string]string{
					"cpu":    "20",
					"memory": "20Gi",
				},
				Labels: map[string]string{
					"xxx/queue-name": "default-queue",
				},
			},
			{
				ID:          "test-cluster-ID-test-node-2",
				Name:        "test-instance-2",
				ClusterID:   "test-cluster-2",
				ClusterName: "test-cluster-2",
				Status:      "Ready",
				Capacity: map[string]string{
					"cpu":    "20",
					"memory": "20Gi",
				},
				Labels: map[string]string{
					"xxx/queue-name": "test-queue",
				},
			},
		}
		var err error
		err = NodeCache.AddNode(&nodeInfos[0])
		assert.Equal(t, nil, err)
		err = NodeCache.AddNode(&nodeInfos[1])
		assert.Equal(t, nil, err)

		// 1. list all
		var nodes []model.NodeInfo
		nodes, err = NodeCache.ListNode([]string{}, "", 0, 0)
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(nodes))
		t.Logf("nodes: %+v", nodes)
		// 2. list one cluster
		nodes, err = NodeCache.ListNode([]string{mockClusterName}, "", 0, 0)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(nodes))
		// 3. list with label filter equal
		nodes, err = NodeCache.ListNode([]string{}, "xxx/queue-name=default-queue", 0, 0)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(nodes))
		assert.Equal(t, nodeInfos[0].ID, nodes[0].ID)
		// 4. list with label filter not equal
		nodes, err = NodeCache.ListNode([]string{}, "xxx/queue-name!=default-queue", 0, 0)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(nodes))
		assert.Equal(t, nodeInfos[1].ID, nodes[0].ID)
		// 5. list with limit
		nodes, err = NodeCache.ListNode([]string{}, "", 2, 0)
		assert.Equal(t, nil, err)
		assert.Equal(t, 2, len(nodes))
		nodes, err = NodeCache.ListNode([]string{}, "", 1, 0)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(nodes))
		// 6. list with offset
		nodes, err = NodeCache.ListNode([]string{}, "", 0, 1)
		assert.Equal(t, nil, err)
		assert.Equal(t, 1, len(nodes))
		nodes, err = NodeCache.ListNode([]string{}, "", 1, 2)
		assert.Equal(t, nil, err)
		assert.Equal(t, 0, len(nodes))
	})

	t.Run("test node cache get/update/delete", func(t *testing.T) {
		mockNodeID := "test-node-id"
		nodeInfo := &model.NodeInfo{
			ID:          mockNodeID,
			Name:        "test-instance",
			ClusterID:   "test-cluster-ID",
			ClusterName: "test-cluster-ID",
			Status:      "Ready",
			Capacity: map[string]string{
				"cpu":    "20",
				"memory": "20Gi",
			},
			Labels: map[string]string{
				"xxx/queue-name": "default-queue",
			},
		}
		err := NodeCache.AddNode(nodeInfo)
		assert.Equal(t, nil, err)

		_, err = NodeCache.GetNode(mockNodeID)
		assert.Equal(t, nil, err)

		updatedNode := &model.NodeInfo{
			ID:     mockNodeID,
			Status: "NotReady",
			Labels: map[string]string{
				"xxx/queue-name": "test-queue",
			},
		}
		err = NodeCache.UpdateNode(mockNodeID, updatedNode)
		assert.Equal(t, nil, err)

		mockNodeInfo, err := NodeCache.GetNode(mockNodeID)
		assert.Equal(t, nil, err)
		t.Logf("node info %v", mockNodeInfo)

		err = NodeCache.DeleteNode(mockNodeID)
		assert.Equal(t, nil, err)
	})
}

func TestCacheLabel(t *testing.T) {
	initMockCache()

	mockObjectID := "test-node-id"
	err := LabelCache.AddLabel(&model.LabelInfo{
		ID:         "test-label-id",
		Name:       "xxx/queue-name",
		Value:      "default-queue",
		ObjectID:   mockObjectID,
		ObjectType: model.ObjectTypeNode,
	})
	assert.Equal(t, nil, err)

	err = LabelCache.DeleteLabel(mockObjectID, model.ObjectTypeNode)
	assert.Equal(t, nil, err)
}
