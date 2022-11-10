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
	mockNodeID := "test-node-id"
	err := NodeCache.AddNode(&model.NodeInfo{
		ID:        mockNodeID,
		Name:      "test-instance",
		ClusterID: "test-cluster-ID",
		Status:    "Ready",
		Capacity: map[string]string{
			"cpu":    "20",
			"memory": "20Gi",
		},
		Labels: map[string]string{
			"xxx/queue-name": "default-queue",
		},
	})
	assert.Equal(t, nil, err)

	_, err = NodeCache.GetNode(mockNodeID)
	assert.Equal(t, nil, err)

	updatedNode := &model.NodeInfo{
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
