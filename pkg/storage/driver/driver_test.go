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

package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
)

func TestInitCache(t *testing.T) {
	err := InitCache("DEBUG")
	assert.Equal(t, nil, err)
}

func TestFlavour(t *testing.T) {
	gf := getGormConf("")
	db := initSQLiteDB(gf)
	storage.InitStores(db)
	// test flavour case_sensitive_like
	testFla := &model.Flavour{
		Name:      "testFla",
		ClusterID: "id1",
		CPU:       "4",
		Mem:       "8Gi",
		ScalarResources: map[schema.ResourceName]string{
			"nvidia.com/gpu": "1",
		},
	}
	storage.Flavour.DeleteFlavour("testFla")
	err := storage.Flavour.CreateFlavour(testFla)
	assert.Nil(t, err)
	// inference  https://stackoverflow.com/questions/15480319/case-sensitive-and-insensitive-like-in-sqlite
	ret, err := storage.Flavour.ListFlavour(0, 1, "", "testFla")
	assert.Nil(t, err)
	assert.Equal(t, len(ret), 1)
	ret, err = storage.Flavour.ListFlavour(0, 1, "", "TESTFLA")
	assert.Nil(t, err)
	assert.Equal(t, len(ret), 0)

}

func TestCluster(t *testing.T) {
	gf := getGormConf("")
	db := initSQLiteDB(gf)
	storage.InitStores(db)
	clusterInfo := &model.ClusterInfo{
		Name: "cluster",
	}
	err := storage.Cluster.CreateCluster(clusterInfo)
	assert.Nil(t, err)
	err = storage.Cluster.CreateCluster(clusterInfo)
	assert.Error(t, err)

}
