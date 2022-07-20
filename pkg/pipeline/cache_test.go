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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/config"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/fs"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/stretchr/testify/assert"
	// . "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

func TestCalculateFingerprint(t *testing.T) {
	cacheKey := map[string]string{
		"name":    "xiaodu",
		"command": "echo 124",
	}
	fp, err := calculateFingerprint(&cacheKey)
	fmt.Println(fp)
	assert.Equal(t, err, nil)

	firstCacheKey := conservativeFirstCacheKey{
		DockerEnv:      "test:1",
		Command:        "echo 123",
		Env:            map[string]string{"name": "xiaodu", "value": "123"},
		Parameters:     map[string]string{"name": "xiaodu", "value": "456"},
		InputArtifacts: map[string]string{"model": "/pf/model"},
		ExtraFS: []schema.FsMount{
			schema.FsMount{
				ID:        "123",
				MountPath: "/abc",
			},
			schema.FsMount{
				ID:        "456",
				MountPath: "/def",
			},
		},
	}

	fp2, err := calculateFingerprint(&firstCacheKey)
	fmt.Println(fp2)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, fp, fp2)

	// 测试参数顺序是否会导致fingerPrint 不一致
	firstCacheKey2 := conservativeFirstCacheKey{
		DockerEnv:      "test:1",
		Command:        "echo 123",
		Env:            map[string]string{"value": "123", "name": "xiaodu"},
		Parameters:     map[string]string{"name": "xiaodu", "value": "456"},
		InputArtifacts: map[string]string{"model": "/pf/model"},
		ExtraFS: []schema.FsMount{
			schema.FsMount{
				ID:        "456",
				MountPath: "/abc",
			},
			schema.FsMount{
				ID:        "123",
				MountPath: "/def",
			},
		},
	}

	fp3, err := calculateFingerprint(&firstCacheKey2)
	fmt.Println(fp3)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, fp3, fp2)

	// 测试参数顺序是否会导致fingerPrint 不一致
	firstCacheKey4 := conservativeFirstCacheKey{
		DockerEnv:      "test:1",
		Command:        "echo 123",
		Env:            map[string]string{"value": "123", "name": "xiaodu"},
		Parameters:     map[string]string{"name": "xiaodu", "value": "456"},
		InputArtifacts: map[string]string{"model": "/pf/model"},
		ExtraFS: []schema.FsMount{
			schema.FsMount{
				ID:        "456",
				MountPath: "/abc",
			},
			schema.FsMount{
				ID:        "123",
				MountPath: "/def",
			},
		},
		MainFS: schema.FsMount{
			ID:        "456",
			MountPath: "/abc",
		},
	}

	fp4, err := calculateFingerprint(&firstCacheKey4)
	fmt.Println(fp3)
	assert.NotEqual(t, fp3, fp4)

	// 测试参数顺序是否会导致fingerPrint 不一致
	firstCacheKey5 := conservativeFirstCacheKey{
		DockerEnv:      "test:1",
		Command:        "echo 123",
		Env:            map[string]string{"value": "123", "name": "xiaodu"},
		Parameters:     map[string]string{"name": "xiaodu", "value": "456"},
		InputArtifacts: map[string]string{"model": "/pf/model"},
		ExtraFS: []schema.FsMount{
			schema.FsMount{
				ID:        "456",
				MountPath: "/abc",
			},
			schema.FsMount{
				ID:        "123",
				MountPath: "/def",
			},
		},
		MainFS: schema.FsMount{
			ID:        "456",
			MountPath: "/abcde",
		},
	}

	fp5, err := calculateFingerprint(&firstCacheKey5)
	fmt.Println(fp5)
	assert.NotEqual(t, fp5, fp4)
}

func mockArtifact() schema.Artifacts {
	return schema.Artifacts{
		Input:  map[string]string{"model": "/class/mode", "data": "/data/predict"},
		Output: map[string]string{"result": "/result/predict"},
	}
}

func mockBaseJob() BaseJob {
	arts := mockArtifact()

	env := map[string]string{
		"num": "1200",
	}
	return BaseJob{
		ID:         "1234",
		Name:       "run1-predict",
		Command:    "python3 predict.py /class/model",
		Parameters: map[string]string{"epoch": "1", "batch_size": "128"},
		Env:        env,
		StartTime:  "2021-11-11:00:00:11",
		EndTime:    "2021-11-21:00:00:11",
		Status:     "init",
		Artifacts:  arts,
	}
}

func mockPaddleFlowJob() PaddleFlowJob {
	baseJob := mockBaseJob()
	return PaddleFlowJob{
		BaseJob: baseJob,
		Image:   "test:1",
	}
}

func mockWorkflowSourceStep() schema.WorkflowSourceStep {
	art := mockArtifact()
	return schema.WorkflowSourceStep{
		Parameters: map[string]interface{}{"epoch": "1", "batch_size": "128908"},
		Command:    "python3 predict.py 1234",
		Deps:       "dataProcess",
		Env:        map[string]string{"num": "1200"},
		Artifacts:  art,
		DockerEnv:  "test.tar",
		ExtraFS: []schema.FsMount{
			schema.FsMount{
				ID:        "456",
				MountPath: "/abc",
			},
			schema.FsMount{
				ID:        "123",
				MountPath: "/abc",
			},
		},
	}
}

func mockRunConfigWithLogger() *runConfig {
	mainFS := schema.FsMount{
		ID: "1234",
	}
	return &runConfig{
		logger: logger.LoggerForRun("run-0000"),
		mainFS: &mainFS,
	}
}

func mockStep() StepRuntime {
	job := mockPaddleFlowJob()
	step := mockWorkflowSourceStep()
	bcr := baseComponentRuntime{
		componentFullName: "predict.defe-1",
		runConfig:         mockRunConfigWithLogger(),
		component:         &step,
	}

	return StepRuntime{
		baseComponentRuntime: &bcr,
		job:                  &job,
	}
}

func mockCacheConfig() schema.Cache {
	return schema.Cache{
		Enable:         true,
		MaxExpiredTime: "167873037492",
		FsScope: []schema.FsScope{
			schema.FsScope{
				ID:   "123",
				Name: "abc",
				Path: "a.txt,b.txt",
			},
			schema.FsScope{
				ID:   "456",
				Name: "abc",
				Path: "c.txt,d.txt",
			},
			schema.FsScope{
				ID:   "789",
				Name: "abc",
				Path: "",
			},
		},
	}
}

func mockerNewConservativeCacheCalculator() (CacheCalculator, error) {
	ServerConf := &config.ServerConfig{}
	err := config.InitConfigFromYaml(ServerConf, "../../config/server/default/paddleserver.yaml")
	config.GlobalServerConfig = ServerConf

	step := mockStep()
	cacheConfig := mockCacheConfig()
	// TODO: mocker fsHandler ?
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer

	job := step.job.(*PaddleFlowJob)
	calculator, err := NewConservativeCacheCalculator(*job, cacheConfig, step.logger, step.mainFS,
		step.getWorkFlowStep().ExtraFS)
	return calculator, err
}

func TestNewConservativeCacheCalculator(t *testing.T) {
	calculator, err := mockerNewConservativeCacheCalculator()
	assert.Equal(t, err, nil)
	_, ok := calculator.(CacheCalculator)
	assert.Equal(t, ok, true)

	_, ok = calculator.(*conservativeCacheCalculator)
	assert.Equal(t, ok, true)
}

func TestGenerateFirstCacheKey(t *testing.T) {
	calculator, err := mockerNewConservativeCacheCalculator()
	assert.Equal(t, err, nil)

	err = calculator.generateFirstCacheKey()
	assert.Equal(t, err, nil)

	arts := mockArtifact()
	cacheKey := calculator.(*conservativeCacheCalculator).firstCacheKey
	assert.Equal(t, cacheKey.DockerEnv, "test:1")
	assert.Equal(t, cacheKey.Parameters, map[string]string{"epoch": "1", "batch_size": "128"})
	assert.Equal(t, cacheKey.Env, map[string]string{"num": "1200"})
	assert.Equal(t, cacheKey.Command, "python3 predict.py /class/model")
	assert.Equal(t, cacheKey.InputArtifacts, arts.Input)
	assert.Equal(t, cacheKey.OutputArtifacts, arts.Output)

	cacheKeyJson, err := json.Marshal(calculator.(*conservativeCacheCalculator).firstCacheKey)
	if err != nil {
		panic(err)
	}
	assert.Contains(t, string(cacheKeyJson), "456")
}

func TestCalculateFirstFingerprint(t *testing.T) {
	calculator, err := mockerNewConservativeCacheCalculator()
	assert.Equal(t, err, nil)

	fp, err := calculator.CalculateFirstFingerprint()
	assert.Equal(t, err, nil)

	fmt.Println(fp)

	cacheKey := calculator.(*conservativeCacheCalculator).firstCacheKey
	fp2, err := calculateFingerprint(cacheKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, fp, fp2)
}

func CreatefileByFsClient(path string, isDir bool) error {
	testFsMeta := common.FSMeta{
		UfsType: common.LocalType,
		SubPath: "./mock_fs_handler",
	}
	fsClient, err := fs.NewFSClientForTest(testFsMeta)
	if !isDir {
		writerCloser, err := fsClient.Create(path)
		if err != nil {
			return err
		}

		defer writerCloser.Close()

		_, err = writerCloser.Write([]byte("test paddleflow pipeline"))
		return err
	} else {
		err = fsClient.MkdirAll(path, 0777)
		return err
	}
}

func TestGetFsScopeModTime(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	handler.NewFsHandlerWithServer("xx", logger.LoggerForRun("innersolve"))

	calculator, err := mockerNewConservativeCacheCalculator()
	assert.Equal(t, err, nil)

	cacheConfig := mockCacheConfig()
	for _, scope := range cacheConfig.FsScope {
		for _, path := range strings.Split(scope.Path, ",") {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			err := CreatefileByFsClient(path, false)
			assert.Equal(t, err, nil)
		}
	}

	fsScopeMap, err := calculator.(*conservativeCacheCalculator).getFsScopeModTime()
	fmt.Println("fsScopeMap:", fsScopeMap)
	assert.Equal(t, err, nil)

	for _, scope := range cacheConfig.FsScope {
		for _, path := range strings.Split(scope.Path, ",") {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			_, ok := fsScopeMap[scope.ID].ModTime[path]
			assert.Equal(t, ok, true)
		}

		if scope.ID == "789" {
			assert.Len(t, fsScopeMap[scope.ID].ModTime, 1)
			assert.Contains(t, fsScopeMap[scope.ID].ModTime, "/")
		}
	}
}

func TestGetInputArtifactModTime(t *testing.T) {
	arts := mockArtifact()
	calculator, err := mockerNewConservativeCacheCalculator()
	assert.Equal(t, err, nil)

	arts.Input["mutil"] = "m1.txt,m2.txt"
	calculator.(*conservativeCacheCalculator).job.Artifacts.Input["mutil"] = "m1.txt,m2.txt"

	for _, path := range arts.Input {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}

		for _, p := range strings.Split(path, ",") {
			err := CreatefileByFsClient(p, true)
			assert.Equal(t, err, nil)
		}
	}

	inArtMap, err := calculator.(*conservativeCacheCalculator).getInputArtifactModTime()
	fmt.Println("inArtMTime:", inArtMap)
	for name, _ := range arts.Input {
		_, ok := inArtMap[name]
		fmt.Println(name, ok, 123566)
		assert.Equal(t, ok, true)
	}
}

func TestCalculateSecondFingerprint(t *testing.T) {
	arts := mockArtifact()
	calculator, err := mockerNewConservativeCacheCalculator()
	assert.Equal(t, err, nil)

	for _, path := range arts.Input {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		err := CreatefileByFsClient(path, true)
		assert.Equal(t, err, nil)
	}
	inArtMap, err := calculator.(*conservativeCacheCalculator).getInputArtifactModTime()

	cacheConfig := mockCacheConfig()

	for _, scope := range cacheConfig.FsScope {
		for _, path := range strings.Split(scope.Path, ",") {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			err := CreatefileByFsClient(path, false)
			assert.Equal(t, err, nil)
		}
	}

	fsScopeMap, err := calculator.(*conservativeCacheCalculator).getFsScopeModTime()

	fp, err := calculator.CalculateSecondFingerprint()
	assert.Equal(t, err, nil)

	secondCacheKey := calculator.(*conservativeCacheCalculator).secondCacheKey
	assert.Equal(t, secondCacheKey.InputArtifactsModTime, inArtMap)
	assert.Equal(t, secondCacheKey.FsScopeModTime, fsScopeMap)

	fp2, err := calculateFingerprint(secondCacheKey)
	assert.Equal(t, err, nil)
	assert.Equal(t, fp, fp2)
	fmt.Println("SecondFingerPrint:", fp)
}

func TestNewCacheCalculator(t *testing.T) {
	ServerConf := &config.ServerConfig{}
	err := config.InitConfigFromYaml(ServerConf, "../../config/server/default/paddleserver.yaml")
	config.GlobalServerConfig = ServerConf

	step := mockStep()
	cacheConfig := mockCacheConfig()
	// TODO: mocker fsHandler ?
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer

	job := step.job.(*PaddleFlowJob)
	calculator, err := NewCacheCalculator(*job, cacheConfig, step.logger, step.mainFS,
		step.getWorkFlowStep().ExtraFS)
	assert.Equal(t, err, nil)
	_, ok := calculator.(CacheCalculator)
	assert.Equal(t, ok, true)

	_, ok = calculator.(*conservativeCacheCalculator)
	assert.Equal(t, ok, true)
}
