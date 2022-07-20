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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"github.com/sirupsen/logrus"
)

// 用于计算保守策略的第一层 fingerprint 的结构
type conservativeFirstCacheKey struct {
	DockerEnv       string
	Command         string
	Env             map[string]string `json:",omitempty"`
	Parameters      map[string]string `json:",omitempty"`
	InputArtifacts  map[string]string `json:",omitempty"`
	OutputArtifacts map[string]string `json:",omitempty"`
	MainFS          schema.FsMount    `json:",omitempty"`
	ExtraFS         []schema.FsMount  `json:",omitempty"`
}

type PathToModTime struct {
	ModTime map[string]string `json:"omitempty"`
}

// 用于计算保守策略的第二层 fingerprint 的结构
type conservativeSecondCacheKey struct {
	// 输入 artifact 的名字到其内容（modtime）的映射
	InputArtifactsModTime map[string]string `json:",omitempty"`

	// Fs 上的文件名与其 modTime 之间的映射关系
	FsScopeModTime map[string]PathToModTime `json:",omitempty"`
}

func calculateFingerprint(cacheKey interface{}) (fingerprint string, err error) {
	// 错误信息应该由 调用本函数的主体去打印
	formattedCacheKeyBytes, err := json.Marshal(cacheKey)
	if err != nil {
		return "", err
	}

	hash := sha256.New()
	hash.Write(formattedCacheKeyBytes)
	md := hash.Sum(nil)

	fingerprint = hex.EncodeToString(md)
	return fingerprint, nil
}

type CacheCalculator interface {
	// 生成 用于计算 第一层 fingerprint 的 cacheKey
	generateFirstCacheKey() error

	// 生成 用于计算 第二层 fingerprint 的 cacheKey
	generateSecondCacheKey() error

	// 计算第一层 fingerprint
	CalculateFirstFingerprint() (fingerprint string, err error)

	// 计算 第二层 fingerprint
	CalculateSecondFingerprint() (fingerprint string, err error)
}

type conservativeCacheCalculator struct {
	job            PaddleFlowJob
	logger         *logrus.Entry
	extraFS        []schema.FsMount
	mainFS         *schema.FsMount
	cacheConfig    schema.Cache
	firstCacheKey  *conservativeFirstCacheKey
	secondCacheKey *conservativeSecondCacheKey
}

// 调用方应该保证在启用了 cache 功能的情况下才会调用NewConservativeCacheCalculator
func NewConservativeCacheCalculator(job PaddleFlowJob, cacheConfig schema.Cache, logger *logrus.Entry,
	mainFs *schema.FsMount, extraFs []schema.FsMount) (CacheCalculator, error) {
	calculator := conservativeCacheCalculator{
		job:         job,
		cacheConfig: cacheConfig,
		logger:      logger,
		mainFS:      mainFs,
		extraFS:     extraFs,
	}
	return &calculator, nil
}

func (cc *conservativeCacheCalculator) generateFirstCacheKey() error {
	// 提取cacheKey 时需要剔除系统变量
	envWithoutSystmeEnv := common.DeleteSystemParamEnv(cc.job.Env)

	// 去除系统环境变量
	cacheKey := conservativeFirstCacheKey{
		DockerEnv:       cc.job.Image,
		Parameters:      cc.job.Parameters,
		Command:         cc.job.Command,
		InputArtifacts:  cc.job.Artifacts.Input,
		OutputArtifacts: cc.job.Artifacts.Output,
		Env:             envWithoutSystmeEnv,
		ExtraFS:         cc.extraFS,
		MainFS:          *cc.mainFS,
	}

	logMsg := fmt.Sprintf("FirstCacheKey: \nDockerEnv: %s, Parameters: %s, Command: %s, InputArtifacts: %s, "+
		"OutputArtifacts: %s, Env: %s, mainFS: %v, extraFS: %v,  JobName: %s", cc.job.Image, cc.job.Parameters,
		cc.job.Command, cc.job.Artifacts.Input, cc.job.Artifacts.Output, cacheKey.Env,
		cacheKey.MainFS, cacheKey.ExtraFS, cc.job.Name)

	cc.logger.Debugf(logMsg)

	cc.firstCacheKey = &cacheKey
	return nil
}

func (cc *conservativeCacheCalculator) CalculateFirstFingerprint() (fingerprint string, err error) {
	err = cc.generateFirstCacheKey()
	if err != nil {
		err = fmt.Errorf("Calculate FirstFingerprint failed due to generating FirstCacheKey: %s", err.Error())
		cc.logger.Errorln(err.Error())
		return "", err
	}

	firstFingerprint, err := calculateFingerprint(cc.firstCacheKey)
	if err != nil {
		err = fmt.Errorf("Calculate FirstFingerprint failed: %s", err.Error())
		cc.logger.Errorln(err.Error())
		return "", err
	}

	return firstFingerprint, err
}

func (cc *conservativeCacheCalculator) getFsScopeModTime() (map[string]PathToModTime, error) {
	// 注意， FsScope 的合法性需要由调用方保证
	smt := map[string]PathToModTime{}
	for _, scope := range cc.cacheConfig.FsScope {
		cc.logger.Infof("begin to get the modtime of scope: %v", scope)
		fsHandler, err := handler.NewFsHandlerWithServer(scope.ID, cc.logger)
		if err != nil {
			errMsg := fmt.Errorf("init fsHandler failed: %s", err.Error())
			cc.logger.Errorln(errMsg)
			return nil, err
		}

		var pathToMT PathToModTime
		if _, ok := smt[scope.ID]; ok {
			pathToMT = smt[scope.ID]
		} else {
			pathToMT = PathToModTime{ModTime: map[string]string{}}
		}

		FsScope := strings.TrimSpace(scope.Path)
		if FsScope == "" {
			FsScope = "/"
		}

		for _, path := range strings.Split(FsScope, ",") {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}

			mtime, err := fsHandler.LastModTime(path)
			if err != nil {
				err = fmt.Errorf("get the mtime of fsScope file[%s] failed: %s", path, err.Error())
				cc.logger.Errorln(err.Error())
				return nil, err
			}
			pathToMT.ModTime[path] = fmt.Sprintf("%d", mtime.UnixNano())
		}

		smt[scope.ID] = pathToMT
	}
	return smt, nil
}

func (cc *conservativeCacheCalculator) getInputArtifactModTime() (map[string]string, error) {
	if cc.mainFS.ID == "" {
		cc.logger.Info("there must be no input artifact because global fsId is empty")
		return map[string]string{}, nil
	}

	fsHandler, err := handler.NewFsHandlerWithServer(cc.mainFS.ID, cc.logger)
	if err != nil {
		errMsg := fmt.Errorf("init fsHandler failed: %s", err.Error())
		cc.logger.Errorln(errMsg)
		return nil, err
	}

	inArt := cc.job.Artifacts.Input
	inArtMtimeMap := map[string]string{}

	for name, paths := range inArt {
		name = strings.TrimSpace(name)
		lastTime := time.Time{}
		pToMTime := []time.Time{}

		for _, path := range strings.Split(paths, ",") {
			path = strings.TrimSpace(path)

			if name == "" || path == "" {
				err := fmt.Errorf("the input artifact[%s] is illegal, name or path of it is empty", name)
				cc.logger.Errorln(err.Error())
				return map[string]string{}, err
			}

			mtime, err := fsHandler.LastModTime(path)
			if err != nil {
				err = fmt.Errorf("get the mtime of inputArtfact[%s] failed: %s", name, err.Error())
				return map[string]string{}, err
			}

			pToMTime = append(pToMTime, mtime)
		}
		lastTime = common.LatestTime(pToMTime)
		inArtMtimeMap[name] = fmt.Sprintf("%d", lastTime.UnixNano())
	}

	return inArtMtimeMap, nil
}

func (cc *conservativeCacheCalculator) generateSecondCacheKey() error {
	fsScopeMTime, err := cc.getFsScopeModTime()
	if err != nil {
		err := fmt.Errorf("generate SecondCacheKey failed: [%s]", err.Error())
		cc.logger.Errorln(err.Error())
		return err
	}

	inArt, err := cc.getInputArtifactModTime()
	if err != nil {
		err := fmt.Errorf("generate SecondCacheKey failed: [%s]", err.Error())
		cc.logger.Errorln(err.Error())
		return err
	}

	cc.secondCacheKey = &conservativeSecondCacheKey{
		InputArtifactsModTime: inArt,
		FsScopeModTime:        fsScopeMTime,
	}

	logMsg := fmt.Sprintf("SecondCacheKey:\nInputArtMTime: %s, FsScopeMTime: %s", inArt, fsScopeMTime)
	cc.logger.Debugf(logMsg)

	return nil
}

func (cc *conservativeCacheCalculator) CalculateSecondFingerprint() (fingerprint string, err error) {
	err = cc.generateSecondCacheKey()
	if err != nil {
		err = fmt.Errorf("Calculate SecondFingerprint failed due to generating SecondCacheKey failed: %s", err.Error())
		cc.logger.Errorln(err.Error())
		return "", err
	}

	secondFingerprint, err := calculateFingerprint(cc.secondCacheKey)
	if err != nil {
		err = fmt.Errorf("Calculate FirstFingerprint failed: %s", err.Error())
		cc.logger.Errorln(err.Error())
		return "", err
	}

	return secondFingerprint, err
}

// 调用方应该保证在启用了 cache 功能的情况下才会调用NewCacheCalculator
func NewCacheCalculator(job PaddleFlowJob, cacheConfig schema.Cache, logger *logrus.Entry,
	mainFs *schema.FsMount, extraFs []schema.FsMount) (CacheCalculator, error) {
	// TODO: 当支持多中 cache 策略时，做好分发的功能
	return NewConservativeCacheCalculator(job, cacheConfig, logger, mainFs, extraFs)
}
