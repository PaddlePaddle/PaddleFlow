package pipeline

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
)

func loadcase(casePath string) []byte {
	data, err := ioutil.ReadFile(casePath)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	return data
}

var runID string = "stepTestRunID"

// 测试updateJob接口（用于计算fingerprint）
func TestUpdateJobForFingerPrint(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, runID, "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	sortedSteps, err := wf.topologicalSort(wf.runtimeSteps)
	assert.Nil(t, err)

	for _, stepName := range sortedSteps {
		st := wf.runtime.entryPoints[stepName]
		st.nodeType = common.NodeTypeEntrypoint
		forCacheFingerprint := true
		err := st.updateJob(forCacheFingerprint, nil)
		assert.Nil(t, err)

		if stepName == "data-preprocess" {
			assert.Equal(t, 2, len(st.job.Job().Parameters))

			fmt.Println(st.job.Job().Env)
			assert.Equal(t, 2+6+2, len(st.job.Job().Env)) // 2 env + 6 sys param + 2 artifact

			assert.Contains(t, st.job.Job().Artifacts.Output, "train_data")
			assert.Contains(t, st.job.Job().Artifacts.Output, "validate_data")
			assert.Equal(t, "", st.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, "", st.job.Job().Artifacts.Output["validate_data"])

			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_VALIDATE_DATA")
			assert.Equal(t, "", st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, "", st.job.Job().Env["PF_OUTPUT_ARTIFACT_VALIDATE_DATA"])

			expectedCommand := "python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre --validate {{ validate_data }} --stepname data-preprocess"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
		if stepName == "main" {
			assert.Equal(t, 7, len(st.job.Job().Parameters))
			assert.Equal(t, "./data/pre", st.job.Job().Parameters["data_file"])
			assert.Equal(t, "dictparam", st.job.Job().Parameters["p3"])
			assert.Equal(t, "0.66", st.job.Job().Parameters["p4"])
			assert.Equal(t, "/path/to/anywhere", st.job.Job().Parameters["p5"])

			assert.Equal(t, 5+6+2, len(st.job.Job().Env)) // 5 env + 6 sys param + 2 artifact

			// input artifact 替换为上游节点的output artifact
			// 实际运行中上游节点的output artifact一定是非空的（因为已经运行了），但是在这个测试case里，上游节点没有生成output artifact，所以是空字符串
			assert.Contains(t, st.job.Job().Artifacts.Input, "train_data")
			assert.Equal(t, "", st.job.Job().Artifacts.Input["train_data"])

			assert.Contains(t, st.job.Job().Artifacts.Output, "train_model")
			assert.Equal(t, "", st.job.Job().Artifacts.Output["train_model"])

			assert.Contains(t, st.job.Job().Env, "PF_INPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_MODEL")
			assert.Equal(t, "", st.job.Job().Env["PF_INPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, "", st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_MODEL"])

			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
		if stepName == "validate" {
			assert.Equal(t, 2, len(st.job.Job().Parameters))
			assert.Contains(t, st.job.Job().Parameters, "refSystem")
			assert.Equal(t, runID, st.job.Job().Parameters["refSystem"])

			assert.Equal(t, 4+6+2, len(st.job.Job().Env)) // 4 env + 6 sys param + 2 artifact
			assert.Contains(t, st.job.Job().Env, "PF_JOB_QUEUE")
			assert.Contains(t, st.job.Job().Env, "PF_JOB_PRIORITY")
			assert.Contains(t, st.job.Job().Env, "test_env_1")
			assert.Contains(t, st.job.Job().Env, "test_env_2")
			assert.Equal(t, "CPU-32G", st.job.Job().Env["PF_JOB_QUEUE"])
			assert.Equal(t, "low", st.job.Job().Env["PF_JOB_PRIORITY"])
			assert.Equal(t, "./data/report", st.job.Job().Env["test_env_1"])
			assert.Equal(t, "./data/pre_validate", st.job.Job().Env["test_env_2"])

			assert.Contains(t, st.job.Job().Artifacts.Input, "data")
			assert.Equal(t, "", st.job.Job().Artifacts.Input["data"])

			assert.Contains(t, st.job.Job().Artifacts.Input, "model")
			assert.Equal(t, "", st.job.Job().Artifacts.Input["model"])

			expectedCommand := "python validate.py --model ./data/model --report ./data/report"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
	}
}

// // 测试updateJob接口（cache命中失败后，替换用于节点运行）
func TestUpdateJob(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "stepTestRunID", "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	sortedSteps, err := wf.topologicalSort(wf.runtimeSteps)
	assert.Nil(t, err)

	for _, stepName := range sortedSteps {
		st := wf.runtime.entryPoints[stepName]

		st.nodeType = common.NodeTypeEntrypoint

		forCacheFingerprint := false
		err := st.updateJob(forCacheFingerprint, nil)
		assert.Nil(t, err)

		OutatfTrainData := "./.pipeline/stepTestRunID/myproject/data-preprocess/train_data"
		OutatfValidateData := "./.pipeline/stepTestRunID/myproject/data-preprocess/validate_data"
		OutatfTrainModel := "./.pipeline/stepTestRunID/myproject/main/train_model"
		if stepName == "data-preprocess" {
			assert.Equal(t, 2, len(st.job.Job().Parameters))

			fmt.Println(st.job.Job().Env)
			assert.Equal(t, 2+6+2, len(st.job.Job().Env)) // 4 env + 6 sys param + 2 artifact

			assert.Contains(t, st.job.Job().Artifacts.Output, "train_data")
			assert.Contains(t, st.job.Job().Artifacts.Output, "validate_data")
			assert.Equal(t, OutatfTrainData, st.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, OutatfValidateData, st.job.Job().Artifacts.Output["validate_data"])

			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_VALIDATE_DATA")
			assert.Equal(t, OutatfTrainData, st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, OutatfValidateData, st.job.Job().Env["PF_OUTPUT_ARTIFACT_VALIDATE_DATA"])

			expectedCommand := fmt.Sprintf("python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre --validate %s --stepname data-preprocess", OutatfValidateData)
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
		if stepName == "main" {
			assert.Equal(t, 7, len(st.job.Job().Parameters))
			assert.Contains(t, st.job.Job().Parameters, "data_file")
			assert.Equal(t, "./data/pre", st.job.Job().Parameters["data_file"])
			assert.Equal(t, "dictparam", st.job.Job().Parameters["p3"])
			assert.Equal(t, "0.66", st.job.Job().Parameters["p4"])
			assert.Equal(t, "/path/to/anywhere", st.job.Job().Parameters["p5"])

			assert.Equal(t, 5+6+2, len(st.job.Job().Env)) // 5 env + 6 sys param + 2 artifact

			// input artifact 替换为上游节点的output artifact
			// 实际运行中上游节点的output artifact一定是非空的（因为已经运行了），但是在这个测试case里，上游节点没有生成output artifact，所以是空字符串
			assert.Contains(t, st.job.Job().Artifacts.Input, "train_data")
			assert.Equal(t, OutatfTrainData, st.job.Job().Artifacts.Input["train_data"])

			assert.Contains(t, st.job.Job().Artifacts.Output, "train_model")
			assert.Equal(t, OutatfTrainModel, st.job.Job().Artifacts.Output["train_model"])

			assert.Contains(t, st.job.Job().Env, "PF_INPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_MODEL")
			assert.Equal(t, OutatfTrainData, st.job.Job().Env["PF_INPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, OutatfTrainModel, st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_MODEL"])

			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
		if stepName == "validate" {
			assert.Equal(t, 2, len(st.job.Job().Parameters))
			assert.Contains(t, st.job.Job().Parameters, "refSystem")
			assert.Equal(t, runID, st.job.Job().Parameters["refSystem"])

			assert.Equal(t, 4+6+2, len(st.job.Job().Env)) // 4 env + 6 sys param + 2 artifact
			assert.Contains(t, st.job.Job().Env, "PF_JOB_QUEUE")
			assert.Contains(t, st.job.Job().Env, "PF_JOB_PRIORITY")
			assert.Contains(t, st.job.Job().Env, "test_env_1")
			assert.Contains(t, st.job.Job().Env, "test_env_2")
			assert.Equal(t, "CPU-32G", st.job.Job().Env["PF_JOB_QUEUE"])
			assert.Equal(t, "low", st.job.Job().Env["PF_JOB_PRIORITY"])
			assert.Equal(t, "./data/report", st.job.Job().Env["test_env_1"])
			assert.Equal(t, "./data/pre_validate", st.job.Job().Env["test_env_2"])

			assert.Contains(t, st.job.Job().Artifacts.Input, "data")
			assert.Equal(t, OutatfValidateData, st.job.Job().Artifacts.Input["data"])

			assert.Contains(t, st.job.Job().Artifacts.Input, "model")
			assert.Equal(t, OutatfTrainModel, st.job.Job().Artifacts.Input["model"])

			expectedCommand := "python validate.py --model ./data/model --report ./data/report"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
	}
}

// 测试updateJob接口（根据cache命中后的artifact路径）
func TestUpdateJobWithCache(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "stepTestRunID", "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	sortedSteps, err := wf.topologicalSort(wf.runtimeSteps)
	assert.Nil(t, err)

	cacheOutputArtifacts := make(map[string]string)
	cacheOutatfTrainData := "./example/path/to/train_data"
	cacheOutatfValidateData := "./example/path/to/validate_data"
	cacheOutputArtifacts["train_data"] = cacheOutatfTrainData
	cacheOutputArtifacts["validate_data"] = cacheOutatfValidateData
	for _, stepName := range sortedSteps {
		st := wf.runtime.entryPoints[stepName]

		st.nodeType = common.NodeTypeEntrypoint

		forCacheFingerprint := false
		if stepName == "data-preprocess" {
			err := st.updateJob(forCacheFingerprint, cacheOutputArtifacts)
			assert.Nil(t, err)
		} else {
			err := st.updateJob(forCacheFingerprint, nil)
			assert.Nil(t, err)
		}

		OutatfTrainModel := "./.pipeline/stepTestRunID/myproject/main/train_model"
		if stepName == "data-preprocess" {
			assert.Equal(t, 2, len(st.job.Job().Parameters))

			fmt.Println(st.job.Job().Env)
			assert.Equal(t, 2+6+2, len(st.job.Job().Env)) // 4 env + 6 sys param + 2 artifact

			assert.Contains(t, st.job.Job().Artifacts.Output, "train_data")
			assert.Contains(t, st.job.Job().Artifacts.Output, "validate_data")
			assert.Equal(t, cacheOutatfTrainData, st.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, cacheOutatfValidateData, st.job.Job().Artifacts.Output["validate_data"])

			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_VALIDATE_DATA")
			assert.Equal(t, cacheOutatfTrainData, st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, cacheOutatfValidateData, st.job.Job().Env["PF_OUTPUT_ARTIFACT_VALIDATE_DATA"])

			expectedCommand := fmt.Sprintf("python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre --validate %s --stepname data-preprocess", cacheOutatfValidateData)
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
		if stepName == "main" {
			assert.Equal(t, 7, len(st.job.Job().Parameters))
			assert.Equal(t, "./data/pre", st.job.Job().Parameters["data_file"])
			assert.Equal(t, "dictparam", st.job.Job().Parameters["p3"])
			assert.Equal(t, "0.66", st.job.Job().Parameters["p4"])
			assert.Equal(t, "/path/to/anywhere", st.job.Job().Parameters["p5"])

			assert.Equal(t, 5+6+2, len(st.job.Job().Env)) // 5 env + 6 sys param + 2 artifact

			// input artifact 替换为上游节点的output artifact
			// 实际运行中上游节点的output artifact一定是非空的（因为已经运行了），但是在这个测试case里，上游节点没有生成output artifact，所以是空字符串
			assert.Contains(t, st.job.Job().Artifacts.Input, "train_data")
			assert.Equal(t, cacheOutatfTrainData, st.job.Job().Artifacts.Input["train_data"])

			assert.Contains(t, st.job.Job().Artifacts.Output, "train_model")
			assert.Equal(t, OutatfTrainModel, st.job.Job().Artifacts.Output["train_model"])

			assert.Contains(t, st.job.Job().Env, "PF_INPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, st.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_MODEL")
			assert.Equal(t, cacheOutatfTrainData, st.job.Job().Env["PF_INPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, OutatfTrainModel, st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_MODEL"])

			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
		if stepName == "validate" {
			assert.Equal(t, 2, len(st.job.Job().Parameters))
			assert.Contains(t, st.job.Job().Parameters, "refSystem")
			assert.Equal(t, runID, st.job.Job().Parameters["refSystem"])

			assert.Equal(t, 4+6+2, len(st.job.Job().Env)) // 4 env + 5 sys param + 2 artifact
			assert.Contains(t, st.job.Job().Env, "PF_JOB_QUEUE")
			assert.Contains(t, st.job.Job().Env, "PF_JOB_PRIORITY")
			assert.Contains(t, st.job.Job().Env, "test_env_1")
			assert.Contains(t, st.job.Job().Env, "test_env_2")
			assert.Equal(t, "CPU-32G", st.job.Job().Env["PF_JOB_QUEUE"])
			assert.Equal(t, "low", st.job.Job().Env["PF_JOB_PRIORITY"])
			assert.Equal(t, "./data/report", st.job.Job().Env["test_env_1"])
			assert.Equal(t, "./data/pre_validate", st.job.Job().Env["test_env_2"])

			assert.Contains(t, st.job.Job().Artifacts.Input, "data")
			assert.Equal(t, cacheOutatfValidateData, st.job.Job().Artifacts.Input["data"])

			assert.Contains(t, st.job.Job().Artifacts.Input, "model")
			assert.Equal(t, OutatfTrainModel, st.job.Job().Artifacts.Input["model"])

			expectedCommand := "python validate.py --model ./data/model --report ./data/report"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
		}
	}
}

// 测试checkCached接口（用于计算fingerprint）
func TestCheckCached(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	mockCbs.GetJobCb = func(runID string, stepName string) (schema.JobView, error) {
		outAtfs := map[string]string{
			"train_data":    "way/to/train_data",
			"validate_data": "way/to/validate_data",
		}
		return schema.JobView{Artifacts: schema.Artifacts{Output: outAtfs}}, nil
	}

	// first fingerprint 查询返回为空
	mockCbs.ListCacheCb = func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{}, nil
	}

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, runID, "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	st := wf.runtime.entryPoints["data-preprocess"]

	st.nodeType = common.NodeTypeEntrypoint
	patches := gomonkey.ApplyMethod(reflect.TypeOf(st.job), "Validate", func(_ *PaddleFlowJob) error {
		return nil
	})
	defer patches.Reset()

	cacheCaculator, err := NewCacheCalculator(*st, wfs.Cache)
	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(cacheCaculator), "CalculateFirstFingerprint", func(_ *conservativeCacheCalculator) (string, error) {
		return "1111", nil
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(cacheCaculator), "CalculateSecondFingerprint", func(_ *conservativeCacheCalculator) (string, error) {
		return "2222", nil
	})
	defer patch2.Reset()

	cacheFound, err := st.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，但是second fingerprint不一致
	updateTime := time.Now().Add(time.Second * time.Duration(-1*100))
	mockCbs.ListCacheCb = func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "3333", RunID: "run-000027", UpdatedAt: updateTime, ExpiredTime: "-1"},
		}, nil
	}

	wf, err = NewWorkflow(wfs, runID, "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	st = wf.runtime.entryPoints["data-preprocess"]
	st.nodeType = common.NodeTypeEntrypoint

	cacheFound, err = st.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，但是cache已经过时
	updateTime = time.Now().Add(time.Second * time.Duration(-1*500))
	mockCbs.ListCacheCb = func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", UpdatedAt: updateTime, ExpiredTime: "300"},
		}, nil
	}

	wf, err = NewWorkflow(wfs, runID, "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	st = wf.runtime.entryPoints["data-preprocess"]

	st.nodeType = common.NodeTypeEntrypoint
	cacheFound, err = st.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，且命中expired time为-1的cache记录
	updateTime = time.Now().Add(time.Second * time.Duration(-1*100))
	mockCbs.ListCacheCb = func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", UpdatedAt: updateTime, ExpiredTime: "-1"},
		}, nil
	}

	wf, err = NewWorkflow(wfs, runID, "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	st = wf.runtime.entryPoints["data-preprocess"]

	st.nodeType = common.NodeTypeEntrypoint
	cacheFound, err = st.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, true, cacheFound)

	// first fingerprint 查询返回非空，且命中expired time不为-1，但依然有效的cache记录
	updateTime = time.Now().Add(time.Second * time.Duration(-1*100))
	mockCbs.ListCacheCb = func(firstFp, fsID, step, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", UpdatedAt: updateTime, ExpiredTime: "300"},
		}, nil
	}

	wf, err = NewWorkflow(wfs, runID, "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	st = wf.runtime.entryPoints["data-preprocess"]
	st.nodeType = common.NodeTypeEntrypoint
	cacheFound, err = st.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, true, cacheFound)
}

func TestPFRUNTIME(t *testing.T) {
	yamlByte := loadcase(runYamlPath)
	wfs, err := schema.ParseWorkflowSource(yamlByte)
	assert.Nil(t, err)

	extra := GetExtra()
	wf, err := NewWorkflow(wfs, "stepTestRunID", "", nil, extra, mockCbs)
	if err != nil {
		t.Errorf("new workflow failed: %s", err.Error())
	}

	wf.runtime.runtimeView = schema.RuntimeView{
		"data-preprocess": schema.JobView{
			JobID: "123",
			Env: map[string]string{
				"PF_RUN_ID": "00001",
				"name1":     "name1",
			},
		},
		"main": schema.JobView{
			JobID: "3456",
		},
		"validate": schema.JobView{
			JobID: "9087",
		},
	}

	// entryPoints
	st := wf.runtime.entryPoints["data-preprocess"]
	st.nodeType = common.NodeTypeEntrypoint
	st.updateJob(false, nil)

	runtimeString := st.job.(*PaddleFlowJob).Env["PF_RUN_TIME"]
	fmt.Println("runtimeString", runtimeString)
	assert.Equal(t, "{}", st.job.Job().Env["PF_RUN_TIME"])

	st = wf.runtime.entryPoints["main"]
	st.nodeType = common.NodeTypeEntrypoint
	st.updateJob(false, nil)
	runtime := schema.RuntimeView{}
	err = json.Unmarshal([]byte(st.job.Job().Env["PF_RUN_TIME"]), &runtime)
	if err != nil {
		t.Errorf("unmarshal runtime failed: %s", err.Error())
	}
	assert.Equal(t, 1, len(runtime))
	assert.Equal(t, runtime["data-preprocess"].Env["name1"], "name1")

	_, ok := runtime["data-preprocess"].Env["PF_RUN_ID"]
	fmt.Println("PF_RUN_ID_test", runtime["data-preprocess"].Env["PF_RUN_ID"])
	assert.Equal(t, ok, false)

	st = wf.runtime.entryPoints["validate"]
	st.nodeType = common.NodeTypeEntrypoint
	st.updateJob(false, nil)
	runtime = schema.RuntimeView{}
	err = json.Unmarshal([]byte(st.job.Job().Env["PF_RUN_TIME"]), &runtime)
	if err != nil {
		t.Errorf("unmarshal runtime failed: %s", err.Error())
	}
	assert.Equal(t, 2, len(runtime))

	_, ok = runtime["data-preprocess"]
	assert.Equal(t, ok, true)

	_, ok = runtime["main"]
	assert.Equal(t, ok, true)

	// postProcess
	assert.Equal(t, 1, len(wf.Source.PostProcess))
	for name, st := range wf.runtime.postProcess {
		st.nodeType = common.NodeTypePostProcess

		assert.Equal(t, name, "mail")

		st.updateJob(false, nil)
		assert.Equal(t, true, strings.Contains(st.job.Job().Command, "hahaha"))

		runtime := schema.RuntimeView{}
		err := json.Unmarshal([]byte(st.job.Job().Env["PF_RUN_TIME"]), &runtime)
		if err != nil {
			t.Errorf("unmarshal runtime failed: %s", err.Error())
		}
		assert.Equal(t, 3, len(runtime))

		_, ok = runtime["data-preprocess"]
		assert.Equal(t, ok, true)

		_, ok = runtime["main"]
		assert.Equal(t, ok, true)

		_, ok = runtime["validate"]
		assert.Equal(t, ok, true)
	}

}
