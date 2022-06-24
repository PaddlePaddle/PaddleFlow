package pipeline

import (
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/models"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
	"github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	pplcommon "github.com/PaddlePaddle/PaddleFlow/pkg/pipeline/common"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
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

const (
	runYamlPath           string = "./testcase/run.yaml"
	noAtfYamlPath         string = "./testcase/runNoAtf.yaml"
	runWrongParamYamlPath string = "./testcase/runWrongParam.yaml"
	runCircleYamlPath     string = "./testcase/runCircle.yaml"
)

// extra map里面的value可能会被修改，从而影响后面的case
// 为了避免上面的问题，封装成函数，不同case分别调用函数，获取全新的extra map
func GetExtra() map[string]string {
	var extra = map[string]string{
		pplcommon.WfExtraInfoKeySource:   "./testcase/run.yaml",
		pplcommon.WfExtraInfoKeyFsID:     "mockFsID",
		pplcommon.WfExtraInfoKeyFsName:   "mockFsname",
		pplcommon.WfExtraInfoKeyUserName: "mockUser",
	}

	return extra
}

var mockCbs = WorkflowCallbacks{
	UpdateRuntimeCb: func(runID string, event interface{}) (int64, bool) {
		fmt.Println("UpdateRunCb: ", event)
		return 1, true
	},
	LogCacheCb: func(req schema.LogRunCacheRequest) (string, error) {
		return "cch-000027", nil
	},
	ListCacheCb: func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{models.RunCache{RunID: "run-000027", JobID: "job-1"},
			models.RunCache{RunID: "run-000028", JobID: "job-2"}}, nil
	},
}

// 测试updateJob接口（用于计算fingerprint）
func TestUpdateJobForFingerPrint(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	rf := mockRunConfigForComponentRuntime()

	sortedSteps, err := common.TopologicalSort(wfs.EntryPoints.EntryPoints)
	assert.Nil(t, err)

	failctx, _ := context.WithCancel(context.Background())
	for _, stepName := range sortedSteps {
		st := wfs.EntryPoints.EntryPoints[stepName].(*schema.WorkflowSourceStep)
		srt := NewStepRuntime(st.Name, st, 0, context.Background(), failctx,
			make(chan<- WorkflowEvent), rf, "dag-11")
		forCacheFingerprint := true
		err := srt.updateJob(forCacheFingerprint)
		assert.Nil(t, err)

		if stepName == "data-preprocess" {
			assert.Equal(t, 2, len(srt.job.Job().Parameters))

			fmt.Println(srt.job.Job().Env)
			assert.Equal(t, 2, len(srt.job.Job().Env)) // 2 env

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_data")
			assert.Contains(t, srt.job.Job().Artifacts.Output, "validate_data")
			assert.Equal(t, "", srt.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, "", srt.job.Job().Artifacts.Output["validate_data"])

			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_VALIDATE_DATA")
			assert.Equal(t, "", srt.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, "", srt.job.Job().Env["PF_OUTPUT_ARTIFACT_VALIDATE_DATA"])

			expectedCommand := "python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre --validate {{ validate_data }} --stepname data-preprocess"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "main" {
			assert.Equal(t, 7, len(srt.job.Job().Parameters))
			assert.Equal(t, "./data/pre", srt.job.Job().Parameters["data_file"])
			assert.Equal(t, "dictparam", srt.job.Job().Parameters["p3"])
			assert.Equal(t, "0.66", srt.job.Job().Parameters["p4"])
			assert.Equal(t, "/path/to/anywhere", srt.job.Job().Parameters["p5"])

			assert.Equal(t, 5, len(srt.job.Job().Env)) // 5 env

			// input artifact 替换为上游节点的output artifact
			// 实际运行中上游节点的output artifact一定是非空的（因为已经运行了），但是在这个测试case里，上游节点没有生成output artifact，所以是空字符串
			assert.Contains(t, srt.job.Job().Artifacts.Input, "train_data")
			assert.Equal(t, "", srt.job.Job().Artifacts.Input["train_data"])

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_model")
			assert.Equal(t, "", srt.job.Job().Artifacts.Output["train_model"])

			assert.Contains(t, srt.job.Job().Env, "PF_INPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_MODEL")
			assert.Equal(t, "", srt.job.Job().Env["PF_INPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, "", srt.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_MODEL"])

			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "validate" {
			assert.Equal(t, 2, len(srt.job.Job().Parameters))
			assert.Contains(t, srt.job.Job().Parameters, "refSystem")
			assert.Equal(t, runID, srt.job.Job().Parameters["refSystem"])

			assert.Equal(t, 4, len(srt.job.Job().Env)) // 4 env
			assert.Contains(t, srt.job.Job().Env, "PF_JOB_QUEUE")
			assert.Contains(t, srt.job.Job().Env, "PF_JOB_PRIORITY")
			assert.Contains(t, srt.job.Job().Env, "test_env_1")
			assert.Contains(t, srt.job.Job().Env, "test_env_2")
			assert.Equal(t, "CPU-32G", srt.job.Job().Env["PF_JOB_QUEUE"])
			assert.Equal(t, "low", srt.job.Job().Env["PF_JOB_PRIORITY"])
			assert.Equal(t, "./data/report", srt.job.Job().Env["test_env_1"])
			assert.Equal(t, "./data/pre_validate", srt.job.Job().Env["test_env_2"])

			assert.Contains(t, srt.job.Job().Artifacts.Input, "data")
			assert.Equal(t, "", srt.job.Job().Artifacts.Input["data"])

			assert.Contains(t, srt.job.Job().Artifacts.Input, "model")
			assert.Equal(t, "", srt.job.Job().Artifacts.Input["model"])

			expectedCommand := "python validate.py --model ./data/model --report ./data/report"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
	}
}

// // 测试updateJob接口（cache命中失败后，替换用于节点运行）
func TestUpdateJob(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	sortedSteps, err := common.TopologicalSort(wfs.EntryPoints.EntryPoints)
	assert.Nil(t, err)
	rf := mockRunConfigForComponentRuntime()
	sysNum := len(common.SysParamNameList)

	failctx, _ := context.WithCancel(context.Background())

	for _, stepName := range sortedSteps {
		st := wfs.EntryPoints.EntryPoints[stepName].(*schema.WorkflowSourceStep)
		srt := NewStepRuntime(st.Name, st, 0, context.Background(), failctx,
			make(chan<- WorkflowEvent), rf, "dag-11")
		forCacheFingerprint := false
		err := srt.updateJob(forCacheFingerprint)

		assert.Nil(t, err)

		OutatfTrainData := "./.pipeline/stepTestRunID/myproject/data-preprocess/train_data"
		OutatfValidateData := "./.pipeline/stepTestRunID/myproject/data-preprocess/validate_data"
		OutatfTrainModel := "./.pipeline/stepTestRunID/myproject/main/train_model"
		if stepName == "data-preprocess" {
			assert.Equal(t, 2, len(srt.job.Job().Parameters))

			fmt.Println(srt.job.Job().Env)
			assert.Equal(t, 2+sysNum+2, len(srt.job.Job().Env)) // 4 env + 6 sys param + 2 artifact

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_data")
			assert.Contains(t, srt.job.Job().Artifacts.Output, "validate_data")
			assert.Equal(t, OutatfTrainData, srt.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, OutatfValidateData, srt.job.Job().Artifacts.Output["validate_data"])

			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_VALIDATE_DATA")
			assert.Equal(t, OutatfTrainData, srt.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, OutatfValidateData, srt.job.Job().Env["PF_OUTPUT_ARTIFACT_VALIDATE_DATA"])

			expectedCommand := fmt.Sprintf("python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre --validate %s --stepname data-preprocess", OutatfValidateData)
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "main" {
			assert.Equal(t, 7, len(srt.job.Job().Parameters))
			assert.Contains(t, srt.job.Job().Parameters, "data_file")
			assert.Equal(t, "./data/pre", srt.job.Job().Parameters["data_file"])
			assert.Equal(t, "dictparam", srt.job.Job().Parameters["p3"])
			assert.Equal(t, "0.66", srt.job.Job().Parameters["p4"])
			assert.Equal(t, "/path/to/anywhere", srt.job.Job().Parameters["p5"])

			assert.Equal(t, 5+6+2, len(srt.job.Job().Env)) // 5 env + 6 sys param + 2 artifact

			// input artifact 替换为上游节点的output artifact
			// 实际运行中上游节点的output artifact一定是非空的（因为已经运行了），但是在这个测试case里，上游节点没有生成output artifact，所以是空字符串
			assert.Contains(t, srt.job.Job().Artifacts.Input, "train_data")
			assert.Equal(t, OutatfTrainData, srt.job.Job().Artifacts.Input["train_data"])

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_model")
			assert.Equal(t, OutatfTrainModel, srt.job.Job().Artifacts.Output["train_model"])

			assert.Contains(t, srt.job.Job().Env, "PF_INPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_MODEL")
			assert.Equal(t, OutatfTrainData, srt.job.Job().Env["PF_INPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, OutatfTrainModel, srt.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_MODEL"])

			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "validate" {
			assert.Equal(t, 2, len(srt.job.Job().Parameters))
			assert.Contains(t, srt.job.Job().Parameters, "refSystem")
			assert.Equal(t, runID, srt.job.Job().Parameters["refSystem"])

			assert.Equal(t, 4+sysNum+2, len(srt.job.Job().Env)) // 4 env + 6 sys param + 2 artifact
			assert.Contains(t, srt.job.Job().Env, "PF_JOB_QUEUE")
			assert.Contains(t, srt.job.Job().Env, "PF_JOB_PRIORITY")
			assert.Contains(t, srt.job.Job().Env, "test_env_1")
			assert.Contains(t, srt.job.Job().Env, "test_env_2")
			assert.Equal(t, "CPU-32G", srt.job.Job().Env["PF_JOB_QUEUE"])
			assert.Equal(t, "low", srt.job.Job().Env["PF_JOB_PRIORITY"])
			assert.Equal(t, "./data/report", srt.job.Job().Env["test_env_1"])
			assert.Equal(t, "./data/pre_validate", srt.job.Job().Env["test_env_2"])

			assert.Contains(t, srt.job.Job().Artifacts.Input, "data")
			assert.Equal(t, OutatfValidateData, srt.job.Job().Artifacts.Input["data"])

			assert.Contains(t, srt.job.Job().Artifacts.Input, "model")
			assert.Equal(t, OutatfTrainModel, srt.job.Job().Artifacts.Input["model"])

			expectedCommand := "python validate.py --model ./data/model --report ./data/report"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
	}
}

// 测试checkCached接口（用于计算fingerprint）
func TestCheckCached(t *testing.T) {
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	mockCbs.GetJobCb = func(runID string, stepName string) (schema.JobView, error) {
		outAtfs := map[string]string{
			"train_data":    "way/to/train_data",
			"validate_data": "way/to/validate_data",
		}
		return schema.JobView{Artifacts: schema.Artifacts{Output: outAtfs}}, nil
	}

	// first fingerprint 查询返回为空
	mockCbs.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{}, nil
	}

	rf := mockRunConfigForComponentRuntime()

	failctx, _ := context.WithCancel(context.Background())

	st := wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt := NewStepRuntime(st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")

	patches := gomonkey.ApplyMethod(reflect.TypeOf(srt.job), "Validate", func(_ *PaddleFlowJob) error {
		return nil
	})
	defer patches.Reset()

	cacheCaculator, err := NewCacheCalculator(*srt, wfs.Cache)
	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(cacheCaculator), "CalculateFirstFingerprint", func(_ *conservativeCacheCalculator) (string, error) {
		return "1111", nil
	})
	defer patch1.Reset()

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(cacheCaculator), "CalculateSecondFingerprint", func(_ *conservativeCacheCalculator) (string, error) {
		return "2222", nil
	})
	defer patch2.Reset()

	cacheFound, err := srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，但是second fingerprint不一致
	updateTime := time.Now().Add(time.Second * time.Duration(-1*100))
	mockCbs.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "3333", RunID: "run-000027", JobID: "job-xxx",
				UpdatedAt: updateTime, ExpiredTime: "-1"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime(st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")

	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，但是cache已经过时
	updateTime = time.Now().Add(time.Second * time.Duration(-1*500))
	mockCbs.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", UpdatedAt: updateTime, ExpiredTime: "300"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime(st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")
	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，且命中expired time为-1的cache记录
	updateTime = time.Now().Add(time.Second * time.Duration(-1*100))
	mockCbs.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", JobID: "job-001",
				UpdatedAt: updateTime, ExpiredTime: "-1"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime(st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")

	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, true, cacheFound)

	// first fingerprint 查询返回非空，且命中expired time不为-1，但依然有效的cache记录
	updateTime = time.Now().Add(time.Second * time.Duration(-1*100))
	mockCbs.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", JobID: "job-001",
				UpdatedAt: updateTime, ExpiredTime: "300"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime(st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")
	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, true, cacheFound)
}
