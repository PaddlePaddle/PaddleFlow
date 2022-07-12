package pipeline

import (
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"
	"time"

	apicommon "github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/handler"
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
		pplcommon.WfExtraInfoKeyFsID:     "fs-mockUser-mockFs",
		pplcommon.WfExtraInfoKeyFsName:   "mockFs",
		pplcommon.WfExtraInfoKeyUserName: "mockUser",
	}

	return extra
}

var updateRuntimeCalled = false

var LogCacheCbCalled = false

var ListCacheCbCalled = false

var mockCbs = WorkflowCallbacks{
	UpdateRuntimeCb: func(runID string, event interface{}) (int64, bool) {
		fmt.Println("UpdateRunCb: ", event)
		updateRuntimeCalled = true
		return 1, true
	},
	LogCacheCb: func(req schema.LogRunCacheRequest) (string, error) {
		ListCacheCbCalled = true

		return "cch-000027", nil
	},
	ListCacheCb: func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		ListCacheCbCalled = true
		return []models.RunCache{models.RunCache{RunID: "run-000027", JobID: "job-1"},
			models.RunCache{RunID: "run-000028", JobID: "job-2"}}, nil
	},
	LogArtifactCb: func(req schema.LogRunArtifactRequest) error {
		return nil
	},
}

// 测试updateJob接口（用于计算fingerprint）
func TestUpdateJobForFingerPrint(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	extra := GetExtra()
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	sortedSteps, err := common.TopologicalSort(wfs.EntryPoints.EntryPoints)
	assert.Nil(t, err)

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da1", "a.entrypoint.da1", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	ds := NewDependencySolver(dr)
	for _, stepName := range sortedSteps {
		err := ds.ResolveBeforeRun(dr.getworkflowSouceDag().EntryPoints[stepName])
		assert.Nil(t, err)

		st := wfs.EntryPoints.EntryPoints[stepName].(*schema.WorkflowSourceStep)
		srt := NewStepRuntime("a.entrypoint."+st.Name, "a.entrypoint."+st.Name, st, 0, context.Background(), failctx,
			make(chan<- WorkflowEvent), rf, "dag-11")
		srt.setSysParams()

		dr.subComponentRumtimes[stepName] = append(dr.subComponentRumtimes[stepName], srt)
		forCacheFingerprint := true
		err = srt.updateJob(forCacheFingerprint)
		assert.Nil(t, err)

		if stepName == "data-preprocess" {
			assert.Equal(t, 2, len(srt.job.Job().Parameters))

			assert.Equal(t, 7, len(srt.job.Job().Env)) // 2 env

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_data")
			assert.Contains(t, srt.job.Job().Artifacts.Output, "validate_data")
			assert.Equal(t, "", srt.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, "", srt.job.Job().Artifacts.Output["validate_data"])

			expectedCommand := "python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre --validate {{ validate_data }} --stepname data-preprocess"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "main" {
			assert.Equal(t, 7, len(srt.job.Job().Parameters))
			assert.Equal(t, "./data/pre", srt.job.Job().Parameters["data_file"])
			assert.Equal(t, "dictparam", srt.job.Job().Parameters["p3"])
			assert.Equal(t, "0.66", srt.job.Job().Parameters["p4"])
			assert.Equal(t, "/path/to/anywhere", srt.job.Job().Parameters["p5"])

			assert.Equal(t, 5+5, len(srt.job.Job().Env)) // 5 env

			// input artifact 替换为上游节点的output artifact
			// 实际运行中上游节点的output artifact一定是非空的（因为已经运行了），但是在这个测试case里，上游节点没有生成output artifact，所以是空字符串
			assert.Contains(t, srt.job.Job().Artifacts.Input, "train_data")
			assert.Equal(t, "", srt.job.Job().Artifacts.Input["train_data"])

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_model")
			assert.Equal(t, "", srt.job.Job().Artifacts.Output["train_model"])

			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "validate" {
			assert.Equal(t, 4, len(srt.job.Job().Parameters))
			assert.Contains(t, srt.job.Job().Parameters, "refSystem")
			assert.Equal(t, "run-000001", srt.job.Job().Parameters["refSystem"])

			assert.Equal(t, 4+5, len(srt.job.Job().Env)) // 4 env
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
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	extra := GetExtra()
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	sortedSteps, err := common.TopologicalSort(wfs.EntryPoints.EntryPoints)
	assert.Nil(t, err)

	sysNum := len(common.SysParamNameList)

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da", "a.entrypoint.da", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	ds := NewDependencySolver(dr)

	for _, stepName := range sortedSteps {
		err := ds.ResolveBeforeRun(dr.getworkflowSouceDag().EntryPoints[stepName])
		assert.Nil(t, err)

		st := wfs.EntryPoints.EntryPoints[stepName].(*schema.WorkflowSourceStep)
		srt := NewStepRuntime("a.entrypoint."+st.Name, "a.entrypoint."+st.Name, st, 0, context.Background(), failctx,
			make(chan<- WorkflowEvent), rf, "dag-11")
		srt.setSysParams()

		dr.subComponentRumtimes[stepName] = append(dr.subComponentRumtimes[stepName], srt)

		err = srt.generateOutArtPathOnFs()
		assert.Nil(t, err)

		forCacheFingerprint := false
		err = srt.updateJob(forCacheFingerprint)

		assert.Nil(t, err)

		OutatfTrainData := ".pipeline/run-000001/myproject/data-preprocess-0-2578f927d74c8ef09add007179d6d227/train_data"
		OutatfValidateData := ".pipeline/run-000001/myproject/data-preprocess-0-2578f927d74c8ef09add007179d6d227/validate_data"
		OutatfTrainModel := ".pipeline/run-000001/myproject/main-0-d85d2ce5c2b131b6efc68069a4f18c9b/train_model"
		if stepName == "data-preprocess" {
			assert.Equal(t, 2, len(srt.job.Job().Parameters))

			assert.Equal(t, 2+sysNum+2, len(srt.job.Job().Env)) // 4 env + 6 sys param + 2 artifact

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_data")
			assert.Contains(t, srt.job.Job().Artifacts.Output, "validate_data")
			assert.Equal(t, OutatfTrainData, srt.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, OutatfValidateData, srt.job.Job().Artifacts.Output["validate_data"])

			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_VALIDATE_DATA")
			assert.Equal(t, common.ArtMountDir+"/"+OutatfTrainData, srt.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, common.ArtMountDir+"/"+OutatfValidateData, srt.job.Job().Env["PF_OUTPUT_ARTIFACT_VALIDATE_DATA"])

			expectedCommand := fmt.Sprintf("python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre --validate %s --stepname data-preprocess",
				common.ArtMountDir+"/"+OutatfValidateData)
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "main" {

			assert.Equal(t, 7, len(srt.job.Job().Parameters))
			assert.Contains(t, srt.job.Job().Parameters, "data_file")
			assert.Equal(t, "./data/pre", srt.job.Job().Parameters["data_file"])
			assert.Equal(t, "dictparam", srt.job.Job().Parameters["p3"])
			assert.Equal(t, "0.66", srt.job.Job().Parameters["p4"])
			assert.Equal(t, "/path/to/anywhere", srt.job.Job().Parameters["p5"])

			assert.Equal(t, 5+5+2, len(srt.job.Job().Env)) // 5 env + 5 sys param + 2 artifact

			// input artifact 替换为上游节点的output artifact
			// 实际运行中上游节点的output artifact一定是非空的（因为已经运行了），但是在这个测试case里，上游节点没有生成output artifact，所以是空字符串
			assert.Contains(t, srt.job.Job().Artifacts.Input, "train_data")
			assert.Equal(t, OutatfTrainData, srt.job.Job().Artifacts.Input["train_data"])

			assert.Contains(t, srt.job.Job().Artifacts.Output, "train_model")
			assert.Equal(t, OutatfTrainModel, srt.job.Job().Artifacts.Output["train_model"])

			assert.Contains(t, srt.job.Job().Env, "PF_INPUT_ARTIFACT_TRAIN_DATA")
			assert.Contains(t, srt.job.Job().Env, "PF_OUTPUT_ARTIFACT_TRAIN_MODEL")
			assert.Equal(t, common.ArtMountDir+"/"+OutatfTrainData, srt.job.Job().Env["PF_INPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, common.ArtMountDir+"/"+OutatfTrainModel, srt.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_MODEL"])

			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, srt.job.Job().Command)
		}
		if stepName == "validate" {
			assert.Equal(t, 4, len(srt.job.Job().Parameters))
			assert.Contains(t, srt.job.Job().Parameters, "refSystem")
			assert.Equal(t, "run-000001", srt.job.Job().Parameters["refSystem"])

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
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	mockCbs.GetJobCb = func(jobID string) (schema.JobView, error) {
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
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	failctx, _ := context.WithCancel(context.Background())

	st := wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt := NewStepRuntime("a.entrypoint."+st.Name, "a.entrypoint."+st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")
	srt.setSysParams()

	patches := gomonkey.ApplyMethod(reflect.TypeOf(srt.job), "Validate", func(_ *PaddleFlowJob) error {
		return nil
	})
	defer patches.Reset()

	job := srt.job.(*PaddleFlowJob)
	cacheCaculator, err := NewCacheCalculator(*job, wfs.Cache, srt.logger, srt.getWorkFlowStep().FsMount,
		srt.GlobalFsID)

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
	rf.callbacks.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "3333", RunID: "run-000027", JobID: "job-xxx",
				UpdatedAt: updateTime, ExpiredTime: "-1"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime("a.entrypoint."+st.Name, "a.entrypoint."+st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")

	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，但是cache已经过时
	updateTime = time.Now().Add(time.Second * time.Duration(-1*500))
	rf.callbacks.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", UpdatedAt: updateTime, ExpiredTime: "300"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime("a.entrypoint."+st.Name, "a.entrypoint."+st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")
	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, false, cacheFound)

	// first fingerprint 查询返回非空，且命中expired time为-1的cache记录
	updateTime = time.Now().Add(time.Second * time.Duration(-1*100))
	rf.callbacks.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", JobID: "job-001",
				UpdatedAt: updateTime, ExpiredTime: "-1"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime("a.entrypoint."+st.Name, "a.entrypoint."+st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")

	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, true, cacheFound)

	// first fingerprint 查询返回非空，且命中expired time不为-1，但依然有效的cache记录
	updateTime = time.Now().Add(time.Second * time.Duration(-1*100))
	rf.callbacks.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", JobID: "job-001",
				UpdatedAt: updateTime, ExpiredTime: "300"},
		}, nil
	}

	st = wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt = NewStepRuntime("a.entrypoint."+st.Name, "a.entrypoint."+st.Name, st, 0, context.Background(), failctx,
		make(chan<- WorkflowEvent), rf, "dag-11")
	cacheFound, err = srt.checkCached()
	assert.Nil(t, err)
	assert.Equal(t, true, cacheFound)
}

func mockToListenEvent(ec chan WorkflowEvent, ep *WorkflowEvent) {
	*ep = <-ec
}

func TestNewStepRuntimeWithStatus(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	extra := GetExtra()
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da", "a.entrypoint.da", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	updateRuntimeCalled = false
	eventChan := make(chan WorkflowEvent)
	ep := &WorkflowEvent{}
	go mockToListenEvent(eventChan, ep)

	st := wfs.EntryPoints.EntryPoints["data-preprocess"]
	srt := newStepRuntimeWithStatus("a.entrypoint.data-preprocess", "a.entrypoint.data-preprocess", st.(*schema.WorkflowSourceStep), 0, context.Background(), failctx,
		eventChan, rf, "dag-11", StatusRuntimeFailed, "failed hahah")

	assert.True(t, updateRuntimeCalled)

	time.Sleep(time.Millisecond * 100)

	assert.True(t, ep.isJobUpdate())
	assert.True(t, srt.done)

	assert.Equal(t, ep.Message, "failed hahah")
	assert.Equal(t, ep.Extra[apicommon.WfEventKeyRunID], "run-000001")
}

func TestExecute(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	extra := GetExtra()
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da", "a.entrypoint.da", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	updateRuntimeCalled = false
	eventChan := make(chan WorkflowEvent)
	// ep := &WorkflowEvent{}
	// go mockToListenEvent(eventChan, ep)

	st := wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	st.Artifacts.Input = map[string]string{}
	st.GetArtifacts().Input["abc"] = "./abc"
	srt := NewStepRuntime("a.entrypoint.data-preprocess", "a.entrypoint.data-preprocess", st, 0, context.Background(), failctx,
		eventChan, rf, "dag-11")
	srt.setSysParams()

	// 1、没有开启cache
	patches := gomonkey.ApplyMethod(reflect.TypeOf(srt.job), "Validate", func(_ *PaddleFlowJob) error {
		return nil
	})
	defer patches.Reset()

	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(srt.job), "Start", func(_ *PaddleFlowJob) (string, error) {
		return "job-001", nil
	})
	defer patch1.Reset()

	artifactLoged := false
	srt.runConfig.callbacks.LogArtifactCb = func(req schema.LogRunArtifactRequest) error {
		artifactLoged = true
		return nil
	}

	srt.parallelismManager.increase()
	srt.Execute()

	assert.Equal(t, artifactLoged, true)

	srt.updateStatus(StatusRuntimeCancelled)
	assert.Equal(t, srt.parallelismManager.CurrentParallelism(), 0)

	// 2、开启了cache， 且命中的情况

	st.Cache.Enable = true
	srt = NewStepRuntime("a.entrypoint.data-preprocess", "a.entrypoint.data-preprocess", st, 0, context.Background(), failctx,
		eventChan, rf, "dag-11")

	job := srt.job.(*PaddleFlowJob)
	cacheCaculator, err := NewCacheCalculator(*job, wfs.Cache, srt.logger, srt.getWorkFlowStep().FsMount,
		srt.GlobalFsID)
	patch12 := gomonkey.ApplyMethod(reflect.TypeOf(cacheCaculator), "CalculateFirstFingerprint", func(_ *conservativeCacheCalculator) (string, error) {
		return "1111", nil
	})
	defer patch12.Reset()

	patch22 := gomonkey.ApplyMethod(reflect.TypeOf(cacheCaculator), "CalculateSecondFingerprint", func(_ *conservativeCacheCalculator) (string, error) {
		return "2222", nil
	})
	defer patch22.Reset()

	rf.callbacks.GetJobCb = func(jobID string) (schema.JobView, error) {
		outAtfs := map[string]string{
			"train_data":    "way/to/train_data",
			"validate_data": "way/to/validate_data",
		}
		return schema.JobView{Artifacts: schema.Artifacts{Output: outAtfs}, Status: StatusRuntimeSucceeded}, nil
	}

	updateTime := time.Now().Add(time.Second * time.Duration(-1*100))
	rf.callbacks.ListCacheCb = func(firstFp, fsID, yamlPath string) ([]models.RunCache, error) {
		return []models.RunCache{
			models.RunCache{FirstFp: "1111", SecondFp: "2222", RunID: "run-000027", JobID: "job-001",
				UpdatedAt: updateTime, ExpiredTime: "-1"},
		}, nil
	}

	srt.parallelismManager.increase()
	srt.Execute()

	assert.Equal(t, srt.status, StatusRuntimeSucceeded)
	assert.Equal(t, srt.parallelismManager.CurrentParallelism(), 0)
}

func TestProcessEventFromJob(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	rf := mockRunConfigForComponentRuntime()
	rf.WorkflowSource = &wfs
	rf.callbacks = mockCbs

	extra := GetExtra()
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da", "a.entrypoint.da", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	Runcalled := false
	rf.callbacks.UpdateRuntimeCb = func(id string, event interface{}) (int64, bool) {
		Runcalled = true
		return 123, true
	}

	eventChan := make(chan WorkflowEvent)
	ep := &WorkflowEvent{}
	go func(eventChan chan WorkflowEvent, ep *WorkflowEvent) {
		for {
			*ep = <-eventChan
		}
	}(eventChan, ep)

	st := wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	st.Artifacts.Input = map[string]string{}
	st.GetArtifacts().Input["abc"] = "./abc"
	srt := NewStepRuntime("a.entrypoint.data-preprocess", "a.entrypoint.data-preprocess", st, 0, context.Background(), failctx,
		eventChan, rf, "dag-11")
	srt.setSysParams()

	event := NewWorkflowEvent(WfEventJobWatchErr, "Watch error", map[string]interface{}{})
	srt.processEventFromJob(*event)

	time.Sleep(time.Millisecond * 100)
	assert.True(t, Runcalled)

	assert.Equal(t, ep.Message, "")

	event = NewWorkflowEvent(WfEventJobUpdate, "Running", map[string]interface{}{
		apicommon.WfEventKeyStatus: StatusRuntimeRunning,
	})

	Runcalled = false
	srt.processEventFromJob(*event)
	time.Sleep(time.Millisecond * 100)
	assert.True(t, Runcalled)
	assert.Equal(t, ep.Message, "Running")

	artifactLoged := false
	srt.runConfig.callbacks.LogArtifactCb = func(req schema.LogRunArtifactRequest) error {
		artifactLoged = true
		return nil
	}

	event = NewWorkflowEvent(WfEventJobUpdate, "succeeded", map[string]interface{}{
		apicommon.WfEventKeyStatus: StatusRuntimeSucceeded,
	})

	Runcalled = false
	srt.increase()
	srt.processEventFromJob(*event)
	time.Sleep(time.Millisecond * 100)

	assert.True(t, Runcalled)
	assert.Equal(t, ep.Message, "succeeded")
	assert.True(t, srt.done)
	assert.True(t, artifactLoged)

}

func TestStart(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	rf := mockRunConfigForComponentRuntime()
	rf.callbacks = mockCbs
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	rf.WorkflowSource = &wfptr.Source

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da1", "a.entrypoint.da1", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	eventChan := make(chan WorkflowEvent)
	ep := &WorkflowEvent{}
	go func(eventChan chan WorkflowEvent, ep *WorkflowEvent) {
		for {
			*ep = <-eventChan
		}
	}(eventChan, ep)

	st := wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	st.Artifacts.Input = map[string]string{}
	st.GetArtifacts().Input["abc"] = "./abc"
	srt := NewStepRuntime("a.entrypoint.data-preprocess", "a.entrypoint.data-preprocess", st, 0, context.Background(), failctx,
		eventChan, rf, "dag-11")
	srt.setSysParams()

	st.Condition = "10 < 11"

	executed := false
	patch22 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Execute", func(_ *StepRuntime) {
		executed = true
		srt.parallelismManager.decrease()
	})
	defer patch22.Reset()

	srt.status = ""
	srt.Start()

	time.Sleep(time.Millisecond * 100)

	assert.True(t, executed)
	assert.Equal(t, ep.Message, "")

	st.Condition = "10 < 9"
	executed = false

	srt.status = ""
	srt.Start()
	time.Sleep(time.Millisecond * 100)

	assert.False(t, executed)
	assert.Equal(t, ep.Extra[apicommon.WfEventKeyStatus], StatusRuntimeSkipped)
	assert.True(t, srt.isSkipped())
	assert.True(t, strings.Contains(ep.Message, "condition"))
	assert.Equal(t, srt.parallelismManager.CurrentParallelism(), 0)

	// 测试disabled 的情况
	*ep = WorkflowEvent{}
	executed = false
	st.Condition = "10 > 9"
	wfptr.Source.Disabled = st.Name

	srt.status = ""
	srt.done = false
	srt.Start()
	time.Sleep(time.Millisecond * 100)

	assert.False(t, executed)
	assert.Equal(t, ep.Extra[apicommon.WfEventKeyStatus], StatusRuntimeSkipped)
	assert.True(t, srt.isSkipped())
	assert.True(t, strings.Contains(ep.Message, "disabled"))
	assert.Equal(t, srt.parallelismManager.CurrentParallelism(), 0)
}

func TestStepRestart(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	rf := mockRunConfigForComponentRuntime()
	rf.callbacks = mockCbs
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	rf.WorkflowSource = &wfptr.Source

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da1", "a.entrypoint.da1", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	eventChan := make(chan WorkflowEvent)
	ep := &WorkflowEvent{}
	go func(eventChan chan WorkflowEvent, ep *WorkflowEvent) {
		for {
			*ep = <-eventChan
		}
	}(eventChan, ep)

	st := wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt := NewStepRuntime("a.entrypoint.data-preprocess", "a.entrypoint.data-preprocess", st, 0, context.Background(), failctx,
		eventChan, rf, "dag-11")
	srt.setSysParams()

	patch2 := gomonkey.ApplyMethod(reflect.TypeOf(srt), "Start", func(srt *StepRuntime) {
		srt.updateStatus(StatusRuntimeRunning)
		fmt.Println("param", srt.parallelismManager.CurrentParallelism())
	})
	defer patch2.Reset()

	jobView := schema.JobView{
		Status: StatusRuntimeFailed,
		PK:     12,
	}

	srt.Restart(&jobView)
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, int64(12), srt.pk)

	assert.Equal(t, StatusRuntimeRunning, srt.status)
}

func TestStop(t *testing.T) {
	handler.NewFsHandlerWithServer = handler.MockerNewFsHandlerWithServer
	testCase := loadcase(runYamlPath)
	wfs, err := schema.GetWorkflowSource([]byte(testCase))
	assert.Nil(t, err)

	extra := GetExtra()
	rf := mockRunConfigForComponentRuntime()
	rf.callbacks = mockCbs
	wfptr, err := NewMockWorkflow(wfs, rf.runID, map[string]interface{}{}, extra, rf.callbacks)
	assert.Nil(t, err)

	wfs = wfptr.Source

	rf.WorkflowSource = &wfptr.Source

	failctx, _ := context.WithCancel(context.Background())
	dr := NewDagRuntime("a.entrypoint.da", "a.entrypoint.da", &wfs.EntryPoints, 0, context.Background(), failctx, make(chan<- WorkflowEvent), rf, "0")
	dr.setSysParams()

	eventChan := make(chan WorkflowEvent)
	ep := &WorkflowEvent{}
	go func(eventChan chan WorkflowEvent, ep *WorkflowEvent) {
		for {
			*ep = <-eventChan
		}
	}(eventChan, ep)

	st := wfs.EntryPoints.EntryPoints["data-preprocess"].(*schema.WorkflowSourceStep)
	srt := NewStepRuntime("a.entrypoint.data-preprocess", "a.entrypoint.data-preprocess", st, 0, context.Background(), failctx,
		eventChan, rf, "dag-11")
	srt.setSysParams()

	srt.done = false
	srt.increase()
	srt.stopWithMsg("stop without jobid")
	time.Sleep(time.Millisecond * 100)

	assert.Equal(t, srt.status, StatusRuntimeFailed)
	assert.True(t, strings.Contains(ep.Message, "jobid"))
	assert.Equal(t, 0, srt.CurrentParallelism())

	srt.done = true
	srt.job.(*PaddleFlowJob).ID = "12334"
	ep = &WorkflowEvent{}
	srt.stopWithMsg("stop")
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, ep.Message, "")
	assert.Equal(t, 0, srt.CurrentParallelism())

	srt.done = false
	ep = &WorkflowEvent{}
	stoped := false
	gomonkey.ApplyMethod(reflect.TypeOf(srt.job), "Stop", func(_ *PaddleFlowJob) error {
		stoped = true
		return nil
	})

	srt.stopWithMsg("stop normal")
	assert.Equal(t, srt.status, StatusRuntimeFailed)
	assert.Equal(t, ep.Message, "")
	assert.Equal(t, 0, srt.CurrentParallelism())
	assert.True(t, stoped)
}
