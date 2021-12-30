package pipeline

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"

	"paddleflow/pkg/common/schema"
)

func loadcase(casePath string) []byte {
	data, err := ioutil.ReadFile(casePath)
	if err != nil {
		fmt.Println("File reading error", err)
		return []byte{}
	}
	return data
}

func parseWorkflowSource(runYaml []byte) schema.WorkflowSource {
	// parse yaml -> WorkflowSource
	wfs := schema.WorkflowSource{}
	if err := yaml.Unmarshal(runYaml, &wfs); err != nil {
		fmt.Printf("Unmarshal runYaml failed. err:%v\n", err)
		return schema.WorkflowSource{}
	}
	return wfs
}

// 测试运行 Workflow 成功
func TestUpdateJob(t *testing.T) {
	testCase := loadcase("./testcase/run.step.yaml")
	wfs := parseWorkflowSource(testCase)
	bwf := NewBaseWorkflow(wfs, "runId", "", nil, nil)
	wf := Workflow{
		BaseWorkflow: bwf,
	}
	wf.runtime = NewWorkflowRuntime(&wf, 10)
	err := bwf.validate()
	assert.Nil(t, err)
	sortedSteps, err := wf.topologicalSort(wf.Source.EntryPoints)
	assert.Nil(t, err)
	for _, stepName := range sortedSteps {
		stepInfo := bwf.Source.EntryPoints[stepName]
		st := &Step{
			name:  stepName,
			wfr:   wf.runtime,
			info:  stepInfo,
			ready: make(chan bool, 1),
			done:  false,
		}
		wf.runtime.steps[stepName] = st
		st.job = NewPaddleFlowJob(st.name, st.info.Image, st.info.Deps)
		err := st.updateJob()
		assert.Nil(t, err)

		if stepName == "data_preprocess" {
			assert.Equal(t, 2, len(st.job.Job().Parameters))
			expectedCommand := "python data_preprocess.py --input ./LINK/mybos_dir/data --output ./data/pre"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
			assert.Equal(t, 7+3, len(st.job.Job().Env)) // env + sys param + 3 artifact
			assert.Equal(t, "./LINK/mybos_dir/data", st.job.Job().Artifacts.Input["data1"])
			assert.Equal(t, "/path/from/param/./data/pre/train", st.job.Job().Artifacts.Output["train_data"])
			assert.Equal(t, "./LINK/mybos_dir/data", st.job.Job().Env["PF_INPUT_ARTIFACT_DATA1"])
			assert.Equal(t, "/path/from/param/./data/pre/train", st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_DATA"])
		}
		if stepName == "main" {
			assert.Equal(t, 3, len(st.job.Job().Parameters))
			assert.Equal(t, "./data/pre", st.job.Job().Parameters["data_file"])
			expectedCommand := "python train.py -r 0.1 -d ./data/pre --output ./data/model"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
			assert.Equal(t, 10+2, len(st.job.Job().Env)) // env + sys param + 2 artifact
			assert.Equal(t, "/path/from/param/./data/pre/train", st.job.Job().Artifacts.Input["train_data"])
			assert.Equal(t, "./data/model", st.job.Job().Artifacts.Output["train_model"])
			assert.Equal(t, "/path/from/param/./data/pre/train", st.job.Job().Env["PF_INPUT_ARTIFACT_TRAIN_DATA"])
			assert.Equal(t, "./data/model", st.job.Job().Env["PF_OUTPUT_ARTIFACT_TRAIN_MODEL"])
		}
		if stepName == "validate" {
			assert.Equal(t, 1, len(st.job.Job().Parameters))
			expectedCommand := "python validate.py --model ./data/model --report ./data/report"
			assert.Equal(t, expectedCommand, st.job.Job().Command)
			assert.Equal(t, 9+2, len(st.job.Job().Env)) // env + sys param + 2 artifact
			assert.Equal(t, "/path/from/param/runId/validate", st.job.Job().Artifacts.Input["data"])
			assert.Equal(t, "./data/model", st.job.Job().Artifacts.Input["model"])
		}
	}
}
