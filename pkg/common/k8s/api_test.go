package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"

	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func TestGetJobGVR(t *testing.T) {
	gvr := GetJobGVR(pfschema.FrameworkPaddle)
	t.Logf("gvr is %v", gvr)
}

func TestGetJobType(t *testing.T) {
	testCases := []struct {
		name    string
		gvk     schema.GroupVersionKind
		jobType pfschema.JobType
	}{
		{
			name:    "single job",
			gvk:     PodGVK,
			jobType: pfschema.TypeSingle,
		},
		{
			name:    "distributed job",
			gvk:     PaddleJobGVK,
			jobType: pfschema.TypeDistributed,
		},
		{
			name:    "workflow job",
			gvk:     ArgoWorkflowGVK,
			jobType: pfschema.TypeWorkflow,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jobType := GetJobType(tc.gvk)
			assert.Equal(t, tc.jobType, jobType)
		})
	}
}

func TestGetJobFramework(t *testing.T) {
	testCases := []struct {
		name      string
		gvk       schema.GroupVersionKind
		framework pfschema.Framework
	}{
		{
			name:      "single job framework",
			gvk:       PodGVK,
			framework: pfschema.FrameworkStandalone,
		},
		{
			name:      "workflow job framework",
			gvk:       ArgoWorkflowGVK,
			framework: "",
		},
		{
			name:      "spark app framework",
			gvk:       SparkAppGVK,
			framework: pfschema.FrameworkSpark,
		},
		{
			name:      "paddle job framework",
			gvk:       PaddleJobGVK,
			framework: pfschema.FrameworkPaddle,
		},
		{
			name:      "pytorch job framework",
			gvk:       PyTorchJobGVK,
			framework: pfschema.FrameworkPytorch,
		},
		{
			name:      "tf job framework",
			gvk:       TFJobGVK,
			framework: pfschema.FrameworkTF,
		},
		{
			name:      "mxnet job framework",
			gvk:       MXNetJobGVK,
			framework: pfschema.FrameworkMXNet,
		},
		{
			name:      "mpi job framework",
			gvk:       MPIJobGVK,
			framework: pfschema.FrameworkMPI,
		},
		{
			name:      "ray job framework",
			gvk:       RayJobGVK,
			framework: pfschema.FrameworkRay,
		},
		{
			name:      "other job framework",
			gvk:       XGBoostJobGVK,
			framework: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fw := GetJobFramework(tc.gvk)
			assert.Equal(t, tc.framework, fw)
		})
	}
}
