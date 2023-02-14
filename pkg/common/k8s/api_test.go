package k8s

import (
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func TestGetJobGVR(t *testing.T) {
	gvr := GetJobGVR(schema.FrameworkPaddle)
	t.Logf("gvr is %v", gvr)
}
