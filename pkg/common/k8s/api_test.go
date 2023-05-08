package k8s

import (
	"testing"

	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func TestGetJobGVR(t *testing.T) {
	gvr := GetJobGVR(pfschema.PaddleKindGroupVersion)
	t.Logf("gvr is %v", gvr)
}
