package k8s

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pfschema "github.com/PaddlePaddle/PaddleFlow/pkg/common/schema"
)

func TestGetJobGVR(t *testing.T) {
	gvr := GetJobGVR(pfschema.PaddleKindGroupVersion)
	t.Logf("gvr is %v", gvr)

	batchGVR := GetJobGVR(pfschema.BatchJobKindGroupVersion)
	assert.Equal(t, BatchJobGVR, batchGVR)
}
