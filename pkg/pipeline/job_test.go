package pipeline

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/controller/job"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/logger"
)

func TestStopJob(t *testing.T) {
	pfj := NewPaddleFlowJob("abc", "abc:qe", "root", make(chan<- WorkflowEvent), nil, nil)

	assert.Equal(t, "root", pfj.userName)

	patches := gomonkey.ApplyMethod(reflect.TypeOf(pfj), "Validate", func(_ *PaddleFlowJob) error {
		return nil
	})
	defer patches.Reset()

	patches2 := gomonkey.ApplyFunc(job.StopJob, func(ctx *logger.RequestContext, id string) error {
		assert.Equal(t, ctx.UserName, pfj.userName)
		return nil
	})
	defer patches2.Reset()

	pfj.Stop()
}
