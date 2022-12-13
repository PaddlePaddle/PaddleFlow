package cache

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

func Test_freeMemory(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "mem percent > 0.3",
		},
	}
	patch := gomonkey.ApplyFunc(utils.GetProcessMemPercent, func() float32 {
		return 50
	})
	defer patch.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			freeMemory()
		})
	}
}
