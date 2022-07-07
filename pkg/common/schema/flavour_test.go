package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckResource(t *testing.T) {
	reses := []struct {
		res       string
		resType   string
		expectErr bool
	}{
		{"1", "cpu", false},
		{"-1", "cpu", true},
		{"0", "cpu", true},
		{"0.1", "cpu", false},
		{"", "cpu", true},

		{"1", "memory", false},
		{"-1", "memory", true},
		{"0", "memory", true},
		{"0.1Gi", "memory", false},

		{"1", "nvidia.com/gpu", false},
		{"0", "nvidia.com/gpu-xxx", false},
		{"-1", "xxx", true},
		{"0.1Gi", "nvidia.com/gpu", false},
	}

	for _, res := range reses {
		var err error
		switch res.resType {
		case "cpu", "memory":
			err = ValidateResourceItem(res.res)
		default:
			err = CheckScalarResource(res.res)
		}
		if res.expectErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
