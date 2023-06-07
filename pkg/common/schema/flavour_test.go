package schema

import (
	"fmt"
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

func TestMapToFlavour(t *testing.T) {
	testCases := []struct {
		name       string
		flavourMap map[string]interface{}
		wantRes    Flavour
		wantErr    error
	}{
		{
			name: "flavour with no scalaResources",
			wantRes: Flavour{
				Name: "flavour1",
				ResourceInfo: ResourceInfo{
					CPU: "1",
					Mem: "1Gi",
				},
			},
			flavourMap: map[string]interface{}{"name": "flavour1", "cpu": "1", "mem": "1Gi"},
		},
		{
			name: "flavour with scalaResources",
			wantRes: Flavour{
				Name: "flavour2",
				ResourceInfo: ResourceInfo{
					CPU: "4",
					Mem: "8Gi",
					ScalarResources: ScalarResourcesType{
						"nvidia.com/gpu": "1",
					},
				},
			},
			flavourMap: map[string]interface{}{"name": "flavour2", "cpu": "4", "mem": "8Gi", "scalarResources": map[ResourceName]string{"nvidia.com/gpu": "1"}},
		},
		{
			name:       "flavour with unexcepted name",
			wantRes:    Flavour{},
			wantErr:    fmt.Errorf("[name] defined in flavour should be string type"),
			flavourMap: map[string]interface{}{"name": 1, "cpu": "4", "mem": "8Gi"},
		},
		{
			name:       "flavour with unexcepted cpu",
			wantRes:    Flavour{},
			wantErr:    fmt.Errorf("[cpu] defined in flavour should be string type"),
			flavourMap: map[string]interface{}{"name": "flavour3", "cpu": 4, "mem": "8Gi"},
		},
		{
			name:       "flavour with unexcepted mem",
			wantRes:    Flavour{},
			wantErr:    fmt.Errorf("[memory] defined in flavour should be string type"),
			flavourMap: map[string]interface{}{"name": "flavour4", "cpu": "4", "mem": 8},
		},
		{
			name:       "flavour with unexcepted scalaResources",
			wantRes:    Flavour{},
			wantErr:    fmt.Errorf("[scalarResources] defined in flavour should be map[string]string type"),
			flavourMap: map[string]interface{}{"name": "flavour5", "cpu": "4", "mem": "8Gi", "scalarResources": map[ResourceName]int{"nvidia.com/gpu": 2}},
		},
		{
			name:       "flavour with invalid scalaResources",
			wantRes:    Flavour{},
			wantErr:    fmt.Errorf("validate scalar resource failed, error: memory resource cannot be negative"),
			flavourMap: map[string]interface{}{"name": "flavour6", "cpu": "4", "mem": "8Gi", "scalarResources": map[ResourceName]string{"memory": "-1"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			flavour, err := MapToFlavour(tc.flavourMap)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantRes, flavour)
		})
	}

}
