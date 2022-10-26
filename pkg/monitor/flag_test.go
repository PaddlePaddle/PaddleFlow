package monitor

import (
	"reflect"
	"testing"
)

func TestMetricsFlags(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{
			name: "metrics num",
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MetricsFlags(); !reflect.DeepEqual(len(got), tt.want) {
				t.Errorf("MetricsFlags() = %v, want %v", len(got), tt.want)
			}
		})
	}
}
