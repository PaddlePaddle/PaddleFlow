package object

import "testing"

func Test_a(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{
			name: "test a",
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := a(); got != tt.want {
				t.Errorf("a() = %v, want %v", got, tt.want)
			}
		})
	}
}
