package test_file

import "testing"

func Test_t(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{
			name: "test",
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := t1(); got != tt.want {
				t.Errorf("t() = %v, want %v", got, tt.want)
			}
		})
	}
}
