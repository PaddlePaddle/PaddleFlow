package object

import "testing"

func Test_addTestFile(t *testing.T) {
	tests := []struct {
		name string
		want int
	}{
		{
			name: "test1",
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := addTestFile(); got != tt.want {
				t.Errorf("addTestFile() = %v, want %v", got, tt.want)
			}
		})
	}
}
