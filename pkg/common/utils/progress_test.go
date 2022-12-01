package utils

import (
	"github.com/stretchr/testify/assert"
	"github.com/vbauerster/mpb/v7"
	"testing"
)

func TestNewDynProgressBar(t *testing.T) {
	type args struct {
		title string
		quiet bool
	}
	tests := []struct {
		name  string
		args  args
		want  *mpb.Progress
		want1 *mpb.Bar
	}{
		{
			name: "not empty",
			args: args{
				title: "x",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := NewDynProgressBar(tt.args.title, tt.args.quiet)
			assert.NotNil(t, got)
			assert.NotNil(t, got1)
		})
	}
}
