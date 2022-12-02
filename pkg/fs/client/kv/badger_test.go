package kv

import (
	"os"
	"testing"
)

func TestNewBadgerClient(t *testing.T) {
	type args struct {
		config Config
	}
	tests := []struct {
		name    string
		args    args
		want    KvClient
		wantErr bool
	}{
		{
			name: "fs id empty",
			args: args{
				config: Config{
					Driver:    DiskType,
					CachePath: "./cache",
				},
			},
			wantErr: false,
		},
	}
	defer os.RemoveAll("./cache")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewBadgerClient(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBadgerClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
