package common

import (
	"os"
	"testing"
)

func TestGetAESEncryptKey(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			name: "empty",
			want: AESEncryptKey,
		},
	}
	os.Setenv(AESEncryptKeyEnv, "")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetAESEncryptKey(); got != tt.want {
				t.Errorf("GetAESEncryptKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
