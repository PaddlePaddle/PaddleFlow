package object

import (
	"reflect"
	"testing"

	"github.com/baidubce/bce-sdk-go/services/sts/api"
)

func TestStsSessionToken(t *testing.T) {
	type args struct {
		ak       string
		sk       string
		duration int
		acl      string
	}
	tests := []struct {
		name    string
		args    args
		want    *api.GetSessionTokenResult
		wantErr bool
	}{
		{
			name: "sts fail",
			args: args{
				ak: "test",
				sk: "testfail",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StsSessionToken(tt.args.ak, tt.args.sk, tt.args.duration, tt.args.acl)
			if (err != nil) != tt.wantErr {
				t.Errorf("StsSessionToken() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StsSessionToken() got = %v, want %v", got, tt.want)
			}
		})
	}
}
