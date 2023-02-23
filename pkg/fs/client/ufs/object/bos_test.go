package object

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	api2 "github.com/baidubce/bce-sdk-go/services/bos/api"
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

func TestBos_String(t *testing.T) {
	type fields struct {
		bucket    string
		bosClient *bos.Client
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "name",
			fields: fields{
				bucket:    "t",
				bosClient: &bos.Client{},
			},
			want: BosName,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := Bos{
				bucket:    tt.fields.bucket,
				bosClient: tt.fields.bosClient,
			}
			if got := storage.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBos_Deletes(t *testing.T) {
	type fields struct {
		bucket    string
		bosClient *bos.Client
	}
	type args struct {
		keys []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "key empty",
			args: args{
				keys: []string{},
			},
			fields:  fields{},
			wantErr: true,
		},
		{
			name: "delete error",
			args: args{
				keys: []string{"1234"},
			},
			fields:  fields{},
			wantErr: true,
		},
	}
	a := &bos.Client{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "DeleteMultipleObjectsFromKeyList",
		func(a *bos.Client, bucket string, keyList []string) (*api2.DeleteMultipleObjectsResult, error) {
			return &api2.DeleteMultipleObjectsResult{
				Errors: []api2.DeleteObjectResult{
					{
						Code:    "124",
						Message: "err",
					},
				},
			}, fmt.Errorf("delete err")
		})
	defer p1.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := Bos{
				bucket:    tt.fields.bucket,
				bosClient: tt.fields.bosClient,
			}
			if err := storage.Deletes(tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("Deletes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBos_AbortUpload(t *testing.T) {
	type fields struct {
		bucket    string
		bosClient *bos.Client
	}
	type args struct {
		key      string
		uploadID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "err",
			fields: fields{},
			args: args{
				key:      "12",
				uploadID: "35",
			},
			wantErr: true,
		},
	}

	var p2 = gomonkey.ApplyFunc(api2.AbortMultipartUpload, func(cli bce.Client, bucket, object, uploadId string) error {
		return fmt.Errorf("AbortMultipartUpload err")
	})
	defer p2.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := Bos{
				bucket:    tt.fields.bucket,
				bosClient: tt.fields.bosClient,
			}
			if err := storage.AbortUpload(tt.args.key, tt.args.uploadID); (err != nil) != tt.wantErr {
				t.Errorf("AbortUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
