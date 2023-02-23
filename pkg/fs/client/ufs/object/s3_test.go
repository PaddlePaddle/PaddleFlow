package object

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/aws/aws-sdk-go/service/s3"
)

func Test_metadataToLower(t *testing.T) {
	type args struct {
		m map[string]*string
	}
	a := "def"
	tests := []struct {
		name string
		args args
		want map[string]*string
	}{
		{
			name: "test1",
			args: args{
				m: map[string]*string{
					"aBc": &a,
				},
			},
			want: map[string]*string{
				"abc": &a,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := metadataToLower(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("metadataToLower() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_metadataToLowerBos(t *testing.T) {
	type args struct {
		m map[string]string
	}
	a := "def"
	tests := []struct {
		name string
		args args
		want map[string]*string
	}{
		{
			name: "test1",
			args: args{
				m: map[string]string{
					"aBc": a,
				},
			},
			want: map[string]*string{
				"abc": &a,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := metadataToLowerBos(tt.args.m); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("metadataToLowerBos() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3Storage_String(t *testing.T) {
	type fields struct {
		bucket string
		s3     *s3.S3
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "test",
			fields: fields{
				bucket: "te",
				s3:     &s3.S3{},
			},
			want: S3Name,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := S3Storage{
				bucket: tt.fields.bucket,
				s3:     tt.fields.s3,
			}
			if got := storage.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3Storage_Deletes(t *testing.T) {
	type fields struct {
		bucket string
		s3     *s3.S3
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
			name: "empty",
			args: args{
				keys: []string{},
			},
			fields: fields{
				bucket: "t",
				s3:     &s3.S3{},
			},
			wantErr: true,
		},
		{
			name: "delete fail",
			args: args{
				keys: []string{"1224"},
			},
			fields: fields{
				bucket: "t",
				s3:     &s3.S3{},
			},
			wantErr: true,
		},
	}

	a := &s3.S3{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "DeleteObjects",
		func(a *s3.S3, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			return nil, fmt.Errorf("delete err")
		})
	defer p1.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := S3Storage{
				bucket: tt.fields.bucket,
				s3:     tt.fields.s3,
			}
			if err := storage.Deletes(tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("Deletes() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestS3Storage_Copy(t *testing.T) {
	type fields struct {
		bucket string
		s3     *s3.S3
	}
	type args struct {
		newKey     string
		copySource string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "copy fail",
			fields: fields{
				bucket: "a",
				s3:     &s3.S3{},
			},
			args: args{
				newKey:     "c",
				copySource: "d",
			},
			wantErr: true,
		},
	}

	a := &s3.S3{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "CopyObject",
		func(a *s3.S3, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
			return nil, fmt.Errorf("copy err")
		})
	defer p1.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := S3Storage{
				bucket: tt.fields.bucket,
				s3:     tt.fields.s3,
			}
			if err := storage.Copy(tt.args.newKey, tt.args.copySource); (err != nil) != tt.wantErr {
				t.Errorf("Copy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestS3Storage_List(t *testing.T) {
	type fields struct {
		bucket string
		s3     *s3.S3
	}
	type args struct {
		input *ListInput
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ListBlobsOutput
		wantErr bool
	}{
		{
			name: "list err",
			fields: fields{
				bucket: "a",
				s3:     &s3.S3{},
			},
			args: args{
				input: &ListInput{},
			},
			wantErr: true,
		},
	}
	a := &s3.S3{}
	var p3 = gomonkey.ApplyMethod(reflect.TypeOf(a), "ListObjectsV2",
		func(a *s3.S3, input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			return nil, fmt.Errorf("list err")
		})
	defer p3.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := S3Storage{
				bucket: tt.fields.bucket,
				s3:     tt.fields.s3,
			}
			got, err := storage.List(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("List() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3Storage_CreateMultipartUpload(t *testing.T) {
	type fields struct {
		bucket string
		s3     *s3.S3
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *MultipartCommitOutPut
		wantErr bool
	}{
		{
			name: "multipart err",
			fields: fields{
				bucket: "a",
				s3:     &s3.S3{},
			},
			args: args{
				key: "key1",
			},
			wantErr: true,
		},
	}

	a := &s3.S3{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "CreateMultipartUpload",
		func(a *s3.S3, input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
			return nil, fmt.Errorf("CreateMultipartUpload err")
		})
	defer p1.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := S3Storage{
				bucket: tt.fields.bucket,
				s3:     tt.fields.s3,
			}
			got, err := storage.CreateMultipartUpload(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateMultipartUpload() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3Storage_UploadPart(t *testing.T) {
	type fields struct {
		bucket string
		s3     *s3.S3
	}
	type args struct {
		key      string
		uploadID string
		num      int64
		body     []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Part
		wantErr bool
	}{
		{
			name: "upload part err",
			fields: fields{
				bucket: "a",
				s3:     &s3.S3{},
			},
			args: args{
				key:      "124",
				num:      1,
				uploadID: "124",
				body:     []byte("sdr"),
			},
			wantErr: true,
		},
	}

	a := &s3.S3{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "UploadPart",
		func(a *s3.S3, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
			return nil, fmt.Errorf("UploadPart err")
		})
	defer p1.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := S3Storage{
				bucket: tt.fields.bucket,
				s3:     tt.fields.s3,
			}
			got, err := storage.UploadPart(tt.args.key, tt.args.uploadID, tt.args.num, tt.args.body)
			if (err != nil) != tt.wantErr {
				t.Errorf("UploadPart() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UploadPart() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3Storage_AbortUpload(t *testing.T) {
	type fields struct {
		bucket string
		s3     *s3.S3
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
			name: "abort upload err",
			fields: fields{
				bucket: "a",
				s3:     &s3.S3{},
			},
			args: args{
				key:      "a",
				uploadID: "1",
			},
			wantErr: true,
		},
	}

	a := &s3.S3{}
	var p1 = gomonkey.ApplyMethod(reflect.TypeOf(a), "AbortMultipartUpload",
		func(a *s3.S3, input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
			return nil, fmt.Errorf("AbortMultipartUpload err")
		})
	defer p1.Reset()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := S3Storage{
				bucket: tt.fields.bucket,
				s3:     tt.fields.s3,
			}
			if err := storage.AbortUpload(tt.args.key, tt.args.uploadID); (err != nil) != tt.wantErr {
				t.Errorf("AbortUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
