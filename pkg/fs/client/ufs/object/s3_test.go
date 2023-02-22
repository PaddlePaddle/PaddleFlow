package object

import (
	"reflect"
	"testing"
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
