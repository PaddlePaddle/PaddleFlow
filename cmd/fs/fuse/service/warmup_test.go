package service

import (
	"os"
	"strconv"
	"testing"
)

func Test_warmup_(t *testing.T) {
	type args struct {
		fname    string
		paths    []string
		threads  int
		warmType string
	}
	fname := "./test/file.txt"
	prepare()
	defer os.RemoveAll("test")
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "read ok data",
			args: args{
				fname:    fname,
				threads:  3,
				warmType: "data",
			},
		},
		{
			name: "read ok meta",
			args: args{
				fname:    fname,
				threads:  3,
				warmType: "meta",
			},
		},
		{
			name: "read not exist",
			args: args{
				fname:    "./test/xxxx.txt",
				threads:  3,
				warmType: "meta",
			},
			wantErr: true,
		},
		{
			name: "read not exist",
			args: args{
				fname:    "./test/file2.txt",
				threads:  3,
				warmType: "meta",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := warmup_(tt.args.fname, tt.args.paths, tt.args.threads, tt.args.warmType); (err != nil) != tt.wantErr {
				t.Errorf("warmup_() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func prepare() {
	os.Mkdir("test", 0755)
	os.Create("./test/file.txt")
	fw, err := os.OpenFile("./test/file.txt", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		path := "./test/" + strconv.Itoa(i)
		_, err = fw.Write([]byte(path + "\n"))
		if err != nil {
			panic(err)
		}
		os.Create(path)
	}
	fw.Close()

	fw, err = os.OpenFile("./test/file2.txt", os.O_WRONLY|os.O_CREATE, 0666)
	fw.Write([]byte("./test/notexist.txt" + "\n"))
	fw.Close()
}
