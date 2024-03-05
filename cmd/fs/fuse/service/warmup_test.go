package service

import (
	"fmt"
	"github.com/panjf2000/ants/v2"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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
	pool, _ = ants.NewPool(3)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := warmup_(tt.args.fname, tt.args.paths, tt.args.threads, tt.args.warmType, false); (err != nil) != tt.wantErr {
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

// TestFindUniqueParentDirs 测试 findUniqueParentDirs 函数
func TestFindUniqueParentDirs(t *testing.T) {
	// 定义测试用例
	testCases := []struct {
		name     string
		paths    []string
		expected []string
	}{
		{
			name: "Standard paths",
			paths: []string{
				"/home/user/docs/report.txt",
				"/home/user/docs/assignment.docx",
				"/var/log/sys.log",
				"/usr/bin/",
				"/usr/bin/someexecutable",
			},
			expected: []string{
				"/home/user/docs/",
				"/var/log/",
				"/usr/bin/",
			},
		},
		{
			name: "Paths with no duplicates",
			paths: []string{
				"/etc/nginx/nginx.conf",
				"/var/www/html/index.html",
			},
			expected: []string{
				"/etc/nginx/",
				"/var/www/html/",
			},
		},
		{
			name:     "Empty paths",
			paths:    []string{},
			expected: []string{},
		},
		{
			name: "Root path",
			paths: []string{
				"/",
			},
			expected: []string{
				"/",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := findUniqueParentDirs(tc.paths)

			sort.Strings(actual)
			sort.Strings(tc.expected)
			fmt.Printf("actual: %+v\n", actual)

			if !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("For paths %v, expected %v but got %v", tc.paths, tc.expected, actual)
			}
		})
	}
}

func generateMockPaths(numPaths int) []string {
	baseDirs := []string{"/home/user", "/var/log", "/etc", "/usr/bin"}

	paths := make([]string, numPaths)
	for i := 0; i < numPaths; i++ {
		baseDir := baseDirs[rand.Intn(len(baseDirs))] // 随机选择一个基础目录
		filename := "file" + strconv.Itoa(i) + ".txt"
		paths[i] = filepath.Join(baseDir, filename)
	}
	return paths
}

func TestFindUniqueParentDirsV1(t *testing.T) {
	const numPaths = 100000000
	paths := generateMockPaths(numPaths)

	uniqueDirs := findUniqueParentDirs(paths)

	if len(uniqueDirs) == 0 {
		t.Errorf("No unique directories found.")
	}
}
