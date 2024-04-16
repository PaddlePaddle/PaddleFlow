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
	pool, _ = ants.NewPool(5)
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
				"/home/user/docs/report.txt",
				"/home/user/docs/assignment.docx",
				"/var/log/sys.log",
				"/usr/bin/someexecutable",
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
			expected: []string{},
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
	baseDirs := []string{
		"/home/user", "/var/log", "/etc", "/usr/bin", "/tmp", "/var/spool", "/opt", "/usr/local/bin",
		"/var/lib", "/usr/share", "/var/cache", "/usr/include", "/var/run", "/usr/lib", "/var/tmp",
		"/usr/sbin", "/var/mail", "/usr/local/include", "/usr/local/lib", "/usr/local/sbin", "/var/spool/mail",
		"/var/spool/cron", "/var/log/apache2", "/var/log/syslog", "/var/log/kern.log", "/var/log/auth.log",
		"/var/log/user.log", "/var/log/dpkg.log", "/var/log/apt", "/var/log/exim4", "/var/log/nginx",
		"/var/log/mysql", "/var/log/postgresql", "/var/log/redis", "/var/log/supervisor", "/var/log/cron",
		"/var/log/maillog", "/var/log/secure", "/var/log/spooler", "/var/log/boot.log", "/var/log/messages",
		"/var/log/daemon.log", "/var/log/debug", "/var/log/lpr.log", "/var/log/mail.log", "/var/log/news",
		"/var/log/uucp", "/var/log/ftp", "/var/log/audit", "/var/log/btmp", "/var/log/wtmp", "/var/log/utmp",
		"/var/log/lastlog", "/var/log/faillog", "/var/log/tallylog", "/var/log/yum.log", "/var/log/clamav",
		"/var/log/cups", "/var/log/httpd", "/var/log/lighttpd", "/var/log/mail", "/var/log/maillog",
		"/var/log/samba", "/var/log/squid", "/var/log/apache", "/var/log/apache2", "/var/log/dist-upgrade",
		"/var/log/installer", "/var/log/unattended-upgrades", "/var/log/cloud-init.log", "/var/log/cloud-init-output.log",
		"/var/log/landscape", "/var/log/alternatives.log", "/var/log/bootstrap.log", "/var/log/dmesg",
		"/var/log/fsck", "/var/log/glusterfs", "/var/log/jenkins", "/var/log/chrony", "/var/log/ntpstats",
		"/var/log/sysstat", "/var/log/upstart", "/var/log/speech-dispatcher", "/var/log/hp", "/var/log/nvidia-installer.log",
		"/var/log/openvpn", "/var/log/ufw.log", "/var/log/unattended-upgrades", "/var/log/docker", "/var/log/kallithea",
		"/var/log/gitlab", "/var/log/zabbix", "/var/log/supervisor", "/var/log/vbox-install.log", "/var/log/vbox-uninstall.log",
		"/var/log/vbox-setup.log", "/var/log/virtualbox", "/var/log/syslog.1", "/var/log/syslog.2.gz",
	}

	paths := make([]string, numPaths)
	for i := 0; i < numPaths; i++ {
		baseDir := baseDirs[rand.Intn(len(baseDirs))] // 随机选择一个基础目录
		filename := "file" + strconv.Itoa(i) + ".txt"
		paths[i] = filepath.Join(baseDir, filename)
	}
	return paths
}

func TestFindUniqueParentDirsV1(t *testing.T) {
	const numPaths = 100000
	paths := generateMockPaths(numPaths)

	uniqueDirs := findUniqueParentDirs(paths)

	if len(uniqueDirs) == 0 {
		t.Errorf("No unique directories found.")
	}
}
