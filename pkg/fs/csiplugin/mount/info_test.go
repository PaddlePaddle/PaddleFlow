package mount

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage"
	"github.com/PaddlePaddle/PaddleFlow/pkg/storage/driver"
)

func TestKubeRuntimePVAndPVC(t *testing.T) {
	driver.InitMockDB()

	fs := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-testfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "testfs",
		Type:          "s3",
		SubPath:       "/supath",
		ServerAddress: "server_address",

		PropertiesMap: map[string]string{
			"accessKey": "accessKey",
			"bucket":    "bucket",
			"endpoint":  "server_address",
			"region":    "bj",
			"secretKey": "secretKey"},
	}
	err := storage.Filesystem.CreatFileSystem(&fs)
	assert.Nil(t, err)
	fsStr, err := json.Marshal(fs)
	assert.Nil(t, err)
	fsBase64 := base64.StdEncoding.EncodeToString(fsStr)

	fsCache := model.FSCacheConfig{
		FsID:       fs.ID,
		CacheDir:   "/data/paddleflow-fs/mnt",
		MetaDriver: "nutsdb",
		BlockSize:  4096,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	err = storage.Filesystem.CreateFSCacheConfig(&fsCache)
	assert.Nil(t, err)
	fsCacheStr, err := json.Marshal(fsCache)
	assert.Nil(t, err)
	fsCacheBase64 := base64.StdEncoding.EncodeToString(fsCacheStr)

	mountInfo, err := ProcessMountInfo(fsBase64, fsCacheBase64, "target", false)
	assert.Nil(t, err)
	assert.Equal(t, fsBase64, mountInfo.FSBase64Str)
	assert.Equal(t, fsCache.CacheDir, mountInfo.CacheConfig.CacheDir)
	assert.Equal(t, fsCache.FsID, mountInfo.CacheConfig.FsID)
	assert.Equal(t, fsCache.MetaDriver, mountInfo.CacheConfig.MetaDriver)
	assert.Equal(t, fsCache.BlockSize, mountInfo.CacheConfig.BlockSize)

	// no config
	fsCache = model.FSCacheConfig{}
	fsCacheStr, err = json.Marshal(fsCache)
	assert.Nil(t, err)
	fsCacheBase64 = base64.StdEncoding.EncodeToString(fsCacheStr)
	mountInfo, err = ProcessMountInfo(fsBase64, fsCacheBase64, "target", false)
	assert.Nil(t, err)
	assert.Equal(t, "", mountInfo.CacheConfig.CacheDir)
	assert.Equal(t, "", mountInfo.CacheConfig.FsID)
	assert.Equal(t, "", mountInfo.CacheConfig.MetaDriver)
	assert.Equal(t, 0, mountInfo.CacheConfig.BlockSize)
	assert.Equal(t, false, mountInfo.CacheConfig.Debug)
}

func TestGetOptions(t *testing.T) {
	fs := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-testfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "testfs",
		Type:          "s3",
		SubPath:       "/supath",
		ServerAddress: "server_address",

		PropertiesMap: map[string]string{
			"accessKey": "accessKey",
			"bucket":    "bucket",
			"endpoint":  "server_address",
			"region":    "bj",
			"secretKey": "secretKey"},
	}

	fsStr, err := json.Marshal(fs)
	assert.Nil(t, err)
	fsBase64 := base64.StdEncoding.EncodeToString(fsStr)

	fsCache := model.FSCacheConfig{
		FsID:       fs.ID,
		CacheDir:   "/data/paddleflow-FS/mnt",
		MetaDriver: "leveldb",
		BlockSize:  4096,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	type args struct {
		mountInfo Info
		readOnly  bool
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "test-pfs-fuse-no-cache",
			args: args{
				mountInfo: Info{
					FS:          fs,
					FSBase64Str: fsBase64,
					TargetPath:  "/target/testPath",
				},
			},
			want: []string{"--fs-info=eyJpZCI6ImZzLXJvb3QtdGVzdGZzIiwiY3JlYXRlVGltZSI6IiIsIm5hbWUiOiJ0ZXN0ZnMiLCJ0eXBlIjoiczMiLCJzZXJ2ZXJBZGRyZXNzIjoic2VydmVyX2FkZHJlc3MiLCJzdWJQYXRoIjoiL3N1cGF0aCIsInByb3BlcnRpZXMiOnsiYWNjZXNzS2V5IjoiYWNjZXNzS2V5IiwiYnVja2V0IjoiYnVja2V0IiwiZW5kcG9pbnQiOiJzZXJ2ZXJfYWRkcmVzcyIsInJlZ2lvbiI6ImJqIiwic2VjcmV0S2V5Ijoic2VjcmV0S2V5In0sInVzZXJOYW1lIjoicm9vdCIsImluZGVwZW5kZW50TW91bnRQcm9jZXNzIjpmYWxzZX0=",
				"--fs-id=fs-root-testfs", "--file-mode=0666", "--dir-mode=0777"},
		},
		{
			name: "test-pfs-fuse-cache",
			args: args{
				mountInfo: Info{
					FS:          fs,
					FSBase64Str: fsBase64,
					TargetPath:  "/target/testPath",
					CacheConfig: fsCache,
				},
			},
			want: []string{"--fs-info=" + fsBase64,
				"--fs-id=fs-root-testfs", "--block-size=4096", "--data-cache-path=" + FusePodCachePath + DataCacheDir,
				"--meta-cache-driver=leveldb", "--meta-cache-path=" + FusePodCachePath + MetaCacheDir,
				"--file-mode=0666", "--dir-mode=0777"},
		},
		{
			name: "test-pfs-fuse-cache-readOnly",
			args: args{
				mountInfo: Info{
					FS:          fs,
					FSBase64Str: fsBase64,
					TargetPath:  "/target/testPath",
					CacheConfig: fsCache,
				},
				readOnly: true,
			},
			want: []string{"--fs-info=" + fsBase64,
				"--fs-id=fs-root-testfs", "--mount-options=ro", "--block-size=4096", "--data-cache-path=" + FusePodCachePath + DataCacheDir,
				"--meta-cache-driver=leveldb", "--meta-cache-path=" + FusePodCachePath + MetaCacheDir,
				"--file-mode=0666", "--dir-mode=0777"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetOptions(tt.args.mountInfo, tt.args.readOnly); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInfo_MountCmd(t *testing.T) {
	targetPath := "/targetPath/test"

	fs := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-testfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "testfs",
		Type:          "s3",
		SubPath:       "/supath",
		ServerAddress: "server_address",

		PropertiesMap: map[string]string{
			"accessKey":     "accessKey",
			"bucket":        "bucket",
			"endpoint":      "server_address",
			"region":        "bj",
			"secretKey":     "secretKey",
			common.DirMode:  "0755",
			common.FileMode: "0644",
		},
	}

	fsStr, err := json.Marshal(fs)
	assert.Nil(t, err)
	fsBase64 := base64.StdEncoding.EncodeToString(fsStr)

	fsCache := model.FSCacheConfig{
		FsID:       fs.ID,
		CacheDir:   "/data/paddleflow-FS/mnt",
		MetaDriver: "leveldb",
		BlockSize:  4096,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	glusterFS := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-glusterfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "glusterfs",
		Type:          common.GlusterFSType,
		SubPath:       "default-volume",
		ServerAddress: "127.0.0.1",
	}
	glusterfsInfo := Info{
		FS:         glusterFS,
		TargetPath: targetPath,
	}
	glusterfsOption := GetOptions(glusterfsInfo, false)

	fsInde := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-testfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:                "root",
		Name:                    "testfs",
		Type:                    "s3",
		SubPath:                 "/supath",
		ServerAddress:           "server_address",
		IndependentMountProcess: true,

		PropertiesMap: map[string]string{
			"accessKey":     "accessKey",
			"bucket":        "bucket",
			"endpoint":      "server_address",
			"region":        "bj",
			"secretKey":     "secretKey",
			common.DirMode:  "0755",
			common.FileMode: "0644",
		},
	}
	fsStr2, err := json.Marshal(fsInde)
	assert.Nil(t, err)
	fsBase64Inde := base64.StdEncoding.EncodeToString(fsStr2)

	info := Info{
		CacheConfig: fsCache,
		FS:          fs,
		FSBase64Str: fsBase64,
		TargetPath:  targetPath,
	}
	options := GetOptions(info, false)

	infoInde := Info{
		CacheConfig: fsCache,
		FS:          fsInde,
		FSBase64Str: fsBase64Inde,
		TargetPath:  targetPath,
	}
	optionsInde := GetOptions(infoInde, false)
	type fields struct {
		CacheConfig model.FSCacheConfig
		FS          model.FileSystem
		FSBase64Str string
		TargetPath  string
		Options     []string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "test-pfs-fuse-pod",
			fields: fields{
				FS:          fs,
				CacheConfig: fsCache,
				FSBase64Str: fsBase64,
				TargetPath:  "/targetPath/test",
				Options:     options,
			},
			want: "/home/paddleflow/pfs-fuse mount --mount-point=/home/paddleflow/mnt/storage " +
				"--fs-info=" + fsBase64 + " --fs-id=fs-root-testfs --block-size=4096 " +
				"--data-cache-path=" + FusePodCachePath + DataCacheDir + " --meta-cache-driver=leveldb " +
				"--meta-cache-path=" + FusePodCachePath + MetaCacheDir + " --file-mode=0644 --dir-mode=0755",
		},
		{
			name: "test-pfs-fuse-independent",
			fields: fields{
				FS:          fsInde,
				CacheConfig: fsCache,
				FSBase64Str: fsBase64,
				TargetPath:  "/targetPath/test",
				Options:     optionsInde,
			},
			want: "/home/paddleflow/mount.sh --mount-point=/targetPath/test --fs-info=" + fsBase64Inde +
				" --fs-id=fs-root-testfs --block-size=4096 --data-cache-path=/data/paddleflow-FS/mnt " +
				"--meta-cache-driver=leveldb --meta-cache-path=/data/paddleflow-FS/mnt " +
				"--file-mode=0644 --dir-mode=0755",
		},
		{
			name: "test-glusterfs",
			fields: fields{
				FS:         glusterFS,
				Options:    glusterfsOption,
				TargetPath: targetPath,
			},
			want: "mount-t glusterfs 127.0.0.1:default-volume /targetPath/test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Info{
				CacheConfig: tt.fields.CacheConfig,
				FS:          tt.fields.FS,
				FSBase64Str: tt.fields.FSBase64Str,
				TargetPath:  tt.fields.TargetPath,
				Options:     tt.fields.Options,
			}
			cmd, args := m.MountCmd()
			got := cmd + strings.Join(args, " ")
			if got != tt.want {
				t.Errorf("MountCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}
