package mount

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
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

	mountInfo, err := ConstructMountInfo(fsBase64, fsCacheBase64, "target", utils.GetFakeK8sClient(), false)
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
	mountInfo, err = ConstructMountInfo(fsBase64, fsCacheBase64, "target", utils.GetFakeK8sClient(), false)
	assert.Nil(t, err)
	assert.Equal(t, "", mountInfo.CacheConfig.CacheDir)
	assert.Equal(t, "", mountInfo.CacheConfig.FsID)
	assert.Equal(t, "", mountInfo.CacheConfig.MetaDriver)
	assert.Equal(t, 0, mountInfo.CacheConfig.BlockSize)
	assert.Equal(t, false, mountInfo.CacheConfig.Debug)
}

func TestInfo_MountCmdArgs(t *testing.T) {

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
		MetaDriver: "disk",
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

	cfs := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-cfs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "cfs",
		Type:          common.CFSType,
		SubPath:       "abc",
		ServerAddress: "cfs-abcde.cfs.bj.baidubce.com",
	}

	afs := model.FileSystem{
		Model: model.Model{
			ID:        "fs-root-afs",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		UserName:      "root",
		Name:          "afs",
		Type:          common.AFSType,
		SubPath:       "/abc",
		ServerAddress: "afs://xxxx.afs.baidu.com:8806",
		PropertiesMap: map[string]string{
			common.AFSUser:     "root",
			common.AFSPassword: "xxx",
		},
	}

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
	targetPath := "/data/lib/kubelet/pods/1d8f8b01-59b0-4e3f-822d-cc3cff54fd4e/volumes/kubernetes.io~csi/pfs-fs-root-indep-default-pv/mount"
	sourcePath := "/data/lib/kubelet/pods/1d8f8b01-59b0-4e3f-822d-cc3cff54fd4e/volumes/kubernetes.io~csi/pfs-fs-root-indep-default-pv/source"
	type fields struct {
		CacheConfig model.FSCacheConfig
		FS          model.FileSystem
		TargetPath  string
		ReadOnly    bool
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
				TargetPath:  targetPath,
			},
			want: "/home/paddleflow/pfs-fuse mount --mount-point=/home/paddleflow/mnt/storage " + "--fs-id=fs-root-testfs --fs-info=" +
				fsBase64 + " --block-size=4096 --meta-cache-driver=disk --file-mode=0644 --dir-mode=0755 " +
				"--data-cache-path=" + FusePodCachePath + DataCacheDir + " " +
				"--meta-cache-path=" + FusePodCachePath + MetaCacheDir,
		},
		{
			name: "test-pfs-fuse-independent",
			fields: fields{
				FS:          fsInde,
				CacheConfig: fsCache,
				TargetPath:  targetPath,
			},
			want: "/home/paddleflow/mount.sh --fs-id=fs-root-testfs --fs-info=" + fsBase64Inde +
				" --block-size=4096 " +
				"--meta-cache-driver=disk " +
				"--file-mode=0644 --dir-mode=0755 --data-cache-path=/data/paddleflow-FS/mnt/data-cache --meta-cache-path=/data/paddleflow-FS/mnt/meta-cache --mount-point=" + sourcePath,
		},
		{
			name: "test-glusterfs",
			fields: fields{
				FS:          glusterFS,
				CacheConfig: fsCache,
				TargetPath:  targetPath,
			},
			want: "mount -t glusterfs 127.0.0.1:default-volume " + sourcePath,
		},
		{
			name: "test-cfs-mount",
			fields: fields{
				FS:         cfs,
				TargetPath: targetPath,
			},
			want: "mount -t nfs4 -o minorversion=1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport cfs-abcde.cfs.bj.baidubce.com:abc " + sourcePath,
		},
		{
			name: "test-afs-mount",
			fields: fields{
				FS:         afs,
				TargetPath: targetPath,
			},
			want: "/home/paddleflow/afs_mount --username=root --password=xxx afs://xxxx.afs.baidu.com:8806/abc " + sourcePath,
		},
		{
			name: "test-pfs-fuse-no-cache",
			fields: fields{
				FS:          fs,
				CacheConfig: model.FSCacheConfig{},
				TargetPath:  targetPath,
			},
			want: "/home/paddleflow/pfs-fuse mount --mount-point=/home/paddleflow/mnt/storage " +
				"--fs-id=fs-root-testfs --fs-info=" + fsBase64 + " --file-mode=0644 --dir-mode=0755",
		},
		{
			name: "test-pfs-fuse-cache-readOnly",
			fields: fields{
				FS:          fs,
				CacheConfig: fsCache,
				TargetPath:  targetPath,
				ReadOnly:    true,
			},
			want: "/home/paddleflow/pfs-fuse mount --mount-point=/home/paddleflow/mnt/storage " +
				"--fs-id=fs-root-testfs --fs-info=" + fsBase64 + " --mount-options=ro --block-size=4096 --meta-cache-driver=disk --file-mode=0644 --dir-mode=0755 " +
				"--data-cache-path=" + FusePodCachePath + DataCacheDir + " " +
				"--meta-cache-path=" + FusePodCachePath + MetaCacheDir,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fsStr, err := json.Marshal(tt.fields.FS)
			assert.Nil(t, err)
			fsBase64 := base64.StdEncoding.EncodeToString(fsStr)

			fsCacheStr, err := json.Marshal(tt.fields.CacheConfig)
			assert.Nil(t, err)
			fsCacheBase64 := base64.StdEncoding.EncodeToString(fsCacheStr)

			mountInfo, err := ConstructMountInfo(fsBase64, fsCacheBase64, tt.fields.TargetPath, utils.GetFakeK8sClient(), tt.fields.ReadOnly)
			assert.Nil(t, err)

			got := mountInfo.Cmd + " " + strings.Join(mountInfo.Args, " ")
			if got != tt.want {
				t.Errorf("cmdAndArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}
