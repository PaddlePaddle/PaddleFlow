package mount

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
		SubPath:       "/elsies3",
		ServerAddress: "s3.bj.bcebos.com",

		PropertiesMap: map[string]string{
			"accessKey": "529dc59c70314dbeabf86d3972c66238",
			"bucket":    "easydata-test",
			"endpoint":  "s3.bj.bcebos.com",
			"region":    "bj",
			"secretKey": "a93c7ea50056869d72960a6a5be74084df3f70f25d9f7fa2154e5625d6c63a04f6f0a15ed34a4afac90a3cbc300c9d91"},
	}
	err := storage.Filesystem.CreatFileSystem(&fs)
	assert.Nil(t, err)
	fsStr, err := json.Marshal(fs)
	assert.Nil(t, err)
	fmt.Printf("\nfsStr: %s\n", fsStr)
	fsBase64 := base64.StdEncoding.EncodeToString(fsStr)
	fmt.Printf("\nfsBase64: %s\n", fsBase64)

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
	fmt.Printf("\nfsCacheStr: %s\n", fsCacheStr)
	fsCacheBase64 := base64.StdEncoding.EncodeToString(fsCacheStr)
	fmt.Printf("\nfsCacheBase64: %s\n", fsCacheBase64)

	mountInfo, err := ProcessMountInfo(fs.ID, "server", fsBase64, fsCacheBase64, false)
	assert.Nil(t, err)
	assert.Equal(t, mountInfo.FsBase64Str, string(fsStr))
	assert.Equal(t, mountInfo.FsCacheConfig.CacheDir, fsCache.CacheDir)
	assert.Equal(t, mountInfo.FsCacheConfig.FsID, fsCache.FsID)
	assert.Equal(t, mountInfo.FsCacheConfig.MetaDriver, fsCache.MetaDriver)
	assert.Equal(t, mountInfo.FsCacheConfig.BlockSize, fsCache.BlockSize)
	fmt.Printf("\nmountInfo: %+v\n", mountInfo)
}
