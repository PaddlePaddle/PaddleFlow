package fs

import (
	"math/rand"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/cache"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/kv"
	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/common"
)

const (
	HDFS_serverAddress = "hdfs_serverAddress"
	HDFS_subPath       = "hdfs_subPath"
	HDFS_group         = "hdfs_group"
	HDFS_user          = "hdfs_user"
)

func TestHDFS(t *testing.T) {
	if os.Getenv(HDFS_serverAddress) == "" || os.Getenv(HDFS_group) == "" || os.Getenv(HDFS_user) == "" {
		log.Info("not ready")
		t.SkipNow()
	}
	rand.Seed(time.Now().UnixNano())
	d := cache.Config{
		BlockSize:    (1 + rand.Intn(100)) * 1024 * 1024,
		MaxReadAhead: 1 + rand.Intn(300*1024*1024),
		Expire:       600 * time.Second,
		Config: kv.Config{
			Driver: kv.MemType,
		},
	}
	log.Infof("test hdfs cache %+v", d)
	SetDataCache(d)
	client := getHDFSClient(t)
	defer func() {
		// err := client.Remove(testBigFileName)
		// assert.Equal(t, nil, err)
		// err = client.Remove(testSmallFileName)
		// assert.Equal(t, nil, err)
		// os.Remove(testBigFileName)
		os.RemoveAll("./tmp")
		os.RemoveAll("./mock-cache")
	}()

	chown(t, client)
	//testBigFile(t, client)
	// testSmallFile(t, client)
	// testMkdirAndList(t, client)
}

func chown(t *testing.T, client FSClient) {
	n, err := client.CreateFile("test.txt", []byte("test"))
	assert.Equal(t, n, 4)
	assert.Equal(t, nil, err)
	defer func() {
		err = client.Remove("test.txt")
		assert.Equal(t, nil, err)
	}()
	err = client.Chown("test.txt", 0, 601)
	assert.Equal(t, nil, err)
	_, err = client.Stat("test.txt")
	assert.Equal(t, nil, err)
}

func getHDFSClient(t *testing.T) FSClient {
	testFsMeta := common.FSMeta{
		UfsType:       common.HDFSType,
		Type:          common.HDFSType,
		ServerAddress: os.Getenv(HDFS_serverAddress),
		Properties: map[string]string{
			common.UserKey: os.Getenv(HDFS_user),
			common.Group:   os.Getenv(HDFS_group),
			common.Address: os.Getenv(HDFS_serverAddress),
		},
		SubPath: os.Getenv(HDFS_subPath),
	}
	DataCachePath = "./mock-cache"
	fsclient, err := NewFSClientForTest(testFsMeta)
	assert.Equal(t, nil, err)
	return fsclient
}
