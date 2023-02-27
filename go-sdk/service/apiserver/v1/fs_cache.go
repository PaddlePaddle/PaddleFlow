package v1

import (
	"context"

	"github.com/PaddlePaddle/PaddleFlow/pkg/apiserver/common"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/core"
	"github.com/PaddlePaddle/PaddleFlow/pkg/common/http/util/http"
	"github.com/PaddlePaddle/PaddleFlow/pkg/model"
)

type fileSystemCache struct {
	client *core.PaddleFlowClient
}

type CreateFileSystemCacheRequest struct {
	Username            string                 `json:"username"`
	FsName              string                 `json:"fsName"`
	FsID                string                 `json:"-"`
	CacheDir            string                 `json:"cacheDir"`
	Quota               int                    `json:"quota"`
	MetaDriver          string                 `json:"metaDriver"`
	BlockSize           int                    `json:"blockSize"`
	Debug               bool                   `json:"debug"`
	CleanCache          bool                   `json:"cleanCache"`
	Resource            model.ResourceLimit    `json:"resource"`
	NodeTaintToleration map[string]interface{} `json:"nodeTaintToleration"`
	ExtraConfig         map[string]string      `json:"extraConfig"`
}

type GetFileSystemCacheRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
}

type GetFileSystemCacheResponse struct {
	CacheDir            string                 `json:"cacheDir"`
	Quota               int                    `json:"quota"`
	MetaDriver          string                 `json:"metaDriver"`
	BlockSize           int                    `json:"blockSize"`
	CleanCache          bool                   `json:"cleanCache"`
	Resource            model.ResourceLimit    `json:"resource"`
	NodeTaintToleration map[string]interface{} `json:"nodeTaintToleration"`
	ExtraConfig         map[string]string      `json:"extraConfig"`
	FsName              string                 `json:"fsName"`
	Username            string                 `json:"username"`
	CreateTime          string                 `json:"createTime"`
	UpdateTime          string                 `json:"updateTime,omitempty"`
}

type DeleteFileSystemCacheRequest struct {
	FsName   string `json:"fsName"`
	Username string `json:"username"`
}

func (f *fileSystemCache) Create(ctx context.Context, request *CreateFileSystemCacheRequest,
	token string) (err error) {
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsCacheApi).
		WithMethod(http.POST).
		WithBody(request).
		Do()
	return
}

func (f *fileSystemCache) Get(ctx context.Context, request *GetFileSystemCacheRequest,
	token string) (result *GetFileSystemCacheResponse, err error) {
	result = &GetFileSystemCacheResponse{}
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsCacheApi+"/"+request.FsName).
		WithQueryParam(KeyUsername, request.Username).
		WithMethod(http.GET).
		WithResult(result).
		Do()
	if err != nil {
		return nil, err
	}
	return
}

func (f *fileSystemCache) Delete(ctx context.Context, request *DeleteFileSystemCacheRequest, token string) (err error) {
	err = core.NewRequestBuilder(f.client).
		WithHeader(common.HeaderKeyAuthorization, token).
		WithURL(FsCacheApi+"/"+request.FsName).
		WithQueryParam(KeyUsername, request.Username).
		WithMethod(http.DELETE).
		Do()
	return
}

type FileSystemCacheGetter interface {
	FileSystemCache() FileSystemCacheInterface
}

type FileSystemCacheInterface interface {
	Create(ctx context.Context, request *CreateFileSystemCacheRequest, token string) error
	Get(ctx context.Context, request *GetFileSystemCacheRequest, token string) (*GetFileSystemCacheResponse, error)
	Delete(ctx context.Context, request *DeleteFileSystemCacheRequest, token string) error
}

// newFileSystemCache returns a fileSystem cache.
func newFileSystemCache(c *APIV1Client) *fileSystemCache {
	return &fileSystemCache{
		client: c.RESTClient(),
	}
}
