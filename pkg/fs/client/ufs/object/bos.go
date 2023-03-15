package object

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/baidubce/bce-sdk-go/bce"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/baidubce/bce-sdk-go/services/sts"
	sts_api "github.com/baidubce/bce-sdk-go/services/sts/api"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	BosName = "bos"
)

type Bos struct {
	bucket    string
	bosClient *bos.Client
}

func StsSessionToken(ak string, sk string, duration int, acl string) (*sts_api.GetSessionTokenResult, error) {
	stsClient, err := sts.NewClient(ak, sk)
	if err != nil {
		log.Errorf("create sts client object: %v", err)
		return nil, err
	}

	result, err := stsClient.GetSessionToken(duration, acl)
	if err != nil {
		log.Errorf("get session token failed: %v", err)
		return nil, err
	}
	return result, nil
}

func NewBosClient(bucket string, bos *bos.Client) Bos {
	return Bos{bucket: bucket, bosClient: bos}
}

func (storage Bos) String() string {
	return BosName
}

func (storage Bos) Get(key string, off, limit int64) (io.ReadCloser, error) {
	log.Tracef("bosClient.GetObject[%s] off[%d] limit[%d]", key, off, limit)
	res, err := storage.bosClient.GetObject(storage.bucket, key, map[string]string{}, off, off+limit)
	if err != nil {
		log.Debugf("bos.GetObject[%s] off[%d] limit[%d] err: %v ", key, off, limit, err)
		return nil, err
	}
	return res.Body, nil
}

func (storage Bos) Put(key string, in io.Reader) error {
	log.Tracef("bos.Put key[%s]", key)
	body := &bce.Body{}
	var err error
	if in != nil {
		b, vlen, err := findLen(in)
		if err != nil {
			return err
		}
		body, err = bce.NewBodyFromSizedReader(b, vlen)
		if err != nil {
			return err
		}
	}
	_, err = storage.bosClient.BasicPutObject(storage.bucket, key, body)
	return err
}

func (storage Bos) Deletes(keys []string) error {
	log.Tracef("bos.Deletes keys[%v]", keys)
	numObjs := len(keys)
	if numObjs == 0 {
		log.Errorf("delete keys empty")
		return fmt.Errorf("delete keys empty")
	}
	var group errgroup.Group
	for i := 0; i < numObjs; i++ {
		tmp := i
		group.Go(func() error {
			err := storage.bosClient.DeleteObject(storage.bucket, keys[tmp])
			return err
		})
	}
	err := group.Wait()
	if err != nil {
		log.Errorf("bos.Deletes keys[%v] err: %v", keys, err)
		return err
	}
	return nil
}

func (storage Bos) Copy(newKey, copySource string) error {
	log.Tracef("bos.Copy newKey[%s] copSource[%s]", newKey, copySource)
	_, err := storage.bosClient.BasicCopyObject(storage.bucket, newKey, storage.bucket, copySource)
	if err != nil {
		log.Errorf("bos.Copy newKey[%s] copSource[%s] err: %v", newKey, copySource, err)
		return err
	}
	return nil
}

func (storage Bos) Head(key string) (*HeadObjectOutput, error) {
	log.Tracef("bos.Head key[%s]", key)
	response, err := storage.bosClient.GetObjectMeta(storage.bucket, key)
	if err != nil {
		log.Debugf("bos.Head key[%s] error: %v", key, err)
		return nil, err
	}
	mtime, _ := time.Parse(time.RFC1123, response.LastModified)
	return &HeadObjectOutput{
		ItemOutput: ItemOutput{
			Key:          key,
			ETag:         response.ETag,
			LastModified: mtime,
			Size:         uint64(response.ContentLength),
			StorageClass: response.StorageClass,
		},
		ContentType: response.ContentType,
		Metadata:    metadataToLowerBos(response.UserMeta),
		IsDir:       strings.HasSuffix(key, "/"),
	}, nil
}

func (storage Bos) List(input *ListInput) (*ListBlobsOutput, error) {
	log.Tracef("bos.List param[%+v]", input)
	request := &api.ListObjectsArgs{
		Prefix:    input.Prefix,
		MaxKeys:   int(input.MaxKeys),
		Marker:    input.ContinuationToken,
		Delimiter: "",
	}
	if input.Delimiter != "" {
		request.Delimiter = input.Delimiter
	}

	resp, err := storage.bosClient.ListObjects(storage.bucket, request)
	if err != nil {
		log.Errorf("bos.input[%+v] err: %v", input, err)
		return nil, err
	}
	prefixes := make([]PrefixOutput, 0)
	items := make([]ItemOutput, 0)

	for _, p := range resp.CommonPrefixes {
		tmp := p
		prefixes = append(prefixes, PrefixOutput{Prefix: tmp.Prefix})
	}
	for _, i := range resp.Contents {
		mod, _ := time.Parse("2006-01-02T15:04:05Z", i.LastModified)
		items = append(items, ItemOutput{
			Key:          i.Key,
			ETag:         i.ETag,
			LastModified: mod,
			Size:         uint64(i.Size),
			StorageClass: i.StorageClass,
		})
	}

	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: resp.NextMarker,
		IsTruncated:           resp.IsTruncated,
	}, nil
}

func (storage Bos) CreateMultipartUpload(key string) (*MultipartCommitOutPut, error) {
	log.Tracef("bos.CreateMultipartUpload key[%s]", key)
	resp, err := storage.bosClient.BasicInitiateMultipartUpload(storage.bucket, key)

	if err != nil {
		log.Errorf("bos.CreateMultipartUpload key[%s] err: %v", key, err)
		return nil, err
	}
	return &MultipartCommitOutPut{
		Key:      key,
		UploadId: resp.UploadId,
		Parts:    make([]*string, 10000), // at most 10K parts
	}, nil
}

func (storage Bos) UploadPart(key string, uploadID string, num int64, body []byte) (*Part, error) {
	log.Tracef("bos.UploadPart key[%s] uploadID[[%v] num[%d]", key, uploadID, num)
	var err error
	newBody, _ := bce.NewBodyFromBytes(body)

	etag, err := storage.bosClient.BasicUploadPart(storage.bucket, key, uploadID, int(num), newBody)

	if err != nil {
		log.Errorf("bos mpu upload: fh.name[%s] uploadID[%s] num[%d] failed. err: %v", key, uploadID, num, err)
		return nil, err
	}
	return &Part{Num: num, ETag: etag}, nil
}

func (storage Bos) AbortUpload(key string, uploadID string) error {
	log.Tracef("bos.AbortUpload key[%s] uploadID[[%v]", key, uploadID)
	err := storage.bosClient.AbortMultipartUpload(storage.bucket, key, uploadID)
	if err != nil {
		log.Errorf("bos.AbortUpload key[%s], uploadID[[%v] err: %v", key, uploadID, err)
	}
	return err
}

func (storage Bos) CompleteUpload(key string, uploadID string, parts []*Part) error {
	log.Tracef("bos.CompleteUpload key[%s] uploadID[[%v]", key, uploadID)
	oparts := make([]api.UploadInfoType, len(parts))
	for i := range parts {
		oparts[i] = api.UploadInfoType{
			PartNumber: int(parts[i].Num),
			ETag:       parts[i].ETag,
		}
	}
	ps := api.CompleteMultipartUploadArgs{Parts: oparts}
	_, err := storage.bosClient.CompleteMultipartUploadFromStruct(storage.bucket, key, uploadID, &ps)
	return err
}

var _ ObjectStorage = (*Bos)(nil)
