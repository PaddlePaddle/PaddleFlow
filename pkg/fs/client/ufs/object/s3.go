package object

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
)

const S3Name = "s3"

type S3Storage struct {
	bucket string
	s3     *s3.S3
}

func NewS3Storage(bucket string, s3 *s3.S3) S3Storage {
	return S3Storage{bucket: bucket, s3: s3}
}

func (storage S3Storage) String() string {
	return S3Name
}

func (storage S3Storage) Get(key string, off, limit int64) (io.ReadCloser, error) {
	log.Tracef("s3.GetObject[%s] off[%d] limit[%d]", key, off, limit)
	request := &s3.GetObjectInput{
		Bucket: &storage.bucket,
		Key:    &key,
	}
	// Range: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	if limit > 0 {
		endPos := off + limit
		r := fmt.Sprintf("bytes=%d-%d", off, endPos-1)
		request.Range = &r
	} else if off > 0 {
		r := fmt.Sprintf("bytes=%d-", off)
		request.Range = &r
	}

	response, err := storage.s3.GetObject(request)
	if err != nil {
		log.Debugf("s3.GetObject[%s] off[%d] limit[%d] err: %v ", key, off, limit, err)
		return nil, err
	}
	return response.Body, nil
}

func (storage S3Storage) Put(key string, in io.Reader) error {
	log.Tracef("s3.PutObject[%s]", key)
	var body io.ReadSeeker
	if in == nil {
		body = nil
	} else if b, ok := in.(io.ReadSeeker); ok {
		body = b
	} else {
		data, err := ioutil.ReadAll(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}

	request := &s3.PutObjectInput{
		Bucket: &storage.bucket,
		Key:    aws.String(key),
		Body:   body,
	}
	_, err := storage.s3.PutObject(request)
	if err != nil {
		log.Errorf("s3.PutObject[%s] err: %v", key, err)
	}
	return err
}

func (storage S3Storage) Deletes(keys []string) error {
	log.Tracef("s3.Deletes keys[%v]", keys)
	numObjs := len(keys)
	if numObjs == 0 {
		log.Errorf("delete keys empty")
		return fmt.Errorf("delete keys empty")
	}

	var items s3.Delete
	var objs = make([]*s3.ObjectIdentifier, numObjs)

	for i, _ := range keys {
		objs[i] = &s3.ObjectIdentifier{Key: &keys[i]}
	}

	// Add list of objects to delete object
	items.SetObjects(objs)
	_, err := storage.s3.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: &storage.bucket,
		Delete: &items,
	})
	if err != nil {
		log.Errorf("s3.Deletes keys[%v] err: %v", keys, err)
	}
	return err
}

func (storage S3Storage) Copy(newKey, copySource string) error {
	log.Tracef("s3.Copy newKey[%s] copSource[%s]", newKey, copySource)
	copySource_ := storage.bucket + "/" + copySource
	request := &s3.CopyObjectInput{
		Bucket:     &storage.bucket,
		Key:        &newKey,
		CopySource: &copySource_,
	}
	_, err := storage.s3.CopyObject(request)
	if err != nil {
		log.Errorf("s3.Copy newKey[%s] copSource[%s] err: %v", newKey, copySource, err)
		return err
	}
	return nil
}

func (storage S3Storage) Head(key string) (*HeadObjectOutput, error) {
	log.Tracef("s3.Head key[%s]", key)
	input := &s3.HeadObjectInput{
		Bucket: &storage.bucket,
		Key:    &key,
	}
	response, err := storage.s3.HeadObject(input)
	if err != nil {
		log.Debugf("s3.Head key[%s] error: %v", key, err)
		return nil, err
	}
	var etag, storageClass string
	var lastModified time.Time
	var size uint64

	if response.ETag != nil {
		etag = *response.ETag
	}
	if response.LastModified != nil {
		lastModified = *response.LastModified
	}
	if response.ContentLength != nil {
		size = uint64(*response.ContentLength)
	}
	if response.StorageClass != nil {
		storageClass = *response.StorageClass
	}

	return &HeadObjectOutput{
		ItemOutput: ItemOutput{
			Key:          key,
			ETag:         etag,
			LastModified: lastModified,
			Size:         size,
			StorageClass: storageClass,
		},
		ContentType: *response.ContentType,
		Metadata:    metadataToLower(response.Metadata),
		IsDir:       strings.HasSuffix(key, "/"),
	}, nil
}

func (storage S3Storage) List(input *ListInput) (*ListBlobsOutput, error) {
	log.Tracef("s3.List param[%+v]", input)
	request := &s3.ListObjectsV2Input{
		Bucket:            &storage.bucket,
		Prefix:            &input.Prefix,
		MaxKeys:           &input.MaxKeys,
		ContinuationToken: &input.ContinuationToken,
		Delimiter:         &input.Delimiter,
	}

	resp, err := storage.s3.ListObjectsV2(request)

	if err != nil {
		log.Errorf("s3.input[%+v] err: %v", input, err)
		return nil, err
	}
	prefixes := make([]PrefixOutput, 0)
	items := make([]ItemOutput, 0)

	for _, p := range resp.CommonPrefixes {
		prefixes = append(prefixes, PrefixOutput{Prefix: *p.Prefix})
	}
	var key, etag, storageClass string
	var lastModified time.Time
	var size uint64
	for _, i := range resp.Contents {
		if i.Key != nil {
			key = *i.Key
		}
		if i.ETag != nil {
			etag = *i.ETag
		}
		if i.LastModified != nil {
			lastModified = *i.LastModified
		}
		if i.Size != nil {
			size = uint64(*i.Size)
		}
		if i.StorageClass != nil {
			storageClass = *i.StorageClass
		}
		items = append(items, ItemOutput{
			Key:          key,
			ETag:         etag,
			LastModified: lastModified,
			Size:         size,
			StorageClass: storageClass,
		})
	}
	var nextContinuationToken string
	if resp.NextContinuationToken != nil {
		nextContinuationToken = *resp.NextContinuationToken
	}
	var IsTruncated bool
	if resp.IsTruncated != nil {
		IsTruncated = *resp.IsTruncated
	}

	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: nextContinuationToken,
		IsTruncated:           IsTruncated,
	}, nil
}

func (storage S3Storage) CreateMultipartUpload(key string) (*MultipartCommitOutPut, error) {
	log.Tracef("s3.CreateMultipartUpload key[%s]", key)
	mpu := s3.CreateMultipartUploadInput{
		Bucket: &storage.bucket,
		Key:    &key,
	}

	resp, err := storage.s3.CreateMultipartUpload(&mpu)
	if err != nil {
		log.Errorf("s3.CreateMultipartUpload key[%s] err: %v", key, err)
		return nil, err
	}
	var uploadId string
	if resp.UploadId != nil {
		uploadId = *resp.UploadId
	}
	return &MultipartCommitOutPut{
		Key:      key,
		UploadId: uploadId,
		Parts:    make([]*string, 10000), // at most 10K parts
	}, nil
}

func (storage S3Storage) UploadPart(key string, uploadID string, num int64, body []byte) (*Part, error) {
	log.Tracef("s3.UploadPart key[%s] uploadID[[%v] num[%d]", key, uploadID, num)
	mpu := s3.UploadPartInput{
		Bucket:     &storage.bucket,
		Key:        &key,
		PartNumber: &num,
		UploadId:   &uploadID,
	}
	// retry up to 3 times if upload a mpu failed
	var err error
	var resp *s3.UploadPartOutput
	mpu.Body = bytes.NewReader(body)
	resp, err = storage.s3.UploadPart(&mpu)
	if err != nil {
		log.Errorf("s3 mpu upload: fh.name[%s], upload part[%v] failed. err: %v", key, mpu, err)
		return nil, err
	}
	return &Part{Num: num, ETag: *resp.ETag}, nil
}

func (storage S3Storage) AbortUpload(key string, uploadID string) error {
	log.Tracef("s3.AbortUpload key[%s] uploadID[[%v]", key, uploadID)
	params := &s3.AbortMultipartUploadInput{
		Bucket:   &storage.bucket,
		Key:      &key,
		UploadId: &uploadID,
	}
	_, err := storage.s3.AbortMultipartUpload(params)
	if err != nil {
		log.Errorf("s3.AbortUpload key[%s], uploadID[[%v] err: %v", key, uploadID, err)
	}
	return err
}

func (storage S3Storage) CompleteUpload(key string, uploadID string, parts []*Part) error {
	log.Tracef("s3.CompleteUpload key[%s] uploadID[[%v]", key, uploadID)
	var s3Parts []*s3.CompletedPart
	for i := range parts {
		n := new(int64)
		*n = parts[i].Num
		s3Parts = append(s3Parts, &s3.CompletedPart{ETag: &parts[i].ETag, PartNumber: n})
	}
	params := &s3.CompleteMultipartUploadInput{
		Bucket:          &storage.bucket,
		Key:             &key,
		UploadId:        &uploadID,
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: s3Parts},
	}
	_, err := storage.s3.CompleteMultipartUpload(params)
	return err
}

func metadataToLower(m map[string]*string) map[string]*string {
	if m != nil {
		var toDelete []string
		for k, v := range m {
			lower := strings.ToLower(k)
			if lower != k {
				m[lower] = v
				toDelete = append(toDelete, k)
			}
		}
		for _, k := range toDelete {
			delete(m, k)
		}
	}
	return m
}

func metadataToLowerBos(m map[string]string) map[string]*string {
	result := make(map[string]*string)
	if m != nil {
		var toDelete []string
		for k, v := range m {
			lower := strings.ToLower(k)
			if lower != k {
				result[lower] = &v
				toDelete = append(toDelete, k)
			}
		}
		for _, k := range toDelete {
			delete(m, k)
		}
	}
	return result
}

var _ ObjectStorage = (*S3Storage)(nil)
