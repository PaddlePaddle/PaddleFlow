/*
Copyright (c) 2023 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ufs

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs/object"
)

func (fs *objectFileSystem) copyObjectMultipart(from, to string, size int64) error {
	log.Debugf("s3 copyObjectMultipart: key[%s], size[%d]", to, size)
	partSize, _, _ := partAndChunkSize(size)
	nParts := size / partSize
	if size%partSize != 0 {
		nParts++
	}
	etags := make([]*string, nParts)
	resp, err := fs.storage.CreateMultipartUpload(to)
	if err != nil {
		log.Errorf("s3 MPU: key[%s], mpu create err: %v", to, err)
		return err
	}
	mpuID := resp.UploadId
	log.Debugf("s3 MPUx: key[%s], mpuId[%s], nParts[%d], partSize[%d]", to, mpuID, nParts, partSize)

	err = fs.mpuCopyParts(size, from, to, mpuID, partSize, nParts, etags)
	if err != nil {
		log.Errorf("s3 MPU: key[%s], mpuId[%s], mpuCopyParts err: %v", to, mpuID, err)
		return err
	}

	parts := make([]*object.Part, nParts)
	for i := int64(0); i < nParts; i++ {
		parts[i] = &object.Part{
			ETag: *etags[i],
			Num:  i + 1,
		}
	}

	err = fs.storage.CompleteUpload(to, mpuID, parts)

	if err != nil {
		log.Errorf("fh.storage.CompleteUpload: key[%s] err[%v]", to, err)
		return err
	}
	return nil
}

func (fs *objectFileSystem) multipartCopy(from, to, mpuId, bytes_ string, partNum int64, etag **string) error {
	copyPart, err := fs.storage.UploadPartCopy(to, mpuId, bytes_, partNum, from)
	if err != nil {
		log.Errorf("s3 UploadPartCopy: from[%s] to[%s] mpu copy part[%d] err: %v", from, to, partNum, err)
		return err
	}
	log.Debugf("result etag %v", copyPart.ETag)
	*etag = &copyPart.ETag
	return nil
}

func (fs *objectFileSystem) mpuCopyParts(size int64, from, to string, mpuId string, partSize, nparts int64, etags []*string) error {
	log.Tracef("s3 mpuCopyParts: key[%s], fileSize[%d], partCnt[%d]",
		to, size, nparts)

	rangeFrom := int64(0)
	rangeTo := int64(0)

	eg := new(errgroup.Group)
	for i := int64(1); i <= nparts; i++ {
		partNum := i
		rangeFrom = rangeTo
		rangeTo = i * partSize
		if rangeTo > size {
			rangeTo = size
		}
		bytes_ := fmt.Sprintf("bytes=%v-%v", rangeFrom, rangeTo-1)
		eg.Go(func() error {
			if err := fs.multipartCopy(from, to, mpuId, bytes_, partNum, &etags[partNum-1]); err != nil {
				log.Errorf("fh.multipartCopy: from[%s] to[%s] multipartCopy[%d] err[%v]",
					from, to, partNum, err)
				return err
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		log.Errorf("eg.Wait: key[%s] err[%v]", to, err)
		return err
	}
	return nil
}
