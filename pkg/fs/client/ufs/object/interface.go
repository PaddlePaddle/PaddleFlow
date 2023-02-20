package object

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type ItemOutput struct {
	Key          string
	ETag         string
	LastModified time.Time
	Size         uint64
	StorageClass string
}

type HeadObjectOutput struct {
	ItemOutput

	ContentType string
	Metadata    map[string]*string
	IsDir       bool
}

type PrefixOutput struct {
	Prefix string
}

type ListBlobsOutput struct {
	Prefixes              []PrefixOutput
	Items                 []ItemOutput
	NextContinuationToken string
	IsTruncated           bool

	RequestId string
}

type MultipartCommitOutPut struct {
	Key string

	Metadata map[string]string
	UploadId string
	Parts    []*string
	NumParts uint32
}

type Part struct {
	Num  int64
	Size int
	ETag string
}

type PendingPart struct {
	Key      string
	UploadID string
	Created  time.Time
}

type ListInput struct {
	Prefix            string
	Delimiter         string
	MaxKeys           int64
	ContinuationToken string
}

func findLen(in io.Reader) (io.Reader, int64, error) {
	var vlen int64
	switch v := in.(type) {
	case *bytes.Buffer:
		vlen = int64(v.Len())
	case *bytes.Reader:
		vlen = int64(v.Len())
	case *strings.Reader:
		vlen = int64(v.Len())
	case *os.File:
		st, err := v.Stat()
		if err != nil {
			return nil, 0, err
		}
		vlen = st.Size()
	case io.ReadSeeker:
		var err error
		vlen, err = v.Seek(0, 2)
		if err != nil {
			return nil, 0, err
		}
		if _, err = v.Seek(0, 0); err != nil {
			return nil, 0, err
		}
	default:
		d, err := ioutil.ReadAll(in)
		if err != nil {
			return nil, 0, err
		}
		vlen = int64(len(d))
		in = bytes.NewReader(d)
	}
	return in, vlen, nil
}

// ObjectStorage is the interface for object storage.
// all of these API should be idempotent.
type ObjectStorage interface {
	// Description of the object storage.
	String() string
	// Get the data for the given object specified by key.
	Get(key string, off, limit int64) (io.ReadCloser, error)
	// Put data read from a reader to an object specified by key.
	Put(key string, in io.Reader) error
	// Delete a object.
	Deletes(key []string) error
	// Copy Object
	Copy(key, copySource string) error

	// Head returns some information about the object or an error if not found.
	Head(key string) (*HeadObjectOutput, error)
	// List returns a list of objects.
	List(input *ListInput) (*ListBlobsOutput, error)
	// CreateMultipartUpload starts to upload a large object part by part.
	CreateMultipartUpload(key string) (*MultipartCommitOutPut, error)
	// UploadPart upload a part of an object.
	UploadPart(key string, uploadID string, num int64, body []byte) (*Part, error)
	// AbortUpload abort a multipart upload.
	AbortUpload(key string, uploadID string) error
	// CompleteUpload finish an multipart upload.
	CompleteUpload(key string, uploadID string, parts []*Part) error
}
