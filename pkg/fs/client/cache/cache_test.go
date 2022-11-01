package cache

import (
	"fmt"
	. "github.com/agiledragon/gomonkey/v2"
	"reflect"
	"sync"
	"testing"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/client/ufs"
)

func Test_rCache_readFromReadAhead(t *testing.T) {
	type fields struct {
		id            string
		flags         uint32
		length        int
		store         *store
		ufs           ufs.UnderFileStorage
		buffers       ReadBufferMap
		bufferPool    *BufferPool
		lock          sync.RWMutex
		seqReadAmount uint64
	}
	type args struct {
		off int64
		buf []byte
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantBytesRead int
		wantErr       bool
	}{
		{
			name: "test read err",
			fields: fields{
				store: &store{
					conf: Config{BlockSize: 1},
				},
				id:      "xx",
				length:  10,
				buffers: ReadBufferMap{1: &ReadBuffer{}},
			},
			wantBytesRead: 0,
			wantErr:       true,
			args: args{
				off: 1,
				buf: make([]byte, 12),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &rCache{
				id:            tt.fields.id,
				flags:         tt.fields.flags,
				length:        tt.fields.length,
				store:         tt.fields.store,
				ufs:           tt.fields.ufs,
				buffers:       tt.fields.buffers,
				bufferPool:    tt.fields.bufferPool,
				lock:          tt.fields.lock,
				seqReadAmount: tt.fields.seqReadAmount,
			}
			if tt.name == "test read err" {
				var p1 = ApplyMethod(reflect.TypeOf(&ReadBuffer{}), "ReadAt",
					func(_ *ReadBuffer, offset uint64, p []byte) (n int, err error) {
						return 0, fmt.Errorf("read fail")
					})
				defer p1.Reset()
			}
			gotBytesRead, err := r.readFromReadAhead(tt.args.off, tt.args.buf)
			if (err != nil) != tt.wantErr {
				t.Errorf("readFromReadAhead() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotBytesRead != tt.wantBytesRead {
				t.Errorf("readFromReadAhead() gotBytesRead = %v, want %v", gotBytesRead, tt.wantBytesRead)
			}
		})
	}
}
