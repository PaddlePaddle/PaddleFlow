package cache

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"github.com/PaddlePaddle/PaddleFlow/pkg/fs/utils"
)

func Test_freeMemory(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "mem percent > 0.3",
		},
	}
	patch := gomonkey.ApplyFunc(utils.GetProcessMemPercent, func() float32 {
		return 50
	})
	defer patch.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			freeMemory()
		})
	}
}

type delayedReader struct {
	data  string
	delay time.Duration
}

func (dr *delayedReader) Read(p []byte) (n int, err error) {
	time.Sleep(dr.delay)
	return copy(p, dr.data), nil
}

// test for func (tr *TimeoutReader) Read(p []byte) (n int, err error)
func TestTimeoutReader_Read(t *testing.T) {
	source := &delayedReader{data: "Hello, world!", delay: 2 * time.Second}

	tests := []struct {
		name    string
		tr      *TimeoutReader
		p       []byte
		wantN   int
		wantErr bool
	}{
		{
			name: "read timeout",
			tr: &TimeoutReader{
				timeout: 1 * time.Second,
				source:  source,
			},
			p:       []byte{1},
			wantN:   0,
			wantErr: true,
		},
		{
			name: "read success",
			tr: &TimeoutReader{
				timeout: 10 * time.Second,
				source:  source,
			},
			p:       []byte{1},
			wantN:   1,
			wantErr: false,
		},
	}
	// gomonkey for reader.Read
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, err := tt.tr.Read(tt.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("TimeoutReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if n != tt.wantN {
				t.Errorf("TimeoutReader.Read() got = %v, want %v", n, tt.wantN)
			}
		})
	}
}
