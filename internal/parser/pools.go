// Package parser provides memory pools for efficient object reuse.
package parser

import (
	"bytes"
	"sync"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

var (
	tickPool = sync.Pool{
		New: func() interface{} {
			return &model.RawTick{}
		},
	}

	bufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	byteSlicePool = sync.Pool{
		New: func() interface{} {
			slice := make([]byte, 0, 4096)
			return &slice
		},
	}
)

// AcquireTick gets a RawTick from the pool.
func AcquireTick() *model.RawTick {
	return tickPool.Get().(*model.RawTick)
}

// ReleaseTick returns a RawTick to the pool for reuse.
func ReleaseTick(tick *model.RawTick) {
	*tick = model.RawTick{}
	tickPool.Put(tick)
}

// AcquireBuffer gets a bytes.Buffer from the pool.
func AcquireBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// ReleaseBuffer returns a bytes.Buffer to the pool for reuse.
func ReleaseBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

// AcquireByteSlice gets a byte slice from the pool.
func AcquireByteSlice() *[]byte {
	return byteSlicePool.Get().(*[]byte)
}

// ReleaseByteSlice returns a byte slice to the pool for reuse.
func ReleaseByteSlice(slice *[]byte) {
	*slice = (*slice)[:0]
	byteSlicePool.Put(slice)
}
