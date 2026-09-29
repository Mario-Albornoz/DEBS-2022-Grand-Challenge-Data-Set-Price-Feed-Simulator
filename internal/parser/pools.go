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

func AcquireTick() *model.RawTick {
	return tickPool.Get().(*model.RawTick)
}

func ReleaseTick(tick *model.RawTick) {
	*tick = model.RawTick{}
	tickPool.Put(tick)
}

func AcquireBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func ReleaseBuffer(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

func AcquireByteSlice() *[]byte {
	return byteSlicePool.Get().(*[]byte)
}

func ReleaseByteSlice(slice *[]byte) {
	*slice = (*slice)[:0]
	byteSlicePool.Put(slice)
}
