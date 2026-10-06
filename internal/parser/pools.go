package parser

import (
	"sync"

	"github.com/Mario-Albornoz/DEBS-2022-Dataset-price-feed-simulator/internal/model"
)

var (
	tickPool = sync.Pool{
		New: func() interface{} {
			return &model.RawTick{}
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
