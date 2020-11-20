package gc

import (
	"sync"
	"sync/atomic"
)

const ratio = 0.6

var (
	mutex sync.RWMutex
	usage = make(map[int64]*objectUsage)
)

type objectUsage struct {
	total int64
	used  int64
}

func Free(key, size int64) {
	mutex.RLock()
	o := usage[key]
	mutex.RUnlock()

	atomic.AddInt64(&o.used, -size)
}

func Add(key, size int64) {
	mutex.RLock()
	o := usage[key]
	mutex.RUnlock()

	atomic.AddInt64(&o.used, size)
}

func Create(key, total int64) {
	mutex.Lock()
	defer mutex.Unlock()

	usage[key] = &objectUsage{total, 0}
}
