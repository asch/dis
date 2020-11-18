package gc

import (
	"sync"
)

const ratio = 0.6

var (
	mutex sync.Mutex
	usage map[int64]*objectUsage
)

type objectUsage struct {
	total int64
	used  int64
}

func Free(key, size int64) {
	mutex.Lock()
	defer mutex.Unlock()

	usage[key].used -= size
}

func Add(key, size int64) {
	mutex.Lock()
	defer mutex.Unlock()

	usage[key].used += size
}

func Create(key, total int64) {
	mutex.Lock()
	defer mutex.Unlock()

	usage[key] = &objectUsage{total, 0}
}
