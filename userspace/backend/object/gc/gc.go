package gc

import (
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	"sync"
	"sync/atomic"
)

const ratio = 0.2
const threshold = 1024 * 1024 * 64

var (
	mutex   sync.RWMutex
	usage   = make(map[int64]*objectUsage)
	Running = new(sync.Mutex)
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

func Destroy(key int64) {
	mutex.Lock()
	defer mutex.Unlock()

	delete(usage, key)
}

func GetPurgeSetGreedy() *map[int64]bool {
	t := redblacktree.NewWith(func(a, b interface{}) int {
		return -utils.Int64Comparator(a, b)
	})
	purgeSet := map[int64]bool{}

	mutex.RLock()
	for k, v := range usage {
		free := v.total - v.used
		t.Put(free, k)
	}
	mutex.RUnlock()

	var freeTotal int64
	it := t.Iterator()
	for it.Next() {
		k := it.Value().(int64)
		purgeSet[k] = true
		freeTotal += it.Key().(int64)
		if freeTotal >= threshold {
			break
		}
	}

	return &purgeSet
}

func GetPurgeSetUniform() *map[int64]bool {
	purgeSet := map[int64]bool{}

	mutex.RLock()
	for k, v := range usage {
		if r := float64(v.used) / float64(v.total); r < ratio {
			purgeSet[k] = true
		}
	}
	mutex.RUnlock()

	return &purgeSet
}
