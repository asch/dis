// SPDX-License-Identifier: GPL-2.0-only
// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package gc

import (
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	"sync"
	"sync/atomic"
)

const ratio = 0.2
const gcTarget = 0.3

var (
	mutex   sync.RWMutex
	usage   = make(map[int64]*objectUsage)
	Running = new(sync.Mutex)
	total   int64
	valid   int64
	statcnt int64
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
	atomic.AddInt64(&valid, -size)
}

func Add(key, size int64) {
	mutex.RLock()
	o := usage[key]
	mutex.RUnlock()

	atomic.AddInt64(&o.used, size)
	atomic.AddInt64(&total, size)
	atomic.AddInt64(&valid, size)
}

func Create(key, total int64) {
	mutex.Lock()
	defer mutex.Unlock()

	usage[key] = &objectUsage{total, 0}
}

func Destroy(key int64) {
	mutex.Lock()
	defer mutex.Unlock()

	o := usage[key]

	atomic.AddInt64(&valid, -o.used)
	atomic.AddInt64(&total, -o.total)
	delete(usage, key)
}

func PrintStats(delay int64, gcMode string) {
	total := atomic.LoadInt64(&total)
	valid := atomic.LoadInt64(&valid)
	garbage := total - valid

	fmt.Printf("STATS: %v,%v,%v,%v,%v,%v\n", statcnt, total, valid, garbage, float64(garbage)/float64(total), gcMode)

	statcnt += delay
}

func Needed() bool {
	total := atomic.LoadInt64(&total)
	valid := atomic.LoadInt64(&valid)
	garbage := total - valid

	if float64(garbage)/float64(total) >= gcTarget {
		return true
	}
	return false
}

func GetPurgeSetGreedy() *map[int64]bool {
	t := redblacktree.NewWith(func(a, b interface{}) int {
		return -utils.Int64Comparator(a, b)
	})

	purgeSet := map[int64]bool{}

	mutex.RLock()
	for k, v := range usage {
		t.Put(v.total-v.used, k)
	}
	mutex.RUnlock()

	total := atomic.LoadInt64(&total)
	valid := atomic.LoadInt64(&valid)
	invalid := total - valid
	toCollect := float64(invalid) - gcTarget*float64(total)

	it := t.Iterator()
	for it.Next() {
		k := it.Value().(int64)
		purgeSet[k] = true
		invalid := it.Key().(int64)
		toCollect -= float64(invalid)
		if toCollect < 0 {
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
