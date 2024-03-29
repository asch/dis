// SPDX-License-Identifier: GPL-2.0-only
// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package extmap

import (
	"dis/backend/object/gc"
	"dis/extent"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/emirpasic/gods/utils"
	"sync"
)

type ExtentMap struct {
	rbt   *redblacktree.Tree
	mutex sync.RWMutex
}

type Extent struct {
	LBA int64
	PBA int64
	Len int64
	Key int64
}

func New() *ExtentMap {
	m := ExtentMap{rbt: redblacktree.NewWith(utils.Int64Comparator)}
	return &m
}

func (this *ExtentMap) Update(e *[]*Extent) {
	this.mutex.Lock()
	for _, ee := range *e {
		this.update(ee)
	}
	this.mutex.Unlock()
}

func (this *ExtentMap) UpdateSingle(e *Extent) {
	this.mutex.Lock()
	this.update(e)
	this.mutex.Unlock()
}

func (this *ExtentMap) Find(e *extent.Extent) *[]*Extent {
	this.mutex.RLock()
	extents := this.find(&Extent{e.LBA, -1, e.Len, -1})

	this.mutex.RUnlock()
	return extents
}

func (this *ExtentMap) RLock() {
	this.mutex.RLock()
}

func (this *ExtentMap) RUnlock() {
	this.mutex.RUnlock()
}

func (this *ExtentMap) Lock() {
	this.mutex.Lock()
}

func (this *ExtentMap) Unlock() {
	this.mutex.Unlock()
}

func (this *ExtentMap) GenerateWritelist(purgeList *map[int64]bool) *[]*Extent {
	writelist := new([]*Extent)

	it := this.rbt.Iterator()
	for it.Next() {
		e := it.Value().(*Extent)
		if (*purgeList)[e.Key] {
			*writelist = append(*writelist, e)
		}
	}

	return writelist
}

func (this *ExtentMap) insert(e *Extent)  {
	this.rbt.Put(e.LBA, e)
}

func (this *ExtentMap) next(e *Extent) *Extent {
	next, _ := this.rbt.Ceiling(e.LBA + 1)
	if next == nil {
		return nil
	}
	return next.Value.(*Extent)
}

func (this *ExtentMap) remove(e *Extent) {
	this.rbt.Remove(e.LBA)
}

func (this *ExtentMap) geq(e *Extent) *redblacktree.Node {

	if e == nil {
		return nil
	}

	if f, _ := this.rbt.Floor(e.LBA); f != nil {
		if fVal := f.Value.(*Extent); fVal.LBA+fVal.Len > e.LBA {
			return f
		}
	}

	if c, _ := this.rbt.Ceiling(e.LBA); c != nil {
		return c
	}

	return nil
}

func (this *ExtentMap) update(e *Extent) {
	node := this.geq(e)
	if node != nil {
		geq := node.Value.(*Extent)
		if geq.LBA < e.LBA && geq.LBA+geq.Len > e.LBA+e.Len {
			n := &Extent{
				LBA: e.LBA + e.Len,
				Len: geq.LBA + geq.Len - (e.LBA + e.Len),
				Key: geq.Key,
			}
			n.PBA = geq.PBA + geq.Len - n.Len

			gc.Free(geq.Key, n.Len)
			gc.Add(n.Key, n.Len)

			geq.Len = e.LBA - geq.LBA
			this.insert(n)
			geq = n
			node = this.geq(geq)

		} else if geq.LBA < e.LBA {
			gc.Free(geq.Key, geq.Len-e.LBA+geq.LBA)
			geq.Len = e.LBA - geq.LBA
			geq = this.next(geq)
			node = this.geq(geq)
		}

		for geq != nil && geq.LBA+geq.Len <= e.LBA+e.Len {
			tmp := this.next(geq)
			this.remove(geq)
			gc.Free(geq.Key, geq.Len)
			geq = tmp
			node = this.geq(geq)
		}

		if geq != nil && e.LBA+e.Len > geq.LBA {
			n := e.LBA + e.Len - geq.LBA
			geq.LBA += n
			node.Key = geq.LBA
			geq.PBA += n
			geq.Len -= n
			gc.Free(geq.Key, n)
		}
	}

	this.insert(&Extent{e.LBA, e.PBA, e.Len, e.Key})
	gc.Add(e.Key, e.Len)
}

func (this *ExtentMap) find(e *Extent) *[]*Extent {
	l := make([]*Extent, 0, 256)
	for {
		node := this.geq(e)
		var geq *Extent
		if node != nil {
			geq = node.Value.(*Extent)
		}

		if geq == nil || geq.LBA >= e.LBA+e.Len {
			if len(l) == cap(l) {
				println("extent list size to small #1")
			}
			l = append(l, &Extent{e.LBA, -1, e.Len, -1})

			return &l
		}

		if e.LBA < geq.LBA {
			if len(l) == cap(l) {
				println("extent list size to small #2")
			}
			l = append(l, &Extent{e.LBA, -1, geq.LBA - e.LBA, -1})

			e.Len -= geq.LBA - e.LBA

			e.LBA = geq.LBA
			e.PBA = geq.PBA
			e.Key = geq.Key
		} else {
			if geq.LBA+geq.Len-e.LBA < e.Len {
				if len(l) == cap(l) {
					println("extent list size to small #3")
				}
				l = append(l, &Extent{
					LBA: e.LBA,
					PBA: geq.PBA + e.LBA - geq.LBA,
					Len: geq.LBA + geq.Len - e.LBA,
					Key: geq.Key,
				})

				e.Len -= geq.LBA + geq.Len - e.LBA

				e.LBA = geq.LBA + geq.Len
				e.PBA = -1
				e.Key = -1
			} else {
				if len(l) == cap(l) {
					println("extent list size to small #4")
				}
				l = append(l, &Extent{e.LBA, geq.PBA + e.LBA - geq.LBA, e.Len, geq.Key})
				return &l
			}
		}
	}
}
