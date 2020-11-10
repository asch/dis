package extmap

import (
	"dis/extent"
	"sort"
	"sync"
)

type ExtentMap struct {
	m     []*Extent
	mutex sync.Mutex
}

type Extent struct {
	LBA int64
	PBA int64
	Len int64
	Key int64
}

func New() *ExtentMap {
	m := ExtentMap{m: make([]*Extent, 0, 1024*1024)}
	return &m
}

func (this *ExtentMap) Update(e *[]*Extent) {
	this.mutex.Lock()
	for _, ee := range *e {
		this.update(ee)
	}
	this.mutex.Unlock()
}

func (this *ExtentMap) Find(e *extent.Extent) *[]*Extent {
	this.mutex.Lock()
	extents := this.find(&Extent{e.LBA, -1, e.Len, -1})
	this.mutex.Unlock()
	return extents
}

func (this *ExtentMap) removeAt(i int64) {
	copy(this.m[i:], this.m[i+1:])
	this.m[len(this.m)-1] = nil
	this.m = this.m[:len(this.m)-1]
}

func (this *ExtentMap) remove(e *Extent) {
	i := sort.Search(len(this.m), this.fnEQ(e))
	this.removeAt(int64(i))
}

func (this *ExtentMap) insertAt(i int, e *Extent) {
	if len(this.m) == cap(this.m) {
		println("Initial extent map size is too small!")
	}
	this.m = append(this.m, nil)
	copy(this.m[i+1:], this.m[i:])
	this.m[i] = e
}

func (this *ExtentMap) insert(e *Extent) {
	i := sort.Search(len(this.m), this.fnEQ(e))
	this.insertAt(i, e)
}

func (this *ExtentMap) next(e *Extent) *Extent {
	i := sort.Search(len(this.m), this.fnGT(e))
	if i == len(this.m) {
		return nil
	}
	return this.m[i]
}

func (this *ExtentMap) fnEQ(e *Extent) func(int) bool {
	return func(i int) bool {
		switch {
		case this.m[i].LBA >= e.LBA:
			return true
		default:
			return false
		}
	}
}

func (this *ExtentMap) fnGT(e *Extent) func(int) bool {
	return func(i int) bool {
		switch {
		case this.m[i].LBA > e.LBA:
			return true
		default:
			return false
		}
	}
}

func (this *ExtentMap) fnGEQ(e *Extent) func(int) bool {
	return func(i int) bool {
		switch {
		case this.m[i].LBA >= e.LBA:
			return true
		case this.m[i].LBA+this.m[i].Len > e.LBA:
			return true
		default:
			return false
		}
	}
}

func (this *ExtentMap) geq(e *Extent) *Extent {
	i := sort.Search(len(this.m), this.fnGEQ(e))
	if i == len(this.m) {
		return nil
	}
	return this.m[i]
}

func (this *ExtentMap) update(e *Extent) {
	if geq := this.geq(e); geq != nil {
		if geq.LBA < e.LBA && geq.LBA+geq.Len > e.LBA+e.Len {
			n := &Extent{
				LBA: e.LBA + e.Len,
				Len: geq.LBA + geq.Len - (e.LBA + e.Len),
				Key: geq.Key,
			}
			n.PBA = geq.PBA + geq.Len - n.Len

			geq.Len = e.LBA - geq.LBA
			this.insert(n)
			geq = n

		} else if geq.LBA < e.LBA {
			geq.Len = e.LBA - geq.LBA
			geq = this.next(geq)
		}

		for geq != nil && geq.LBA+geq.Len <= e.LBA+e.Len {
			tmp := this.next(geq)
			this.remove(geq)
			geq = tmp
		}

		if geq != nil && e.LBA+e.Len > geq.LBA {
			n := e.LBA + e.Len - geq.LBA
			geq.LBA += n
			geq.PBA += n
			geq.Len -= n
		}
	}

	this.insert(&Extent{e.LBA, e.PBA, e.Len, e.Key})
}

func (this *ExtentMap) find(e *Extent) *[]*Extent {
	l := make([]*Extent, 0, 256)
	for {
		geq := this.geq(e)

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
