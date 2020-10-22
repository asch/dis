package s3map

import (
	"dis/extent"
	"sort"
	"sync"
)

type S3map struct {
	m     []*S3extent
	mutex sync.Mutex
}

type S3extent struct {
	LBA int64
	PBA int64
	Len int64
	Key int64
}

func New() *S3map {
	s3m := S3map{m: make([]*S3extent, 0, 1024*1024)}
	return &s3m
}

func (this *S3map) Update(e *[]*S3extent) {
	this.mutex.Lock()
	for _, ee := range *e {
		this.update(ee)
	}
	this.mutex.Unlock()
}

func (this *S3map) Find(e *extent.Extent) *[]*S3extent {
	this.mutex.Lock()
	s3extents := this.find(&S3extent{e.LBA, -1, e.Len, -1})
	this.mutex.Unlock()
	return s3extents
}

func (this *S3map) removeAt(i int64) {
	copy(this.m[i:], this.m[i+1:])
	this.m[len(this.m)-1] = nil
	this.m = this.m[:len(this.m)-1]
}

func (this *S3map) remove(e *S3extent) {
	i := sort.Search(len(this.m), this.fnEQ(e))
	this.removeAt(int64(i))
}

func (this *S3map) insertAt(i int, e *S3extent) {
	this.m = append(this.m, nil)
	copy(this.m[i+1:], this.m[i:])
	this.m[i] = e
}

func (this *S3map) insert(e *S3extent) {
	i := sort.Search(len(this.m), this.fnEQ(e))
	this.insertAt(i, e)
}

func (this *S3map) next(e *S3extent) *S3extent {
	i := sort.Search(len(this.m), this.fnGT(e))
	if i == len(this.m) {
		return nil
	}
	return this.m[i]
}

func (this *S3map) fnEQ(e *S3extent) func(int) bool {
	return func(i int) bool {
		switch {
		case this.m[i].LBA >= e.LBA:
			return true
		default:
			return false
		}
	}
}

func (this *S3map) fnGT(e *S3extent) func(int) bool {
	return func(i int) bool {
		switch {
		case this.m[i].LBA > e.LBA:
			return true
		default:
			return false
		}
	}
}

func (this *S3map) fnGEQ(e *S3extent) func(int) bool {
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

func (this *S3map) geq(e *S3extent) *S3extent {
	i := sort.Search(len(this.m), this.fnGEQ(e))
	if i == len(this.m) {
		return nil
	}
	return this.m[i]
}

func (this *S3map) update(e *S3extent) {
	if geq := this.geq(e); geq != nil {
		if geq.LBA < e.LBA && geq.LBA+geq.Len > e.LBA+e.Len {
			n := &S3extent{
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

	this.insert(&S3extent{e.LBA, e.PBA, e.Len, e.Key})
}

func (this *S3map) find(e *S3extent) *[]*S3extent {
	l := make([]*S3extent, 0, 256)
	for {
		geq := this.geq(e)

		if geq == nil || geq.LBA >= e.LBA+e.Len {
			l = append(l, &S3extent{e.LBA, -1, e.Len, -1})
			return &l
		}

		if e.LBA < geq.LBA {
			l = append(l, &S3extent{e.LBA, -1, geq.LBA - e.LBA, -1})

			e.Len -= geq.LBA - e.LBA

			e.LBA = geq.LBA
			e.PBA = geq.PBA
			e.Key = geq.Key
		} else {
			if geq.LBA+geq.Len-e.LBA < e.Len {
				l = append(l, &S3extent{
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
				l = append(l, &S3extent{e.LBA, geq.PBA + e.LBA - geq.LBA, e.Len, geq.Key})
				return &l
			}
		}
	}
}
