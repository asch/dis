package object

import (
	"dis/backend/object/extmap"
	"dis/backend/object/gc"
	"dis/backend/object/s3"
	"dis/cache"
	"dis/extent"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

const objectSize = 1024 * 1024 * 32

const (
	uploadWorkers    = 10
	cacheReadWorkers = 50
	writelistLen     = objectSize / 512
	maxWritePeriod   = 1 * time.Second
)

var (
	mutex     sync.RWMutex
	uploading = make(map[int64]bool)
)

type cacheReadJob struct {
	e        *extent.Extent
	buf      *[]byte
	reads    *sync.WaitGroup
	allReads *sync.WaitGroup
}

type Object struct {
	buf       *[]byte
	writelist *[]*extmap.Extent
	blocks    int64
	reads     *sync.WaitGroup
	key       int64
	extents   int64
	upload    sync.WaitGroup
}

func nextObject() *Object {
	buf := make([]byte, 0, objectSize)
	writelist := make([]*extmap.Extent, 0, writelistLen)
	var reads sync.WaitGroup
	var headerBlocks int64 = (writelistLen * 16) / 512

	o := Object{
		buf:       &buf,
		writelist: &writelist,
		reads:     &reads,
		blocks:    headerBlocks,
	}
	o.upload.Add(1)

	return &o
}

func (this *Object) size() int64 {
	return this.blocks * 512
}

func (this *Object) assignKey() {
	this.key = atomic.LoadInt64(&seqNumber)
	atomic.AddInt64(&seqNumber, 1)

	for _, e := range *this.writelist {
		e.Key = this.key
	}
}

func (o *Object) add(e *extent.Extent) []byte {
	*o.buf = (*o.buf)[:(o.blocks+e.Len)*512]
	slice := (*o.buf)[o.blocks*512:]

	*o.writelist = append(*o.writelist, &extmap.Extent{
		LBA: e.LBA,
		PBA: o.blocks,
		Len: e.Len,
		Key: o.key})

	o.fillHeader(e)

	o.extents++
	o.blocks += e.Len

	return slice
}

func (o *Object) fillHeader(e *extent.Extent) {
	const int64Size = 8
	off := o.extents * int64Size * 2

	headerSlice := (*o.buf)[off:]
	binary.PutVarint(headerSlice, e.LBA)

	headerSlice = headerSlice[int64Size:]
	binary.PutVarint(headerSlice, e.Len)
}

func writer() {
	cacheReadChan := make(chan cacheReadJob)
	for i := 0; i < cacheReadWorkers; i++ {
		go func() {
			for c := range cacheReadChan {
				cache.Read(c.buf, c.e.PBA*512)
				c.reads.Done()
				c.allReads.Done()
			}
		}()
	}

	uploadChan := make(chan *Object)
	for i := 0; i < uploadWorkers; i++ {
		go func() {
			for u := range uploadChan {
				*u.buf = (*u.buf)[:cap(*u.buf)]
				u.reads.Wait()
				s3.Upload(u.key, u.buf)
				u.upload.Done()
				mutex.Lock()
				delete(uploading, u.key)
				mutex.Unlock()
			}
		}()
	}

	ticker := time.NewTicker(maxWritePeriod)

	o := nextObject()
	upload := func() {
		if o.extents == 0 {
			return
		}
		o.assignKey()
		mutex.Lock()
		uploading[o.key] = true
		mutex.Unlock()
		em.Update(o.writelist)
		uploadChan <- o
		o = nextObject()
		for len(ticker.C) > 0 {
			<-ticker.C
		}
		ticker.Reset(maxWritePeriod)
	}

	var allReads sync.WaitGroup
	for {
		select {
		case extents := <-workloads:
			for i := range *extents {
				e := &(*extents)[i]
				if o.size()+e.Len*512 > objectSize || len(ticker.C) > 0 {
					upload()
				}

				slice := o.add(e)
				o.reads.Add(1)
				allReads.Add(1)
				cacheReadChan <- cacheReadJob{e, &slice, o.reads, &allReads}
			}
			allReads.Wait()
		case <-ticker.C:
			upload()
		}
	}
}

func computeSectors(extents *[]extent.Extent) int64 {
	var sectors int64
	for i := range *extents {
		e := &(*extents)[i]
		sectors += e.Len
	}

	return sectors
}

func writer2() {
	mapUpdateChan := make(chan *[]*extmap.Extent, mapUpdateBuf)
	go mapUpdateWorker(mapUpdateChan)

	uploadChan := make(chan *Object, uploadBuf)
	for i := 0; i < uploadWorkers; i++ {
		go uploadWorker(uploadChan)
	}

	o := nextObject(0)
	for extents := range workloads {
		pr := cache.NewPrereader(extents)
		//cache.WriteUntrackMulti(extents)
		sectors := computeSectors(extents)
		for i := range *extents {
			e := &(*extents)[i]

			from := o.blocks * 512
			to := (o.blocks + e.Len) * 512

			*o.writelist = append(*o.writelist, &extmap.Extent{
				LBA: e.LBA,
				PBA: o.blocks,
				Len: e.Len,
				Key: o.key,
			})

			o.blocks += e.Len
			pr.Copy((*o.buf)[from:to], e.PBA*512)
		}

		if (o.blocks+sectors)*512 > objectSize {
			mapUpdateChan <- o.writelist
			//uploadChan <- uploadJob{key, (*buf)[:blocks*512]}
			o = nextObject(o.key + 1)
		}
	}
}

func (this *ObjectBackend) Write(extents *[]extent.Extent) {
	workloads <- extents
}
