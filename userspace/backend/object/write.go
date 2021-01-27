package object

import (
	"dis/backend/object/extmap"
	"dis/backend/object/gc"
	"dis/cache"
	"dis/extent"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"
)

const (
	uploadWorkers    = 30
	cacheReadWorkers = 30
	maxWritePeriod   = 5 * time.Second
)

var (
	mutex        sync.RWMutex
	uploading    = make(map[int64]bool)
	writelistLen = objectSize / 512
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
}

func nextObject(inGC bool) *Object {
	buf := make([]byte, 0, objectSize)
	var writelist []*extmap.Extent
	if inGC {
		writelist = make([]*extmap.Extent, 0, 0)
	} else {
		writelist = make([]*extmap.Extent, 0, writelistLen)
	}
	var reads sync.WaitGroup
	var headerBlocks int64 = (writelistLen * 16) / 512

	o := Object{
		buf:       &buf,
		writelist: &writelist,
		reads:     &reads,
		blocks:    headerBlocks,
	}

	return &o
}

func (this *Object) size() int64 {
	return this.blocks * 512
}

func (this *Object) assignKey() {
	this.key = atomic.LoadInt64(&seqNumber)
	atomic.AddInt64(&seqNumber, 1)
	var headerBlocks int64 = (writelistLen * 16) / 512
	gc.Create(this.key, this.blocks-headerBlocks)

	for _, e := range *this.writelist {
		e.Key = this.key
	}
}

func (o *Object) add(lba, length int64, inGC bool) []byte {
	*o.buf = (*o.buf)[:(o.blocks+length)*512]
	slice := (*o.buf)[o.blocks*512:]

	if !inGC {
		*o.writelist = append(*o.writelist, &extmap.Extent{
			LBA: lba,
			PBA: o.blocks,
			Len: length,
			Key: o.key})
	}

	o.fillHeader(lba, length)
	o.extents++
	o.blocks += length

	return slice
}

func (o *Object) fillHeader(lba, length int64) {
	const int64Size = 8
	off := o.extents * int64Size * 2

	headerSlice := (*o.buf)[off:]
	binary.PutVarint(headerSlice, lba)

	headerSlice = headerSlice[int64Size:]
	binary.PutVarint(headerSlice, length)
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
				uploadF(u.key, u.buf)
				mutex.Lock()
				delete(uploading, u.key)
				mutex.Unlock()
			}
		}()
	}

	ticker := time.NewTicker(maxWritePeriod)

	o := nextObject(false)
	upload := func() {
		if o.extents == 0 {
			return
		}
		//gc.Running.Wait()
		gc.Running.Lock()
		o.assignKey()
		mutex.Lock()
		uploading[o.key] = true
		mutex.Unlock()
		em.Update(o.writelist)

		gc.Running.Unlock()

		uploadChan <- o
		o = nextObject(false)
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

				slice := o.add(e.LBA, e.Len, false)
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

//func writer2() {
//	mapUpdateChan := make(chan *[]*extmap.Extent, mapUpdateBuf)
//	//go mapUpdateWorker(mapUpdateChan)
//
//	uploadChan := make(chan *Object, uploadBuf)
//	for i := 0; i < uploadWorkers; i++ {
//		go uploadWorker(uploadChan)
//	}
//
//	o := nextObject(0)
//	for extents := range workloads {
//		pr := cache.NewPrereader(extents)
//		//cache.WriteUntrackMulti(extents)
//		sectors := computeSectors(extents)
//		for i := range *extents {
//			e := &(*extents)[i]
//
//			from := o.blocks * 512
//			to := (o.blocks + e.Len) * 512
//
//			*o.writelist = append(*o.writelist, &extmap.Extent{
//				LBA: e.LBA,
//				PBA: o.blocks,
//				Len: e.Len,
//				Key: o.key,
//			})
//
//			o.blocks += e.Len
//			pr.Copy((*o.buf)[from:to], e.PBA*512)
//		}
//
//		if (o.blocks+sectors)*512 > objectSize {
//			mapUpdateChan <- o.writelist
//			//uploadChan <- uploadJob{key, (*buf)[:blocks*512]}
//			o = nextObject(o.key + 1)
//		}
//	}
//}

func (this *ObjectBackend) Write(extents *[]extent.Extent) {
	workloads <- extents
}
