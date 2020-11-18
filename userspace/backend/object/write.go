package object

import (
	"dis/backend/object/extmap"
	"dis/backend/object/s3"
	"dis/cache"
	"dis/extent"
	"encoding/binary"
	"sync"
	"time"
)

const workloadsBuf = 1024 * 1024 * 2
const objectSize = 1024 * 1024 * 32

const (
	uploadWorkers      = 30
	uploadBuf          = 10
	cacheReadWorkers   = 30
	cacheReadBuf       = 10
	mapUpdateBuf       = uploadWorkers + uploadBuf
	cacheWriteTrackBuf = 1024
	writelistLen       = objectSize / 512
	maxWritePeriod     = 1 * time.Second
)

type cacheReadJob struct {
	e                   *extent.Extent
	buf                 *[]byte
	reads               *sync.WaitGroup
	cacheWriteTrackChan chan *extent.Extent
}

func uploadWorker(oo <-chan *Object) {
	for o := range oo {
		o.reads.Wait()
		*o.buf = (*o.buf)[:cap(*o.buf)]
		s3.Upload(o.key, o.buf)
	}
}

func cacheReadWorker(jobs <-chan cacheReadJob) {
	for job := range jobs {
		cache.Read(job.buf, job.e.PBA*512)
		job.cacheWriteTrackChan <- job.e
		job.reads.Done()
	}
}

func mapUpdateWorker(writelists <-chan *[]*extmap.Extent) {
	for writelist := range writelists {
		em.Update(writelist)
	}
}

func cacheWriteTrack(cacheWriteTrackChan <-chan *extent.Extent) {
	extents := make([]*extent.Extent, 0, 256)
	for e := range cacheWriteTrackChan {
		extents = append(extents, e)
		if len(extents) == cap(extents) {
			cache.WriteUntrackMulti(&extents)
			extents = extents[:0]
		}
	}
}

type Object struct {
	buf       *[]byte
	writelist *[]*extmap.Extent
	blocks    int64
	reads     *sync.WaitGroup
	key       int64
	extents   int64
}

func nextObject(key int64) *Object {
	buf := make([]byte, 0, objectSize)
	writelist := make([]*extmap.Extent, 0, writelistLen)
	var reads sync.WaitGroup
	var headerBlocks int64 = (writelistLen * 16) / 512

	return &Object{
		buf:       &buf,
		writelist: &writelist,
		reads:     &reads,
		key:       key,
		blocks:    headerBlocks,
	}
}

func (this *Object) size() int64 {
	return this.blocks * 512
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
	cacheReadChan := make(chan cacheReadJob, cacheReadBuf)
	for i := 0; i < cacheReadWorkers; i++ {
		go cacheReadWorker(cacheReadChan)
	}

	uploadChan := make(chan *Object, uploadBuf)
	for i := 0; i < uploadWorkers; i++ {
		go uploadWorker(uploadChan)
	}

	mapUpdateChan := make(chan *[]*extmap.Extent, mapUpdateBuf)
	go mapUpdateWorker(mapUpdateChan)

	cacheWriteTrackChan := make(chan *extent.Extent, cacheWriteTrackBuf)
	go cacheWriteTrack(cacheWriteTrackChan)


	ticker := time.NewTicker(maxWritePeriod)
	o := nextObject(startKey)

	upload := func() {
		if o.extents == 0 {
			return
		}
		gc.Create(o.key, int64(len(*o.buf)))
		mapUpdateChan <- o.writelist
		uploadChan <- o
		o = nextObject(o.key + 1)
		ticker.Reset(maxWritePeriod)
	}

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
				cacheReadChan <- cacheReadJob{e, &slice, o.reads, cacheWriteTrackChan}
			}
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
