package object

import (
	"dis/backend/object/extmap"
	"dis/backend/object/s3"
	"dis/cache"
	"dis/extent"
	"sync"
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
)

type uploadJob struct {
	key   int64
	buf   []byte
	reads *sync.WaitGroup
}

type cacheReadJob struct {
	e                   *extent.Extent
	buf                 *[]byte
	reads               *sync.WaitGroup
	cacheWriteTrackChan chan *extent.Extent
}

func uploadWorker(jobs <-chan uploadJob) {
	for job := range jobs {
		job.reads.Wait()
		s3.Upload(job.key, &job.buf)
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
}

func nextObject(key int64) *Object {
	buf := make([]byte, 0, objectSize)
	writelist := make([]*extmap.Extent, 0, writelistLen)
	var reads sync.WaitGroup

	return &Object{
		buf:       &buf,
		writelist: &writelist,
		reads:     &reads,
		key:       key,
	}
}

func writer() {
	cacheReadChan := make(chan cacheReadJob, cacheReadBuf)
	for i := 0; i < cacheReadWorkers; i++ {
		go cacheReadWorker(cacheReadChan)
	}

	uploadChan := make(chan uploadJob, uploadBuf)
	for i := 0; i < uploadWorkers; i++ {
		go uploadWorker(uploadChan)
	}

	mapUpdateChan := make(chan *[]*extmap.Extent, mapUpdateBuf)
	go mapUpdateWorker(mapUpdateChan)

	cacheWriteTrackChan := make(chan *extent.Extent, cacheWriteTrackBuf)
	go cacheWriteTrack(cacheWriteTrackChan)

	o := nextObject(0)
	for extents := range workloads {
		for i := range *extents {
			e := &(*extents)[i]
			if (o.blocks+e.Len)*512 > objectSize && len(*o.writelist) > 0 {
				mapUpdateChan <- o.writelist
				uploadChan <- uploadJob{o.key, *o.buf, o.reads}
				o = nextObject(o.key + 1)
			}
			*o.buf = (*o.buf)[:(o.blocks+e.Len)*512]
			slice := (*o.buf)[o.blocks*512:]

			*o.writelist = append(*o.writelist, &extmap.Extent{
				LBA: e.LBA,
				PBA: o.blocks,
				Len: e.Len,
				Key: o.key})

			o.blocks += e.Len
			o.reads.Add(1)
			cacheReadChan <- cacheReadJob{e, &slice, o.reads, cacheWriteTrackChan}
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

	uploadChan := make(chan uploadJob, uploadBuf)
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

			*o.writelist = append(*o.writelist, &extmap.Extent{e.LBA, o.blocks, e.Len, o.key})
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
