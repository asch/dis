package s3

import (
	"dis/backend/s3/s3map"
	"dis/cache"
	"dis/extent"
	"sync"
)

const workloadsBuf = 1024 * 1024 * 2
const s3limit = 1024 * 1024 * 32

const (
	uploadWorkers      = 30
	uploadBuf          = 10
	cacheReadWorkers   = 30
	cacheReadBuf       = 10
	mapUpdateBuf       = uploadWorkers + uploadBuf
	cacheWriteTrackBuf = 1024
	writelistLen       = 4096
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
		s3op.Upload(job.key, &job.buf)
	}
}

func cacheReadWorker(jobs <-chan cacheReadJob) {
	for job := range jobs {
		cache.Read(job.buf, job.e.PBA*512)
		job.cacheWriteTrackChan <- job.e
		job.reads.Done()
	}
}

func mapUpdateWorker(writelists <-chan *[]*s3map.S3extent) {
	for writelist := range writelists {
		s3m.Update(writelist)
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

func nextObject(key int64) (*[]byte, *[]*s3map.S3extent, int64, int64, *sync.WaitGroup) {
	buf := make([]byte, 0, s3limit)
	writelist := make([]*s3map.S3extent, 0, writelistLen)
	var blocks int64
	var reads sync.WaitGroup

	return &buf, &writelist, blocks, key, &reads
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

	mapUpdateChan := make(chan *[]*s3map.S3extent, mapUpdateBuf)
	go mapUpdateWorker(mapUpdateChan)

	cacheWriteTrackChan := make(chan *extent.Extent, cacheWriteTrackBuf)
	go cacheWriteTrack(cacheWriteTrackChan)

	buf, writelist, blocks, key, reads := nextObject(0)
	for extents := range workloads {
		for i := range *extents {
			e := &(*extents)[i]
			if (blocks+e.Len)*512 > s3limit && len(*writelist) > 0 {
				mapUpdateChan <- writelist
				uploadChan <- uploadJob{key, *buf, reads}
				buf, writelist, blocks, key, reads = nextObject(key + 1)
			}
			*buf = (*buf)[:(blocks+e.Len)*512]
			slice := (*buf)[blocks*512:]

			if len(*writelist) == cap(*writelist) {
				println("Consider raising writelistLen to avoid memory copy during append!")
			}

			*writelist = append(*writelist, &s3map.S3extent{
				LBA: e.LBA,
				PBA: blocks,
				Len: e.Len,
				Key: key})

			blocks += e.Len
			reads.Add(1)
			cacheReadChan <- cacheReadJob{e, &slice, reads, cacheWriteTrackChan}
		}
	}
}

func (this *S3Backend) Write(extents *[]extent.Extent) {
	workloads <- extents
}
