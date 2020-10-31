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
	uploadWorkers    = 10
	uploadBuf        = 10
	cacheReadWorkers = 10
	cacheReadBuf     = 10
)

type uploadJob struct {
	key   int64
	buf   []byte
	reads *sync.WaitGroup
}

type cacheReadJob struct {
	pba   int64
	buf   *[]byte
	reads *sync.WaitGroup
}

func uploadWorker(jobs <-chan uploadJob) {
	for job := range jobs {
		job.reads.Wait()
		s3op.Upload(job.key, &job.buf)
	}
}

func cacheReadWorker(jobs <-chan cacheReadJob) {
	for job := range jobs {
		cache.Read(job.buf, job.pba*512)
		job.reads.Done()
	}
}

func nextObject(key int64) (*[]byte, *[]*s3map.S3extent, int64, int64, *sync.WaitGroup) {
	buf := make([]byte, 0, s3limit)
	var writelist []*s3map.S3extent
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

	buf, writelist, blocks, key, reads := nextObject(0)
	for extents := range workloads {
		for i := range *extents {
			e := &(*extents)[i]
			if (blocks+e.Len)*512 > s3limit && len(*writelist) > 0 {
				s3m.Update(writelist)
				uploadChan <- uploadJob{key, *buf, reads}
				buf, writelist, blocks, key, reads = nextObject(key + 1)
			}
			*buf = (*buf)[:(blocks+e.Len)*512]
			slice := (*buf)[blocks*512:]

			*writelist = append(*writelist, &s3map.S3extent{
				LBA: e.LBA,
				PBA: blocks,
				Len: e.Len,
				Key: key})

			blocks += e.Len
			reads.Add(1)
			cacheReadChan <- cacheReadJob{e.PBA, &slice, reads}
		}
	}
}

func (this *S3Backend) Write(extents *[]extent.Extent) {
	workloads <- extents
}
