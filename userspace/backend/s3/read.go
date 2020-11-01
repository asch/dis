package s3

import (
	"dis/backend/s3/s3map"
	"dis/cache"
	"dis/extent"
	"dis/l2cache"
	"fmt"
	"sync"
	"time"
)

const (
	downloadWorkers   = 10
	downloadBuf       = 10
	cacheWriteWorkers = 10
	cacheWriteBuf     = 10
)

var (
	cacheWriteChan = make(chan cacheWriteJob, cacheWriteBuf)
	downloadChan   = make(chan downloadJob, downloadBuf)
)

func partDownload(s3e *s3map.S3extent, slice *[]byte) {
	from := fmt.Sprintf("%d", s3e.PBA*512)
	to := fmt.Sprintf("%d", (s3e.PBA+s3e.Len)*512-1)
	rng := "bytes=" + from + "-" + to
	s3op.Download(s3e.Key, slice, &rng)
}

type cacheWriteJob struct {
	e     *extent.Extent
	reads *sync.WaitGroup
}

type downloadJob struct {
	s3e     *s3map.S3extent
	buf     *[]byte
	s3reads *sync.WaitGroup
}

func fillPartFromChunk(slice []byte, chunkI int64, chunkTo, chunkFrom int64, wg *sync.WaitGroup, key int64) {
	id := func(key, chunk int64) int64 {
		return key*1000 + chunk
	}

	oneChunk := func(i int64) *string {
		from := fmt.Sprintf("%d", i*l2cache.ChunkSize)
		to := fmt.Sprintf("%d", i*l2cache.ChunkSize + l2cache.ChunkSize - 1)
		rng := "bytes=" + from + "-" + to
		return &rng
	}

	cacheKey := id(key, chunkI)
again:
	chunk, ok := l2cache.GetChunk(cacheKey)
	if !ok {
		l2cache.PutChunk(cacheKey, nil)
		buf := make([]byte, l2cache.ChunkSize)
		s3op.Download(key, &buf, oneChunk(chunkI))
		l2cache.PutChunk(cacheKey, &buf)
		chunk = &buf
	} else if chunk == nil {
		time.Sleep(10 * time.Microsecond)
		goto again
	}
	copy(slice, (*chunk)[chunkFrom:chunkTo])
	wg.Done()
}

func downloadWorker(jobs <-chan downloadJob) {
	for job := range jobs {
		first := job.s3e.PBA*512 / l2cache.ChunkSize
		last := (job.s3e.PBA + job.s3e.Len - 1) * 512 / l2cache.ChunkSize
		part := *job.buf
		var waitChunks sync.WaitGroup
		waitChunks.Add(int(last - first + 1))
		for i := first; i <= last; i++ {
			chunkFrom, chunkTo := int64(0), int64(l2cache.ChunkSize)
			if i == first {
				chunkFrom = job.s3e.PBA*512 % l2cache.ChunkSize
			}

			if i == last {
				chunkTo = ((job.s3e.PBA + job.s3e.Len) * 512 - 1) % l2cache.ChunkSize + 1
			}
			go fillPartFromChunk(part, i, chunkTo, chunkFrom, &waitChunks, job.s3e.Key)

			if i != last {
				part = part[chunkTo-chunkFrom:]
			}
		}
		waitChunks.Wait()
		job.s3reads.Done()
	}
}

func cacheWriteWorker(jobs <-chan cacheWriteJob) {
	for job := range jobs {
		buf := make([]byte, job.e.Len*512)
		s3reads := new(sync.WaitGroup)
		for _, s3e := range *s3m.Find(job.e) {
			if s3e.Key == -1 {
				continue
			}
			s := (s3e.LBA - job.e.LBA) * 512
			slice := buf[s:]
			s3reads.Add(1)
			downloadChan <- downloadJob{s3e, &slice, s3reads}
		}
		s3reads.Wait()
		cache.Write(&buf, job.e.PBA*512)
		job.reads.Done()
	}
}

func (this *S3Backend) Read(extents *[]extent.Extent) {
	var reads sync.WaitGroup

	reads.Add(len(*extents))
	for i := range *extents {
		e := &(*extents)[i]
		cacheWriteChan <- cacheWriteJob{e, &reads}
	}
	reads.Wait()
}
