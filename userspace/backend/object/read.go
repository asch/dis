package object

import (
	"dis/backend/object/extmap"
	"dis/backend/object/s3"
	"dis/cache"
	"dis/extent"
	"dis/l2cache"
	"sync"
	"time"
)

const (
	downloadWorkers   = 20
	cacheWriteWorkers = 20
)

var (
	cacheWriteChan = make(chan cacheWriteJob)
	downloadChan   = make(chan downloadJob)
)

func partDownload(e *extmap.Extent, slice *[]byte) {
	s3.Download(e.Key, slice, e.PBA*512, (e.PBA+e.Len)*512-1)
}

type cacheWriteJob struct {
	e     *extent.Extent
	reads *sync.WaitGroup
}

type downloadJob struct {
	e     *extmap.Extent
	buf   *[]byte
	reads *sync.WaitGroup
}

func fillPartFromChunk(slice []byte, chunkI int64, chunkTo, chunkFrom int64, wg *sync.WaitGroup, key int64) {
	id := func(key, chunk int64) int64 {
		return key*1000 + chunk
	}

	cacheKey := id(key, chunkI)
again:
	chunk, ok := l2cache.GetOrReserveChunk(cacheKey)
	if !ok {
		buf := make([]byte, l2cache.ChunkSize)
		s3.Download(key, &buf, chunkI*l2cache.ChunkSize, chunkI*l2cache.ChunkSize+l2cache.ChunkSize-1)
		l2cache.PutChunk(cacheKey, &buf)
		chunk = &buf
	} else if chunk == nil {
		time.Sleep(100 * time.Microsecond)
		goto again
	}
	copy(slice, (*chunk)[chunkFrom:chunkTo])
	wg.Done()
}

func downloadWorker(jobs <-chan downloadJob) {
	for job := range jobs {
		first := job.e.PBA * 512 / l2cache.ChunkSize
		last := (job.e.PBA + job.e.Len - 1) * 512 / l2cache.ChunkSize
		part := *job.buf
		var waitChunks sync.WaitGroup
		waitChunks.Add(int(last - first + 1))
		for i := first; i <= last; i++ {
			chunkFrom, chunkTo := int64(0), int64(l2cache.ChunkSize)
			if i == first {
				chunkFrom = job.e.PBA * 512 % l2cache.ChunkSize
			}

			if i == last {
				chunkTo = ((job.e.PBA+job.e.Len)*512-1)%l2cache.ChunkSize + 1
			}
			go fillPartFromChunk(part, i, chunkTo, chunkFrom, &waitChunks, job.e.Key)

			if i != last {
				part = part[chunkTo-chunkFrom:]
			}
		}
		waitChunks.Wait()
		job.reads.Done()
	}
}

func cacheWriteWorker(jobs <-chan cacheWriteJob) {
	for job := range jobs {
		buf := make([]byte, job.e.Len*512)
		s3reads := new(sync.WaitGroup)
		for _, e := range *em.Find(job.e) {
			if e.Key == -1 {
				continue
			}
			s := (e.LBA - job.e.LBA) * 512
			ss := s + e.Len*512
			slice := buf[s:ss]
			s3reads.Add(1)
			downloadChan <- downloadJob{e, &slice, s3reads}
		}
		s3reads.Wait()
		cache.Write(&buf, job.e.PBA*512)
		job.reads.Done()
	}
}

func (this *ObjectBackend) Read(extents *[]extent.Extent) {
	var reads sync.WaitGroup

	reads.Add(len(*extents))
	for i := range *extents {
		e := &(*extents)[i]
		cacheWriteChan <- cacheWriteJob{e, &reads}
	}
	reads.Wait()
}
