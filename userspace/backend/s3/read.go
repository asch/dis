package s3

import (
	"dis/backend/s3/s3map"
	"dis/cache"
	"dis/extent"
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

func cachedDownload(s3e *s3map.S3extent, slice *[]byte) {
	from := s3e.PBA * 512
	to := (s3e.PBA + s3e.Len) * 512

again:
	if obj, ok := l2cache.Get(s3e.Key); ok && obj != nil {
		copy(*slice, (*obj.(*[]byte))[from:to])
	} else if ok && obj == nil {
		time.Sleep(100 * time.Microsecond)
		goto again
	} else {
		l2cache.Add(s3e.Key, nil)
		buf := make([]byte, s3limit)
		rng := "bytes=0-"
		s3op.Download(s3e.Key, &buf, &rng)
		copy(*slice, buf[from:to])
		l2cache.Add(s3e.Key, &buf)
	}
}

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

func downloadWorker(jobs <-chan downloadJob) {
	for job := range jobs {
		cachedDownload(job.s3e, job.buf)
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
