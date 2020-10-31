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
	uploadWorkers = 10
	uploadBuf     = 10
	readWorkers   = 10
	readBuf       = 10
)

type uploadJob struct {
	key int64
	buf []byte
}

type readJob struct {
	pba int64
	buf *[]byte
}

func uploadWorker(jobs <-chan uploadJob) {
	for job := range jobs {
		s3op.Upload(job.key, &job.buf)
	}
}

func readWorker(jobs <-chan readJob, reads *sync.WaitGroup) {
	for job := range jobs {
		cache.Read(job.buf, job.pba*512)
		reads.Done()
	}
}

func (this *S3Backend) writer() {
	var reads sync.WaitGroup

	readChan := make(chan readJob, readBuf)
	for i := 0; i < readWorkers; i++ {
		go readWorker(readChan, &reads)
	}

	uploadChan := make(chan uploadJob, uploadBuf)
	for i := 0; i < uploadWorkers; i++ {
		go uploadWorker(uploadChan)
	}

	buf := make([]byte, 0, s3limit)
	writelist := []*s3map.S3extent{}

	var blocks int64
	var key int64

	for extents := range workloads {
		for i := range *extents {
			e := &(*extents)[i]
			if (blocks+e.Len)*512 > s3limit {
				reads.Wait()
				s3m.Update(&writelist)
				uploadChan <- uploadJob{key, buf}

				buf = make([]byte, 0, s3limit)
				writelist = []*s3map.S3extent{}
				blocks = 0
				key++
			}
			buf = buf[:(blocks+e.Len)*512]
			slice := buf[blocks*512:]

			writelist = append(writelist, &s3map.S3extent{
				LBA: e.LBA,
				PBA: blocks,
				Len: e.Len,
				Key: key})

			blocks += e.Len
			reads.Add(1)
			readChan <- readJob{e.PBA, &slice}
		}
	}
}

func (this *S3Backend) Write(extents *[]extent.Extent) {
	workloads <- extents
}
