package s3

import (
	"dis/backend/s3/s3map"
	"dis/cache"
	"dis/extent"
	"sync"
)

const workloadsBuf = 1024 * 1024 * 2
const s3limit = 1024 * 1024 * 32

func r(sliceCH chan *[]byte, pbaCH chan int64, reads *sync.WaitGroup) {
	for {
		slice := <-sliceCH
		pba := <-pbaCH
		cache.Read(slice, pba*512)
		reads.Done()
	}
}

func u(keyCH chan int64, bufCH chan []byte) {
	for {
		key := <-keyCH
		buf := <-bufCH
		s3op.Upload(key, &buf)
	}
}

func (this *S3Backend) writer() {
	sliceCH := make(chan *[]byte, 10)
	pbaCH := make(chan int64, 10)

	var reads sync.WaitGroup

	for i := 0; i < 10; i++ {
		go r(sliceCH, pbaCH, &reads)
	}

	keyCH := make(chan int64, 2)
	bufCH := make(chan []byte, 2)
	for i := 0; i < 3; i++ {
		go u(keyCH, bufCH)
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
				keyCH <- key
				bufCH <- buf

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
			sliceCH <- &slice
			pbaCH <- e.PBA
		}
	}
}

func (this *S3Backend) Write(extents *[]extent.Extent) {
	workloads <- extents
}
