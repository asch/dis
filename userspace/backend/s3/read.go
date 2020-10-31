package s3

import (
	"dis/backend/s3/s3map"
	"dis/cache"
	"dis/extent"
	"fmt"
	"sync"
	"time"
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

func (this *S3Backend) Read(extents *[]extent.Extent) {
	var reads sync.WaitGroup

	reads.Add(len(*extents))
	for i := range *extents {
		e := &(*extents)[i]
		go func() {
			buf := make([]byte, e.Len*512)
			var s3reads sync.WaitGroup
			for _, s3e := range *s3m.Find(e) {
				if s3e.Key != -1 {
					s3reads.Add(1)
					go func(s3e *s3map.S3extent, e *extent.Extent) {
						s := (s3e.LBA - e.LBA) * 512
						slice := buf[s:]
						//partDownload(s3e, &slice)
						cachedDownload(s3e, &slice)
						s3reads.Done()
					}(s3e, e)
				}
			}
			s3reads.Wait()
			cache.Write(&buf, e.PBA*512)
			reads.Done()
		}()
	}
	reads.Wait()
}
