package s3

import (
	"dis/backend/s3/s3map"
	"dis/backend/s3/s3ops"
	"dis/cache"
	"dis/extent"
	"dis/parser"
	"fmt"
	"github.com/hashicorp/golang-lru/simplelru"
	"sync"
	"time"
)

const (
	configSection = "backend.s3"
	envPrefix     = "dis_backend_s3"
)

var (
	bucket    string
	region    string
	remote    string
	s3op      *s3ops.S3session
	s3m       *s3map.S3map
	workloads chan *[]extent.Extent
	lru       *simplelru.LRU
)

type S3Backend struct{}

const workloadsBuf = 1024 * 1024 * 2

func (this *S3Backend) Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	v.BindEnv("bucket")
	v.BindEnv("region")
	v.BindEnv("remote")
	bucket = v.GetString("bucket")
	region = v.GetString("region")
	remote = v.GetString("remote")

	if bucket == "" || region == "" || remote == "" {
		panic("")
	}

	s3op = s3ops.New(&s3ops.Options{Bucket: bucket, Region: region, Remote: remote})
	s3m = s3map.New()

	var err error
	lru, err = simplelru.NewLRU(200, nil)
	if err != nil {
		panic(err)
	}

	workloads = make(chan *[]extent.Extent, workloadsBuf)
	go this.writer()
}

const s3limit = 1024 * 1024 * 32

func (this *S3Backend) writer() {
	var reads sync.WaitGroup
	var upload sync.Mutex

	buf := make([]byte, 0, s3limit)
	writelist := []*s3map.S3extent{}

	var blocks int64
	var key int64

	for extents := range workloads {
		for i := range *extents {
			e := &(*extents)[i]
			if (blocks+e.Len)*512 > s3limit {
				upload.Lock()
				reads.Wait()
				go func(key int64, buf []byte, writelist []*s3map.S3extent) {
					s3op.Upload(key, &buf)
					s3m.Update(&writelist)
					upload.Unlock()
				}(key, buf, writelist)

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
			go func() {
				cache.Read(&slice, e.PBA*512)
				reads.Done()
			}()
		}
	}
}

func (this *S3Backend) Write(extents *[]extent.Extent) {
	workloads <- extents
}

var mutex sync.Mutex

func cachedDownload(s3e *s3map.S3extent, slice *[]byte) {
again:
	mutex.Lock()

	from := s3e.PBA * 512
	to := (s3e.PBA + s3e.Len) * 512

	if obj, ok := lru.Get(s3e.Key); ok && obj != nil {
		copy(*slice, (*obj.(*[]byte))[from:to])
	} else if ok && obj == nil {
		mutex.Unlock()
		time.Sleep(100 * time.Microsecond)
		goto again
	} else {
		lru.Add(s3e.Key, nil)
		mutex.Unlock()
		buf := make([]byte, s3limit)
		rng := "0-"
		s3op.Download(s3e.Key, &buf, &rng)
		copy(*slice, buf[from:to])
		mutex.Lock()
		lru.Add(s3e.Key, &buf)
	}

	mutex.Unlock()
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
