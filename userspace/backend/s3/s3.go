package s3

import (
	"dis/backend/s3/s3map"
	"dis/backend/s3/s3ops"
	"dis/cache"
	"dis/extent"
	"dis/parser"
	"fmt"
	"sync"
)

const (
	configSection = "backend.s3"
	envPrefix     = "dis_backend_s3"
)

var (
	bucket string
	region string
	remote string
	s3op   *s3ops.S3session
	s3m    *s3map.S3map
	workloads chan *[]extent.Extent
)

type S3Backend struct{}

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

	workloads = make(chan *[]extent.Extent, 1024*1024*1024)
	go this.writer()
}

const s3limit = 1024 * 1024 * 128

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

func (this *S3Backend) Read(extents *[]extent.Extent) {
	var reads sync.WaitGroup

	for i := range *extents {
		e := &(*extents)[i]
		cache.Reserve(e)
		reads.Add(1)
		go func() {
			buf := make([]byte, e.Len*512)
			for _, s3e := range *s3m.Find(e) {
				if s3e.Key != -1 {
					s := (s3e.LBA - e.LBA) * 512
					slice := buf[s:]
					from := fmt.Sprintf("%d", s3e.PBA*512)
					to := fmt.Sprintf("%d", (s3e.PBA+s3e.Len)*512-1)
					rng := "bytes=" + from + "-" + to
					s3op.Download(s3e.Key, &slice, &rng)
				}
			}

			cache.Write(&buf, e.PBA*512)
			reads.Done()
		}()
	}

	reads.Wait()
}
