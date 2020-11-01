package s3

import (
	"dis/backend/s3/s3map"
	"dis/backend/s3/s3ops"
	"dis/extent"
	"dis/parser"
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

	workloads = make(chan *[]extent.Extent, workloadsBuf)
	go writer()

	for i := 0; i < cacheWriteWorkers; i++ {
		go cacheWriteWorker(cacheWriteChan)
	}

	for i := 0; i < downloadWorkers; i++ {
		go downloadWorker(downloadChan)
	}
}
