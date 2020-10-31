package s3

import (
	"dis/backend/s3/s3map"
	"dis/backend/s3/s3ops"
	"dis/extent"
	"dis/parser"
	"github.com/hashicorp/golang-lru"
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
	l2cache   *lru.TwoQueueCache
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

	var err error
	l2cache, err = lru.New2Q(100)
	if err != nil {
		panic(err)
	}

	workloads = make(chan *[]extent.Extent, workloadsBuf)
	go writer()
}
