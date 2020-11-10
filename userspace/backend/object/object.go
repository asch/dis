package object

import (
	"dis/backend/object/extmap"
	"dis/backend/object/s3"
	"dis/extent"
	"dis/parser"
)

const (
	configSection = "backend.object"
	envPrefix     = "dis_backend_object"
)

var (
	bucket    string
	region    string
	remote    string
	s3s       *s3.S3session
	em        *extmap.ExtentMap
	workloads chan *[]extent.Extent
)

type ObjectBackend struct{}

func (this *ObjectBackend) Init() {
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

	s3s = s3.New(&s3.Options{Bucket: bucket, Region: region, Remote: remote})
	em = extmap.New()

	workloads = make(chan *[]extent.Extent, workloadsBuf)
	go writer()

	for i := 0; i < cacheWriteWorkers; i++ {
		go cacheWriteWorker(cacheWriteChan)
	}

	for i := 0; i < downloadWorkers; i++ {
		go downloadWorker(downloadChan)
	}
}
