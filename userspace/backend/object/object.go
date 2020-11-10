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
	em        *extmap.ExtentMap
	workloads chan *[]extent.Extent
)

type ObjectBackend struct{}

func (this *ObjectBackend) Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)

	s3.Init()
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
