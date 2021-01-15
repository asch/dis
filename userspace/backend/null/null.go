package null

import (
	"dis/cache"
	"dis/extent"
	"dis/parser"
	"fmt"
	"sync"
)

const (
	configSection = "backend.null"
	envPrefix     = "dis_backend_null"
)

var (
	skipReadInWritePath bool
)

type NullBackend struct{}

func (this *NullBackend) Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)

	v.BindEnv("skipReadInWritePath")
	skipReadInWritePath = v.GetBool("skipReadInWritePath")

	fmt.Println("NullBackend.Init()")
}

func (this *NullBackend) Write(extents *[]extent.Extent) {
	if skipReadInWritePath {
		return
	}

	var wg sync.WaitGroup

	for i := range *extents {
		e := &(*extents)[i]
		wg.Add(1)
		go func() {
			buffer := make([]byte, e.Len*512)
			cache.Read(&buffer, e.PBA*512)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (this *NullBackend) Read(extents *[]extent.Extent) {
	fmt.Println("NullBackend.Read()")
}
