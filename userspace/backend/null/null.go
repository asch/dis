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
	waitForIoctlRound   bool
)

type NullBackend struct{}

func (this *NullBackend) Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)

	v.BindEnv("skipReadInWritePath")
	v.BindEnv("waitForIoctlRound")
	skipReadInWritePath = v.GetBool("skipReadInWritePath")
	waitForIoctlRound = v.GetBool("waitForIoctlRound")
}

func (this *NullBackend) Write(extents *[]extent.Extent) {
	if skipReadInWritePath {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(*extents))
	for i := range *extents {
		e := &(*extents)[i]
		go func() {
			buffer := make([]byte, e.Len*512)
			cache.Read(&buffer, e.PBA*512)
			wg.Done()
		}()
	}

	if waitForIoctlRound {
		wg.Wait()
	}
}

func (this *NullBackend) Read(extents *[]extent.Extent) {
	fmt.Println("NullBackend.Read()")
}
