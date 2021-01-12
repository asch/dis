package null

import (
	"dis/extent"
	"dis/parser"
	"fmt"
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
	fmt.Println("NullBackend.Write()")
}

func (this *NullBackend) Read(extents *[]extent.Extent) {
	fmt.Println("NullBackend.Read()")
}
