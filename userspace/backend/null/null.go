package null

import (
	"dis/extent"
	"fmt"
)

type NullBackend struct{}

func (this *NullBackend) Init() {
	fmt.Println("NullBackend.Init()")
}

func (this *NullBackend) Write(extents *[]extent.Extent) {
	fmt.Println("NullBackend.Write()")
}

func (this *NullBackend) Read(extents *[]extent.Extent) {
	fmt.Println("NullBackend.Read()")
}
