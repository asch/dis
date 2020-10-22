package main

import (
	"dis/backend"
	"dis/cache"
	"dis/ioctl"
	"dis/parser"
)

func main() {
	print("Initializing... ")

	parser.Init()
	cache.Init()
	ioctl.Init()
	backend.Init()

	println("Done")

	done := make(chan struct{})
	go ioctl.Read()
	go ioctl.Write()
	<-done
}
