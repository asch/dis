// SPDX-License-Identifier: GPL-2.0-only
// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package main

import (
	"dis/backend"
	"dis/cache"
	"dis/ioctl"
	//"dis/l2cache"
	"dis/parser"
	"os"
)

func main() {
	print("Initializing... ")

	parser.Init()
	cache.Init()
	//l2cache.Init()
	ioctl.Init()
	backend.Init()

	println("Done")

	f, err := os.Create("/tmp/dis_ready")
	if err != nil {
		panic(err)
	}
	f.Close()

	done := make(chan struct{})
	go ioctl.Read()
	go ioctl.Write()
	<-done
}
