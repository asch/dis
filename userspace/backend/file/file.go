// SPDX-License-Identifier: GPL-2.0-only
// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package file

import (
	"dis/cache"
	"dis/extent"
	"dis/parser"
	"golang.org/x/sys/unix"
	"sync"
)

const (
	configSection = "backend.file"
	envPrefix     = "dis_backend_file"
)

type FileBackend struct{}

var (
	file string
	fd   int
)

func (this *FileBackend) Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	v.BindEnv("file")
	file = v.GetString("file")

	if file == "" {
		panic("")
	}

	var err error
	fd, err = unix.Open(file, unix.O_RDWR, 0)
	if err != nil {
		panic(err)
	}
}

func (this *FileBackend) Write(extents *[]extent.Extent) {
	bufs := make(map[*extent.Extent]*[]byte)

	var reads sync.WaitGroup
	reads.Add(len(*extents))
	for i := range *extents {
		e := &(*extents)[i]
		buf := make([]byte, e.Len*512)
		bufs[e] = &buf

		go func() {
			cache.Read(&buf, e.PBA*512)
			reads.Done()
		}()
	}
	reads.Wait()

	var writes sync.WaitGroup
	writes.Add(len(*extents))
	for i := range *extents {
		e := &(*extents)[i]
		buf := bufs[e]
		go func() {
			_, err := unix.Pwrite(fd, *buf, e.LBA*512)
			if err != nil {
				panic(err)
			}
			writes.Done()
		}()
	}
	writes.Wait()
}

func (this *FileBackend) Read(extents *[]extent.Extent) {
	var reads sync.WaitGroup
	reads.Add(len(*extents))
	for i := range *extents {
		e := &(*extents)[i]
		cache.Reserve(e)

		go func() {
			buf := make([]byte, e.Len*512)
			_, err := unix.Pread(fd, buf, e.LBA*512)
			if err != nil {
				panic(err)
			}
			cache.Write(&buf, e.PBA*512)
			reads.Done()
		}()
	}
	reads.Wait()
}
