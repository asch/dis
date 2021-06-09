// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package ioctl

import (
	"dis/extent"
	"dis/parser"
	"golang.org/x/sys/unix"
	"reflect"
	"unsafe"
)

const (
	configSection = "ioctl"
	envPrefix     = "dis_ioctl"
)

var (
	n     int
	ctl   string
	ctlFD int
)

func Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	v.BindEnv("ctl")
	v.BindEnv("extents")
	ctl = v.GetString("ctl")
	n = v.GetInt("extents")

	if n == 0 || ctl == "" {
		panic("")
	}

	var err error
	ctlFD, err = unix.Open(ctl, unix.O_RDWR, 0)
	if err != nil {
		panic(err)
	}
}

type ioctlRW struct {
	extentsN int
	extents  unsafe.Pointer
}

type ioctlResolve struct {
	extentsN         int
	extents          unsafe.Pointer
	clearLO, clearHI int64
}

func RWIOCTL(ioctlNo uint) *[]extent.Extent {
	extents := make([]extent.Extent, n)

	ioctl := ioctlRW{
		extentsN: len(extents),
		extents:  rawData(extents),
	}

	p := unsafe.Pointer(&ioctl)
	err := unix.IoctlSetInt(ctlFD, ioctlNo, int(uintptr(p)))
	if err != nil {
		panic(err)
	}

	updateLen(&extents)

	return &extents
}

func rawData(e []extent.Extent) unsafe.Pointer {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&e))
	raw := unsafe.Pointer(hdr.Data)

	return raw
}

func updateLen(extents *[]extent.Extent) {
	for i := range *extents {
		e := &(*extents)[i]
		if e.Len == 0 {
			*extents = (*extents)[:i]
			break
		}
	}
}

func resolveIOCTL(extents *[]extent.Extent, clearLO, clearHI int64) {
	resolve := ioctlResolve{
		extentsN: len(*extents),
		extents:  rawData(*extents),
		clearHI:  clearHI,
		clearLO:  clearLO,
	}

	p := unsafe.Pointer(&resolve)
	err := unix.IoctlSetInt(ctlFD, resolveNo(), int(uintptr(p)))
	if err != nil {
		panic(err)
	}
}
