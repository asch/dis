package cache

import (
	"dis/extent"
	"dis/parser"
	"errors"
	"golang.org/x/sys/unix"
)

const (
	configSection = "cache"
	envPrefix     = "dis_cache"
)

var (
	Base     int64
	Bound    int64
	Frontier int64
	file     string
	fd       int
)

func Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	v.BindEnv("base")
	v.BindEnv("bound")
	v.BindEnv("file")
	Base = v.GetInt64("base")
	Bound = v.GetInt64("bound")
	file = v.GetString("file")

	if Base == 0 || Bound == 0 || file == "" {
		panic(errors.New(""))
	}
	Frontier = Base

	var err error
	fd, err = unix.Open(file, unix.O_RDWR|unix.O_DIRECT, 0)
	if err != nil {
		panic(err)
	}
}

func Write(buf *[]byte, dest int64) {
	_, err := unix.Pwrite(fd, *buf, dest)
	if err != nil {
		panic(err)
	}
}

func Read(buf *[]byte, dest int64) {
	_, err := unix.Pread(fd, *buf, dest)
	if err != nil {
		panic(err)
	}
}

func Reserve(e *extent.Extent) {
	if Frontier+e.Len >= Bound {
		Frontier = Base
	}
	e.PBA = Frontier
	Frontier += roundUp(e.Len, 8)
}

func roundDown(x, y int64) int64 { return x - x%y }
func roundUp(x, y int64) int64   { return roundDown(x+y-1, y) }
