package cache

import (
	"dis/extent"
	"dis/parser"
	"math"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

const (
	configSection = "cache"
	envPrefix     = "dis_cache"
)

var (
	Base                  int64
	Bound                 int64
	Frontier              int64
	file                  string
	fd                    int
	availWriteSectors     int64
	maxUndoneWriteSectors int64 = 4096
	headerSectors         int64 = 8
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
		panic("")
	}
	Frontier = Base
	margin := maxUndoneWriteSectors + 128*4096
	availWriteSectors = Base - margin

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

func WriteTrack(e *[]extent.Extent) {
	var total int64
	for _, ee := range *e {
		total += ee.Len + headerSectors
	}

again:
	if atomic.LoadInt64(&availWriteSectors) > total {
		atomic.AddInt64(&availWriteSectors, -total)
		return
	}

	time.Sleep(100 * time.Microsecond)
	goto again
}

func WriteUntrackSingle(e *extent.Extent) {
	atomic.AddInt64(&availWriteSectors, e.Len+headerSectors)
}

func WriteUntrackMulti(e *[]*extent.Extent) {
	var total int64
	for _, ee := range *e {
		total += ee.Len + headerSectors
	}
	atomic.AddInt64(&availWriteSectors, total)
}

func roundDown(x, y int64) int64 { return x - x%y }
func roundUp(x, y int64) int64   { return roundDown(x+y-1, y) }
