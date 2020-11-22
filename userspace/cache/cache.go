package cache

import (
	"dis/extent"
	"dis/parser"
	"math"

	"golang.org/x/sys/unix"
)

const (
	configSection = "cache"
	envPrefix     = "dis_cache"
)

var (
	Base          int64
	Bound         int64
	Frontier      int64
	file          string
	fd            int
	headerSectors int64 = 8
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

type Prereader struct {
	buf  []byte
	off1 int64
	off2 int64
}

func NewPrereader(extents *[]extent.Extent) *Prereader {
	prereader := new(Prereader)
	var begin, end bool
	for i := range *extents {
		e := &(*extents)[i]
		if e.PBA == 0+headerSectors {
			begin = true
		}
		if e.PBA > 2*Base/3 {
			end = true
		}
	}
	wrapped := begin && end

	if wrapped {
		minL := int64(0)
		maxL := minL
		minR := Base
		maxR := int64(math.MinInt64)
		for i := range *extents {
			e := &(*extents)[i]
			if e.PBA < maxL || math.Abs(float64(e.PBA-maxL)) < math.Abs(float64(e.PBA+e.Len-minR)) {
				if e.PBA+e.Len > maxL {
					maxL = e.PBA + e.Len
				}
			} else {
				if e.PBA < minR {
					minR = e.PBA
				}
				if e.PBA+e.Len > maxR {
					maxR = e.PBA + e.Len
				}
			}
		}
		prereader.buf = make([]byte, (maxL-minL+maxR-minR)*512)
		prereader.off1 = minL
		prereader.off2 = minR
		bufL := prereader.buf[:(maxL-minL)*512]
		bufR := prereader.buf[(maxR-minR)*512:]
		Read(&bufL, minL*512)
		Read(&bufR, minR*512)
	} else {
		var min, max int64 = math.MaxInt64, math.MinInt64
		for i := range *extents {
			e := &(*extents)[i]
			if e.PBA < min {
				min = e.PBA
			}
			if e.PBA+e.Len > max {
				max = e.PBA + e.Len
			}
		}
		prereader.buf = make([]byte, (max-min)*512)
		prereader.off1 = min
		prereader.off2 = 0
		Read(&prereader.buf, min*512)
	}

	return prereader
}

func (this *Prereader) Copy(buf []byte, dest int64) {
	if this.off2 == 0 || dest < this.off2 {
		dest -= this.off1 * 512
	} else {
		dest -= this.off2 * 512
	}
	copy(buf, this.buf[dest:])
}
