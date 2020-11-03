package l2cache

import (
	"dis/parser"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/golang-lru"
	"golang.org/x/sys/unix"
)

const (
	configSection = "l2cache"
	envPrefix     = "dis_l2cache"
)

var (
	cache     *lru.Cache
	base      int64
	bound     int64
	file      string
	fd        int
	chunks    int64
	freeChunk int64
	ChunkSize int64
)

func Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	v.BindEnv("base")
	v.BindEnv("bound")
	v.BindEnv("file")
	v.BindEnv("chunksize")
	base = v.GetInt64("base")
	bound = v.GetInt64("bound")
	file = v.GetString("file")
	ChunkSize = v.GetInt64("chunksize")

	if ChunkSize == 0 || bound == 0 || file == "" {
		panic("")
	}

	chunks = (bound - base) * 512 / ChunkSize

	var err error
	cache, err = lru.NewWithEvict(int(chunks-1), onEvict)
	if err != nil {
		panic(err)
	}

	fd, err = unix.Open(file, unix.O_RDWR|unix.O_DIRECT, 0)
	if err != nil {
		panic(err)
	}
}

var mutex sync.Mutex

func GetOrReserveChunk(id int64) (*[]byte, bool) {
	mutex.Lock()
	defer mutex.Unlock()

	chunk, ok := cache.Get(id)
	if !ok {
		cache.Add(id, nil)
		return nil, false
	}

	if chunk == interface{}(nil) {
		return nil, true
	}

	buf := readChunk(chunk.(int64))
	return buf, true
}

func PutChunk(id int64, content *[]byte) {
	mutex.Lock()
	defer mutex.Unlock()

	current := atomic.LoadInt64(&freeChunk)
	if cache.Len() < int(chunks-1) {
		atomic.AddInt64(&freeChunk, 1)
	}

	writeChunk(current, content)
	cache.Add(id, current)
}

func onEvict(key interface{}, value interface{}) {
	atomic.StoreInt64(&freeChunk, value.(int64))
}

func writeChunk(chunk int64, buf *[]byte) {
	_, err := unix.Pwrite(fd, *buf, chunk*ChunkSize)
	if err != nil {
		panic(err)
	}
}

func readChunk(chunk int64) *[]byte {
	buf := make([]byte, ChunkSize)
	_, err := unix.Pread(fd, buf, chunk*ChunkSize)
	if err != nil {
		panic(err)
	}
	return &buf
}
