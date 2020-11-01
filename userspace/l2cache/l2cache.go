package l2cache

import (
	"github.com/hashicorp/golang-lru"
)

const (
	ChunkSize = 1024 * 1024
	chunks = 1000
)

var (
	cache *lru.Cache
)

func Init() {
	var err error
	cache, err = lru.New(chunks)
	if err != nil {
		panic(err)
	}
}

func GetChunk(id int64) (*[]byte, bool) {
	val, ok := cache.Get(id)
	if !ok {
		return nil, false
	}
	return val.(*[]byte), true
}

func PutChunk(id int64, content *[]byte) {
	cache.Add(id, content)
}
