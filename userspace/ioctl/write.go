package ioctl

import (
	"dis/backend"
	"dis/cache"
)

func Write() {
	for {
		extents := RWIOCTL(writeNo)
		cache.WriteReserve(extents)
		backend.Write(extents)
	}
}
