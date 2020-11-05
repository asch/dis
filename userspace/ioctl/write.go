package ioctl

import (
	"dis/backend"
	"dis/cache"
)

func Write() {
	for {
		extents := RWIOCTL(writeNo)
		cache.WriteTrack(extents)
		backend.Write(extents)
	}
}
