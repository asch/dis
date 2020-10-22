package ioctl

import (
	"dis/backend"
)

func Write() {
	for {
		extents := RWIOCTL(writeNo)
		backend.Write(extents)
	}
}
