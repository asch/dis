package ioctl

import (
	"dis/backend"
)

func Write() {
	for {
		extents := RWIOCTL(writeNo())
		// FIXME: Probable bug in kernel code, sometimes zero-length ioctl set is being sent
		if len(*extents) == 0 {
			println("W IOCTL: Zero-length extent set received from kernel!")
			continue
		}
		backend.Write(extents)
	}
}
