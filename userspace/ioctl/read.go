package ioctl

import (
	"dis/backend"
	"dis/cache"
)

func Read() {
	var nextClean int64
	for {
		extents := RWIOCTL(readNo)
		backend.Read(extents)

		octant := 8 * (cache.Frontier - cache.Base) / (cache.Bound - cache.Base)
		eight := (cache.Bound - cache.Base) / 8

		var clearLO, clearHI int64
		if (octant+2)%8 == nextClean {
			clearLO = cache.Base + nextClean*eight
			clearHI = clearLO + eight
			nextClean = (nextClean + 1) % 8
		}

		resolveIOCTL(extents, clearLO, clearHI)
	}
}
