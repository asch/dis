// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package ioctl

import (
	"dis/backend"
	"dis/cache"
	"fmt"
)

func Read() {
	var nextClean int64

	for {
		extents := RWIOCTL(readNo())
		// FIXME: Probable bug in kernel code, sometimes zero-length ioctl set is being sent
		if len(*extents) == 0 {
			println("R IOCTL: Zero-length extent set received from kernel!")
			continue
		}
		for i := range *extents {
			e := &(*extents)[i]
			cache.Reserve(e)
		}
		backend.Read(extents)

		// FIXME: If the length of read extents in this round makes the
		// frontier to jump over two octants it fails to clean the
		// skipped octant.
		octant := 8 * (cache.Frontier - cache.Base) / (cache.Bound - cache.Base)
		eight := (cache.Bound - cache.Base) / 8

		var clearLO, clearHI int64
		if (octant+2)%8 == nextClean {
			clearLO = cache.Base + nextClean*eight
			clearHI = clearLO + eight
			nextClean = (nextClean + 1) % 8
			fmt.Println("Cleaning from ", clearLO, "to ", clearHI)
		}

		resolveIOCTL(extents, clearLO, clearHI)
	}
}
