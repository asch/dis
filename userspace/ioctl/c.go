package ioctl

// #include <stdint.h>
// #include "../../kernel/dm-disbd.h"
import "C"

func readNo() uint {
	return C.IOCTL_DIS_READS
}

func writeNo() uint {
	return C.IOCTL_DIS_WRITES
}

func resolveNo() uint {
	return C.IOCTL_DIS_RESOLVE
}
