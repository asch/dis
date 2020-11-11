#ifndef __DISBD_U_H__
#define __DISBD_U_H__

#include <asm/ioctl.h>
#include <stdint.h>

#define DIS_HDR_MAGIC 0x4c443353
#define DIS_HDR_SIZE 4096

struct _disheader {
	uint32_t magic;
	uint32_t seq;
	uint32_t crc32;
	uint16_t n_extents;
	uint16_t n_sectors;
} __attribute__((packed));

struct _ext_local {
	uint64_t lba : 47;
	uint64_t len : 16;
	uint64_t dirty : 1;
} __attribute__((packed));

#define HDR_EXTS ((DIS_HDR_SIZE - sizeof(struct _disheader)) / sizeof(struct _ext_local))

struct dis_header {
	struct _disheader h;
	struct _ext_local extents[HDR_EXTS];
};

#ifdef _KERNEL_
BUILD_BUG_ON(sizeof(struct dis_header) != DIS_HDR_SIZE)
#endif

#define MAGIC_NUMBER 100
#define IOCTL_DIS_NO_OP _IO(MAGIC_NUMBER, 0)

#define PBA_NONE (0x7FULL << 40)

struct dis_extent {
	uint64_t lba;
	uint64_t pba;
	uint64_t len;
};

struct ioctl_get_map {
	uint32_t map;
	uint64_t start;
	uint32_t n_extents;
	uint32_t total_extents;
	struct dis_extent *extents;
};

#define IOCTL_DIS_GET_MAP _IOWR(MAGIC_NUMBER, 1, struct ioctl_get_map)

struct ioctl_writes {
	uint64_t n_extents;
	struct dis_extent *extents;
};

#define IOCTL_DIS_WRITES _IOWR(MAGIC_NUMBER, 2, struct ioctl_writes)

struct ioctl_reads {
	uint64_t n_extents;
	struct dis_extent *extents;
};

#define IOCTL_DIS_READS _IOWR(MAGIC_NUMBER, 3, struct ioctl_reads)

struct ioctl_resolve {
	uint64_t n_extents;
	struct dis_extent *extents;
	uint64_t clear_lo;
	uint64_t clear_hi;
};

#define IOCTL_DIS_RESOLVE _IOW(MAGIC_NUMBER, 4, struct ioctl_resolve)

#endif
