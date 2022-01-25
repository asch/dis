#!/bin/bash
set -euxo pipefail

cache_sectors=$((cache_size_M*1024*1024/512))

store_size_M=$((1024*80))
store_sectors=$((store_size_M*1024*1024/512))

loop=/dev/nvme0n1p1

sudo insmod ../kernel/dm-disbd.ko

echo 0 $store_sectors disbd $loop disa 0 $((cache_sectors/2)) $((cache_sectors/2 - 4*1024*1024*1024/512)) | sudo dmsetup --noudevsync create disa

sleep 1

cd ../userspace
export DIS_CACHE_BASE=$((cache_sectors/2))
export DIS_CACHE_BOUND=$cache_sectors
export DIS_CACHE_FILE=$loop
export DIS_IOCTL_CTL=/dev/disbd/disa
export AWS_ACCESS_KEY_ID="79RPB6L1D0AH02U1GS1Y"
export AWS_SECRET_ACCESS_KEY="cP7OEGfleOwkTksvKdIxigT0CKA4MCv510ffz1Vg"

echo "n" | sudo -E go run .
