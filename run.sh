#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

set -euxo pipefail

clean() {
	sudo dmsetup remove --retry disa
	sudo rmmod dm_disbd
	sudo losetup -d $loop
	sudo rm -f $cache_path $store_path
}

make -C kernel

cache_path=$(pwd)/cache.raw
store_path=$(pwd)/store.raw

#cache_size_M=1
cache_size_M=4096
cache_sectors=$((cache_size_M*1024*1024/512))

l2cache_size_M=8
l2cache_sectors=$((l2cache_size_M*1024*1024/512))

#store_size_M=128
store_size_M=$((10*1024))
store_sectors=$((store_size_M*1024*1024/512))

sudo insmod kernel/dm-disbd.ko

rm -f $store_path && truncate -s ${store_size_M}M $store_path
rm -f $cache_path && truncate -s $((cache_size_M + l2cache_size_M))M $cache_path

loop=$(sudo losetup -f --show $cache_path)
trap clean 0

max_w_ioctl_sectors=$((420*1024/512))
#max_w_ioctl_sectors=0
echo 0 $store_sectors disbd $loop disa 0 $((cache_sectors/2)) $((cache_sectors/2 - max_w_ioctl_sectors)) | sudo dmsetup --noudevsync create disa
sleep 1

(
cd userspace
export DIS_CACHE_BASE=$((cache_sectors/2))
export DIS_CACHE_BOUND=$cache_sectors
export DIS_CACHE_FILE=$loop
export DIS_L2CACHE_BASE=$cache_sectors
export DIS_L2CACHE_BOUND=$((cache_sectors + l2cache_sectors))
export DIS_L2CACHE_FILE=$loop
export DIS_L2CACHE_CHUNKSIZE=$((1024*1024))
export DIS_BACKEND_ENABLED="object"
export DIS_BACKEND_FILE_FILE=$store_path
export DIS_BACKEND_OBJECT_API="s3"
#export DIS_BACKEND_OBJECT_OBJECTSIZEM=4
export DIS_BACKEND_OBJECT_OBJECTSIZEM=32
export DIS_BACKEND_OBJECT_GCMODE="off"

# What gcthread function should be used
# 1: Old version with range reads
# 2: New version downloading the whole object first
export DIS_BACKEND_OBJECT_GCVERSION=2

export DIS_BACKEND_OBJECT_S3_BUCKET="dis2"
export DIS_BACKEND_OBJECT_S3_REGION="us-east-1"
export DIS_BACKEND_OBJECT_S3_REMOTE="http://192.168.122.1:9000"
export DIS_BACKEND_OBJECT_RADOS_POOL="ec-pool"
export DIS_BACKEND_NULL_SKIPREADINWRITEPATH="false"
export DIS_BACKEND_NULL_WAITFORIOCTLROUND="true"
export DIS_IOCTL_CTL=/dev/disbd/disa
#export DIS_IOCTL_EXTENTS=8
export DIS_IOCTL_EXTENTS=128
export AWS_ACCESS_KEY_ID="Server-Access-Key"
export AWS_SECRET_ACCESS_KEY="Server-Secret-Key"

/bin/time sudo -E go run .
)
