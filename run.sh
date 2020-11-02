#/bin/bash
set -euxo pipefail

clean() {
	sudo dmsetup remove --retry s3a
	sudo rmmod dm_s3bd
	sudo losetup -d $loop
	sudo losetup -d $l2loop
	sudo rm -f $cache_path $store_path $l2cache_path
}

cache_path=$(pwd)/cache.raw
l2cache_path=$(pwd)/l2cache.raw
store_path=$(pwd)/store.raw

cache_size_M=16
cache_sectors=$((cache_size_M*1024*1024/512))

l2cache_size_M=$((256))
l2cache_sectors=$((l2cache_size_M*1024*1024/512))

store_size_M=1024
store_sectors=$((store_size_M*1024*1024/512))

sudo insmod kernel/dm-s3bd.ko

rm -f $store_path && truncate -s ${store_size_M}M $store_path
rm -f $cache_path && truncate -s ${cache_size_M}M $cache_path
rm -f $l2cache_path && truncate -s ${l2cache_size_M}M $l2cache_path

loop=$(sudo losetup -f --show $cache_path)
l2loop=$(sudo losetup -f --show $l2cache_path)
trap clean 0

echo 0 $store_sectors s3bd $loop s3a 0 $((cache_sectors/2)) 4096 | sudo dmsetup --noudevsync create s3a
sleep 1

(
cd userspace
export DIS_CACHE_BASE=$((cache_sectors/2))
export DIS_CACHE_BOUND=$cache_sectors
export DIS_CACHE_FILE=$loop
export DIS_L2CACHE_BASE=0
export DIS_L2CACHE_BOUND=$l2cache_sectors
export DIS_L2CACHE_FILE=$l2loop
export DIS_BACKEND_FILE_FILE=$store_path
export DIS_IOCTL_CTL=/dev/s3bd/s3a
export AWS_ACCESS_KEY_ID="Server-Access-Key"
export AWS_SECRET_ACCESS_KEY="Server-Secret-Key"

/bin/time sudo -E go run .
)
