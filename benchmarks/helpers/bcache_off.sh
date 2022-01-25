#!/bin/bash
set -euxo pipefail

sudo bash -c 'echo 1 > /sys/block/nvme0n1/nvme0n1p1/bcache/set/stop'
sleep 1

sudo wipefs -a /dev/nvme0n1p1 /dev/rbd0
sudo rbd unmap /dev/rbd0
