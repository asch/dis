#!/bin/bash
set -euxo pipefail

sudo mkfs.ext4 -F -F /dev/mapper/s3a
sudo mount /dev/mapper/s3a /mnt
sudo umount /mnt
