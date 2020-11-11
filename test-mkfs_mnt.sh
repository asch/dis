#!/bin/bash
set -euxo pipefail

sudo mkfs.ext4 -F -F /dev/mapper/disa
sudo mount /dev/mapper/disa /mnt
sudo umount /mnt
