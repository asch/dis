#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

set -euxo pipefail

if [[ $RBD_POOL == "rbd" ]]; then
	sudo rbd map drive_80G
else
	sudo rbd map drive_80G-EC
fi

sudo wipefs -a /dev/nvme0n1p1 /dev/rbd0
sudo make-bcache --wipe-cache -C /dev/nvme0n1p1 -B /dev/rbd0
sleep 5

set +e
sudo bash -c 'echo /dev/rbd0 > /sys/fs/bcache/register'
sudo bash -c 'echo /dev/nvme0n1p1 > /sys/fs/bcache/register'
set -e

sudo bash -c 'echo writeback > /sys/block/bcache0/bcache/cache_mode'
sudo bash -c 'echo 0 > /sys/block/bcache0/bcache/sequential_cutoff'
sudo bash -c 'echo 0 > /sys/fs/bcache/**/congested_read_threshold_us'
sudo bash -c 'echo 0 > /sys/fs/bcache/**/congested_write_threshold_us'
