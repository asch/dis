#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

set -euxo pipefail

sudo mkfs.ext4 -F -F /dev/mapper/disa
sudo fsck.ext4 -f /dev/mapper/disa

sudo mount /dev/mapper/disa /mnt
sudo umount /mnt
