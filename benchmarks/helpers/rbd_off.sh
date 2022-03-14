#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

set -euxo pipefail

sudo wipefs -a /dev/rbd0
sudo rbd unmap /dev/rbd0
