#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

set -euxo pipefail

if [[ $RBD_POOL == "rbd" ]]; then
	sudo rbd map drive_80G
else
	sudo rbd map drive_80G-EC
fi

sudo wipefs -a /dev/rbd0
