#!/bin/bash
# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

set -euxo pipefail

sudo pkill -2 dis$
sleep 1
sudo dmsetup remove --retry disa
sudo rmmod dm_disbd
