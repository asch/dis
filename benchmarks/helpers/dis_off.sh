#!/bin/bash
set -euxo pipefail

sudo pkill -2 dis$
sleep 1
sudo dmsetup remove --retry disa
sudo rmmod dm_disbd
