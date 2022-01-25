#!/bin/bash
set -euxo pipefail

sudo wipefs -a /dev/rbd0
sudo rbd unmap /dev/rbd0
