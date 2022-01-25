#!/bin/bash
set -euxo pipefail

if [[ $RBD_POOL == "rbd" ]]; then
	sudo rbd map drive_80G
else
	sudo rbd map drive_80G-EC
fi

sudo wipefs -a /dev/rbd0
