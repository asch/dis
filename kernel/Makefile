# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2016 Peter Desnoyers
# Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

CONFIG_MODULE_SIG=n

module := disbd
obj-m := dm-$(module).o

KDIR := /lib/modules/5.0.0-13-generic/build

PWD := $(shell pwd)
CFLAGS_dm-$(module).o += -DDEBUG -ggdb3 -Wno-unused-function -Wno-declaration-after-statement -Og

all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
