// SPDX-License-Identifier: GPL-2.0-only
// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package rados

import (
	"dis/parser"
	"fmt"
	"github.com/ceph/go-ceph/rados"
)

const (
	configSection = "backend.object.rados"
	envPrefix     = "dis_backend_object_rados"
)

var (
	conn *rados.Conn
	pool string
)

func Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)
	v.BindEnv("pool")
	pool = v.GetString("pool")

	if pool == "" {
		panic("")
	}

	var err error
	conn, err = rados.NewConn()
	if err != nil {
		panic(err)
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		panic(err)
	}

	err = conn.Connect()
	if err != nil {
		panic(err)
	}

	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		panic(err)
	}

	ioctx.ListObjects(func(oid string) { ioctx.Delete(oid) })

	go func() { ioctx.Destroy() }()
}

const keyFmt = "%08d"

func Upload(key int64, buf *[]byte) {
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		panic(err)
	}

	err = ioctx.Write(fmt.Sprintf(keyFmt, key), *buf, 0)
	if err != nil {
		panic(err)
	}

	go func() { ioctx.Destroy() }()
}

func Download(key int64, buf *[]byte, from, to int64) {
	if to-from+1 != int64(len(*buf)) {
		panic("")
	}
	ioctx, err := conn.OpenIOContext(pool)
	if err != nil {
		panic(err)
	}

	_, err = ioctx.Read(fmt.Sprintf(keyFmt, key), *buf, uint64(from))
	if err != nil {
		panic(err)
	}

	go func() { ioctx.Destroy() }()
}
