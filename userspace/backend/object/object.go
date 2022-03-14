// SPDX-License-Identifier: GPL-2.0-only
// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package object

import (
	"dis/backend/object/api/rados"
	"dis/backend/object/api/s3"
	"dis/backend/object/extmap"
	"dis/backend/object/gc"
	"dis/extent"
	"dis/parser"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	configSection = "backend.object"
	envPrefix     = "dis_backend_object"
)

var (
	bucket      string
	region      string
	remote      string
	em          *extmap.ExtentMap
	workloads   chan *[]extent.Extent
	seqNumber   int64
	api         string
	gcMode      string
	gcVersion   int64
	objectSizeM int64
	objectSize  int64
	uploadF     func(key int64, buf *[]byte)
	downloadF   func(key int64, buf *[]byte, from, to int64)
)

type ObjectBackend struct{}

func (this *ObjectBackend) Init() {
	v := parser.Sub(configSection)
	v.SetEnvPrefix(envPrefix)

	v.BindEnv("api")
	v.BindEnv("gcMode")
	v.BindEnv("gcVersion")
	v.BindEnv("objectSizeM")
	api = v.GetString("api")
	gcMode = v.GetString("gcMode")
	gcVersion = v.GetInt64("gcVersion")
	objectSizeM = v.GetInt64("objectSizeM")
	objectSize = objectSizeM * 1024 * 1024

	if gcMode != "on" && gcMode != "statsOnly" && gcMode != "off" && gcMode != "silent" && objectSize == 0 {
		panic("")
	}

	em = extmap.New()

	if api == "s3" {
		uploadF = s3.Upload
		downloadF = s3.Download
		s3.FnHeaderToMap = func(header *[]byte, key, size int64) {
			atomic.StoreInt64(&seqNumber, key+1)
			const int64Size = 8
			var blocks int64 = (writelistLen * 16) / 512

			gc.Create(key, size/512)
			for i := 0; i < len(*header); i += 2 * int64Size {
				var e extmap.Extent
				e.Key = key
				e.PBA = blocks
				e.LBA, _ = binary.Varint((*header)[i : i+int64Size])
				e.Len, _ = binary.Varint((*header)[i+int64Size : i+2*int64Size])
				if e.Len == 0 {
					break
				}
				em.UpdateSingle(&e)
				blocks += e.Len
			}
		}
		s3.Init()
	} else if api == "rados" {
		uploadF = rados.Upload
		downloadF = rados.Download
		rados.Init()
	} else {
		panic("")
	}

	workloads = make(chan *[]extent.Extent)
	go writer()

	for i := 0; i < cacheWriteWorkers; i++ {
		go cacheWriteWorker(cacheWriteChan)
	}

	for i := 0; i < downloadWorkers; i++ {
		//go downloadWorker(downloadChan)
		go func() {
			for d := range downloadChan {
				for {
					mutex.RLock()
					wait := uploading[d.e.Key]
					mutex.RUnlock()
					if wait != true {
						break
					}
					time.Sleep(500 * time.Microsecond)
				}
				partDownload(d.e, d.buf)
				d.reads.Done()
			}
		}()
	}

	switch gcVersion {
	case 1:
		go gcthread()
	case 2:
		go gcthread2()
	default:
		go gcthread()
	}

	go func() {
		if gcMode != "on" && gcMode != "statsOnly" {
			return
		}

		fmt.Println("STATS: time,total,valid,invalid,ratio,gcmode")
		const delaySec = 5
		for {
			gc.PrintStats(delaySec, gcMode)
			time.Sleep(delaySec * time.Second)
		}
	}()
}
