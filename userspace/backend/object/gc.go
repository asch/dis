// Copyright (C) 2020-2021 Vojtech Aschenbrenner <v@asch.cz>

package object

import (
	"dis/backend/object/api/s3"
	"dis/backend/object/extmap"
	"dis/backend/object/gc"
	"fmt"
	"sync"
	"time"
)

func getDownloadChan() chan downloadJob {
	ch := make(chan downloadJob)
	for i := 0; i < 5; i++ {
		go func() {
			for c := range ch {
				for {
					mutex.RLock()
					wait := uploading[c.e.Key]
					mutex.RUnlock()
					if wait != true {
						break
					}
					time.Sleep(500 * time.Microsecond)
				}
				partDownload(c.e, c.buf)
				c.reads.Done()
			}
		}()
	}

	return ch
}

func getUploadChan() (chan *Object, *sync.WaitGroup) {
	ch := make(chan *Object)
	var uploadsWG sync.WaitGroup
	for i := 0; i < 5; i++ {
		go func() {
			for c := range ch {
				c.reads.Wait()
				*c.buf = (*c.buf)[:cap(*c.buf)]
				s3.Upload(c.key, c.buf)
				uploadsWG.Done()
			}
		}()
	}

	return ch, &uploadsWG
}

func gcthread() {
	if gcMode != "on" && gcMode != "silent" {
		return
	}
	const gcPeriod = 5 * time.Second
	for {
		time.Sleep(gcPeriod)
		if !gc.Needed() {
			continue
		}
		gc.Running.Lock()
		em.RLock()
		fmt.Println("GC Started")
		purgeSet := gc.GetPurgeSetGreedy()
		fmt.Println("Objects viable for GC: ", len(*purgeSet))
		wl := em.GenerateWritelist(purgeSet)
		newPBAs := make([]int64, len(*wl))
		newKeys := make([]int64, len(*wl))
		slicedKeys := newKeys

		downloader := getDownloadChan()
		uploader, uploadsWG := getUploadChan()

		o := nextObject(true)
		upload := func() {
			if o.extents == 0 {
				return
			}
			o.assignKey()

			var i int64
			for i = 0; i < o.extents; i++ {
				slicedKeys[i] = o.key
			}
			slicedKeys = slicedKeys[i:]

			uploadsWG.Add(1)
			uploader <- o
			o = nextObject(true)
		}

		for i, e := range *wl {
			if o.size()+e.Len*512 > objectSize {
				upload()
			}

			newPBAs[i] = o.blocks
			slice := o.add(e.LBA, e.Len, true)

			o.reads.Add(1)
			go func(o *Object, e *extmap.Extent) {
				downloader <- downloadJob{e, &slice, o.reads}
			}(o, e)
		}
		upload()

		uploadsWG.Wait()
		em.RUnlock()
		em.Lock()

		for i, e := range *wl {
			e.PBA = newPBAs[i]
			e.Key = newKeys[i]
			gc.Add(e.Key, e.Len)
		}

		em.Unlock()
		gc.Running.Unlock()

		for key := range *purgeSet {
			s3.Void(key)
			gc.Destroy(key)
		}

		fmt.Println("GC Done")
	}
}

func gcthread2() {
	if gcMode != "on" && gcMode != "silent" {
		return
	}
	const gcPeriod = 5 * time.Second
	for {
		time.Sleep(gcPeriod)
		if !gc.Needed() {
			continue
		}
		gc.Running.Lock()
		em.RLock()

		fmt.Println("GC Started")
		purgeSet := gc.GetPurgeSetGreedy()
		fmt.Println("Objects viable for GC: ", len(*purgeSet))

		gc.Running.Unlock()
		em.RUnlock()

		downloader := getDownloadChan()

		// Buffer for downloaded objects
		var buffer map[int64][]byte

		// WaitGroup for running downloads
		var wg sync.WaitGroup

		for k, _ := range *purgeSet {
			// Object
			b := make([]byte, objectSize)

			// Extent for the whole object
			e := extmap.Extent{
				LBA: 0,
				PBA: 0,
				Len: objectSize / 512,
				Key: k,
			}

			wg.Add(1)
			downloader <- downloadJob{&e, &b, &wg}

			// Store object to buffer.
			//
			// Does not need to wait for download, just slice is
			// copied and waiting is done after the end of the loop.
			buffer[k] = b
		}
		wg.Wait()

		gc.Running.Lock()
		em.RLock()

		wl := em.GenerateWritelist(purgeSet)
		newPBAs := make([]int64, len(*wl))
		newKeys := make([]int64, len(*wl))
		slicedKeys := newKeys

		uploader, uploadsWG := getUploadChan()

		o := nextObject(true)
		upload := func() {
			if o.extents == 0 {
				return
			}
			o.assignKey()

			var i int64
			for i = 0; i < o.extents; i++ {
				slicedKeys[i] = o.key
			}
			slicedKeys = slicedKeys[i:]

			uploadsWG.Add(1)
			uploader <- o
			o = nextObject(true)
		}

		for i, e := range *wl {
			if o.size()+e.Len*512 > objectSize {
				upload()
			}

			newPBAs[i] = o.blocks
			slice := o.add(e.LBA, e.Len, true)

			// Just copy needed extents from buffered objects
			copy(slice, buffer[e.Key][e.PBA*512:(e.PBA+e.Len)*512])
		}
		upload()

		uploadsWG.Wait()
		em.RUnlock()
		em.Lock()

		for i, e := range *wl {
			e.PBA = newPBAs[i]
			e.Key = newKeys[i]
			gc.Add(e.Key, e.Len)
		}

		em.Unlock()
		gc.Running.Unlock()

		for key := range *purgeSet {
			s3.Void(key)
			gc.Destroy(key)
		}

		fmt.Println("GC Done")
	}
}
