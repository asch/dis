package object

import (
	"dis/backend/object/extmap"
	"dis/backend/object/gc"
	"dis/backend/object/s3"
	"fmt"
	"sync"
	"time"
)

func getDownloadChan() chan downloadJob {
	ch := make(chan downloadJob)
	for i := 0; i < 5; i++ {
		go func() {
			for c := range ch {
				mutex.RLock()
			again:
				if uploading[c.e.Key] {
					time.Sleep(500 * time.Microsecond)
					goto again
				}
				mutex.RUnlock()
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
				s3.Upload(c.key, c.buf)
				uploadsWG.Done()
			}
		}()
	}

	return ch, &uploadsWG
}

func gcthread() {
	const gcPeriod = 120 * time.Second
	for {
		time.Sleep(gcPeriod)
		//gc.Running.Add(1)
		gc.Running.Lock()
		em.RLock()
		fmt.Println("GC Started")
		purgeSet := gc.GetPurgeSet()
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

			slice := o.add(e.LBA, e.Len, true)
			newPBAs[i] = o.blocks

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
		//gc.Running.Done()
		gc.Running.Unlock()

		for key := range *purgeSet {
			s3.Void(key)
			gc.Destroy(key)
		}

		fmt.Println("GC Done")
	}
}
