package profiler

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"time"
)

func enable() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	f, err := os.Create("cpu.pprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(f)

	ff, err := os.Create("mem.pprof")
	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(10 * time.Minute)
		pprof.StopCPUProfile()
		runtime.GC()
		pprof.WriteHeapProfile(ff)
		ff.Close()
		println("Profiling Stopped!")

	}()
}
