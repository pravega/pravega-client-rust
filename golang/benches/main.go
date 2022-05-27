package main

// #include <pthread.h>
import "C"

import (
	"flag"
	"fmt"
	"os"
	stream_manager "github.com/vangork/pravega-client-rust/golang/pkg"
	"runtime/pprof"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

func main() {
	f, _ := os.Create("profile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	url := flag.String("url", "127.0.0.1:9090", "controller url")
	scope := flag.String("scope", "dell", "scope")
	streamName := flag.String("stream", "test", "stream")
	size := flag.Int("size", 1024*1024, "event size")
	count := flag.Int("count", 100, "event count")
	writerCount := flag.Int("writer_count", 1, "writer_count")
	inflight := flag.Int("inflight", 0, "inflight")
	flag.Parse()
	fmt.Println("url:", *url)
	fmt.Println("scope:", *scope)
	fmt.Println("stream:", *streamName)
	fmt.Println("size:", *size)
	fmt.Println("count:", *count)
	fmt.Println("writer_count:", *writerCount)
	fmt.Println("inflight:", *inflight)
	data := make([]byte, *size)
	for i := range data {
		data[i] = 'a'
	}

	sm, err := stream_manager.NewStreamManager(*url)

	if err != nil {
		log.Errorf("failed to create sm:%v", err)
		os.Exit(1)
	}

	_, err = sm.CreateScope(*scope)
	if err != nil {
		log.Errorf("failed to create scope:%v", err)
		os.Exit(1)
	}

	_, err = sm.CreateStream(*scope, *streamName, 3)
	if err != nil {
		log.Errorf("failed to create stream:%v", err)
		os.Exit(1)
	}

	sem := semaphore.NewWeighted(int64(*writerCount))
	ctx := context.TODO()
	timestamps := time.Now()
	for i := 0; i < *writerCount; i++ {
		sem.Acquire(ctx, 1)
		go func() {

			//runtime.LockOSThread()
			fmt.Println("threadId", C.pthread_self())
			sw, err := sm.CreateWriter(*scope, *streamName, uint(*inflight))
			if err != nil {
				log.Errorf("failed to create stream writer:%v", err)
			}
			num := *count

			for i := 0; i < num; i++ {
				s := uuid.New().String()
				sw.WriteEvent(string(data), s)
			}
			sw.Flush()
			sem.Release(1)
		}()
	}
	sem.Acquire(ctx, int64(*writerCount))
	milliseconds := time.Now().Sub(timestamps).Milliseconds()
	fmt.Printf("cost time: %d milliseconds\n", milliseconds)
	fmt.Printf("each event: %f milliseconds\n", float64(milliseconds)/float64(*count*(*writerCount)))
	if err != nil {
		log.Fatalf("%v", err)
	}
}
