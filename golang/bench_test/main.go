package main

import "C"

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	client "github.com/pravega/pravega-client-rust/golang/pkg"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

func main() {
	f, _ := os.Create("profile")
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	url := flag.String("uri", "127.0.0.1:9090", "controller uri")
	scope := flag.String("scope", "foo", "scope")
	stream := flag.String("stream", "bar", "stream")
	size := flag.Int("size", 1024, "event size")
	count := flag.Int("events", 100, "number of events")
	writerCount := flag.Int("writers", 1, "number of writers")
	inflight := flag.Int("inflight", 0, "inflight")
	flag.Parse()
	fmt.Println("url:", *url)
	fmt.Println("scope:", *scope)
	fmt.Println("stream:", *stream)
	fmt.Println("size:", *size)
	fmt.Println("count:", *count)
	fmt.Println("writers", *writerCount)
	fmt.Println("inflight:", *inflight)
	data := make([]byte, *size)
	for i := range data {
		data[i] = 'a'
	}

	manager, err := client.NewStreamManager(*url)
	if err != nil {
		log.Errorf("failed to create sm: %v", err)
		os.Exit(1)
	}
	defer manager.Close()

	_, err = manager.CreateScope(*scope)
	if err != nil {
		log.Errorf("failed to create scope: %v", err)
		os.Exit(1)
	}

	_, err = manager.CreateStream(*scope, *stream, 3)
	if err != nil {
		log.Errorf("failed to create stream: %v", err)
		os.Exit(1)
	}

	sem := semaphore.NewWeighted(int64(*writerCount))
	ctx := context.TODO()
	timestamps := time.Now()
	for i := 0; i < *writerCount; i++ {
		sem.Acquire(ctx, 1)
		go func() {
			writer, err := manager.CreateWriter(*scope, *stream, uint(*inflight))
			if err != nil {
				log.Errorf("failed to create stream writer: %v", err)
			}
			defer writer.Close()
			num := *count

			for i := 0; i < num; i++ {
				writer.WriteEvent(data)
			}
			writer.Flush()
			sem.Release(1)
		}()
	}
	sem.Acquire(ctx, int64(*writerCount))
	milliseconds := time.Now().Sub(timestamps).Milliseconds()
	fmt.Printf("cost time: %d milliseconds\n", milliseconds)
	fmt.Printf("each event: %f milliseconds\n", float64(milliseconds) / float64(*count * (*writerCount)))
	if err != nil {
		log.Fatalf("%v", err)
	}
}
