package main

import "C"

import (
	"flag"
	"fmt"
	"time"

	client "github.com/pravega/pravega-client-rust/golang/pkg"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
)

func main() {
	// uncomment the below if you want to know the cpu usage
	// f, _ := os.Create("profile")
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()
	uri := flag.String("uri", "127.0.0.1:9090", "controller uri")
	scope := flag.String("scope", "foo", "scope")
	stream := flag.String("stream", "bar", "stream")
	size := flag.Int("size", 1024, "event size")
	count := flag.Int("events", 100, "number of events")
	writerCount := flag.Int("writers", 1, "number of writers")
	flag.Parse()
	fmt.Println("uri:", *uri)
	fmt.Println("scope:", *scope)
	fmt.Println("stream:", *stream)
	fmt.Println("size:", *size)
	fmt.Println("events:", *count)
	fmt.Println("writers", *writerCount)
	data := make([]byte, *size)
	for i := range data {
		data[i] = 'a'
	}

	config := client.NewClientConfig()
	config.ControllerUri = *uri
	manager, err := client.NewStreamManager(config)
	if err != nil {
		log.Fatalf("failed to create sm: %v", err)
	}
	defer manager.Close()

	_, err = manager.CreateScope(*scope)
	if err != nil {
		log.Fatalf("failed to create scope: %v", err)
	}

	streamConfig := client.NewStreamConfiguration(*scope, *stream)
	streamConfig.Scale.MinSegments = 3

	_, err = manager.CreateStream(streamConfig)
	if err != nil {
		log.Fatalf("failed to create stream: %v", err)
	}

	sem := semaphore.NewWeighted(int64(*writerCount))
	ctx := context.TODO()
	start := time.Now()
	for i := 0; i < *writerCount; i++ {
		sem.Acquire(ctx, 1)
		go func() {
			writer, err := manager.CreateWriter(*scope, *stream)
			if err != nil {
				log.Fatalf("failed to create stream writer: %v", err)
			}
			defer writer.Close()

			for j := 0; j < *count; j++ {
				err = writer.WriteEvent(data)
				if err != nil {
					log.Fatalf("failed to write event: %v", err)
				}
			}
			writer.Flush()
			sem.Release(1)
		}()
	}
	sem.Acquire(ctx, int64(*writerCount))
	elapse := time.Since(start).Milliseconds()
	fmt.Printf("cost time: %d milliseconds\n", elapse)
	fmt.Printf("each event: %f milliseconds\n", float64(elapse) / float64(*count * (*writerCount)))
}
