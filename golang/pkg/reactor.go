package pkg

import (
	"fmt"
)

// We declare the channel at this level, chan.go needs to see this too!
var stringChannel chan string

type Reactor struct {
	name string
}

func NewReactor(name string) *Reactor {
	// Initializes a channel with "string" type.
	stringChannel = make(chan string)

	return &Reactor{
		name: name,
	}
}

func (r *Reactor) Run() {
	go func() {
		for {
			select {
			case receivedString := <-stringChannel:
			  fmt.Println("stringChannel receives:", receivedString)
			}
		}
	}()
}
