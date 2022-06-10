package pkg

/*
#include <stdlib.h>

#include "common.h"
*/
import "C"

// This is an exported function. It takes a *C.Char, converts it to a Go string and sends it to the channel.
//export publishString
func publishString() {
	var goString string = "Hello from c code"
	stringChannel <- goString
	return
}
