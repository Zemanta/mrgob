package job

import (
	"fmt"
	"os"
)

var AppCounterGroup = "GOMR"
var CounterPipe = os.Stderr

var counterMsg = "reporter:counter:%s,%s,%d\n"

// Count increases hadoop counter for the running job. Use it for counting processed lines, errors etc.
func Count(name string, c int) {
	if CounterPipe == nil {
		return
	}
	fmt.Fprintf(CounterPipe, counterMsg, AppCounterGroup, name, c)
}
