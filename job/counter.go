package job

import (
	"fmt"
	"os"
)

var AppCounterGroup = "GOMR"

var counterMsg = "reporter:counter:%s,%s,%d\n"

// Count increases hadoop counter for the running job. Use it for counting processed lines, errors etc.
func Count(name string, c int) {
	fmt.Fprintf(os.Stderr, counterMsg, AppCounterGroup, name, c)
}
