package job

import (
	"fmt"
	"os"
)

var (
	AppCounterGroup = "GOMR"

	counterMsg = "reporter:counter:%s,%s,%d\n"
)

func Count(name string, c int) {
	fmt.Fprintf(os.Stderr, counterMsg, AppCounterGroup, name, c)
}
