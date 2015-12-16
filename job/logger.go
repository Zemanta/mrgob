package job

import (
	"log"
	"os"
)

var MRLogPrefix = "[GOMR]"

// Log is an instance of go's logger which prefixes all log lines so they can be collected in the runner.
var Log = log.New(os.Stderr, MRLogPrefix, log.Ldate|log.Ltime|log.Lshortfile)
