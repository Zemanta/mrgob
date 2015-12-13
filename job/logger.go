package job

import (
	"log"
	"os"
)

var MRLogPrefix = "[GOMR]"

var Log = log.New(os.Stderr, MRLogPrefix, log.Ldate|log.Ltime|log.Lshortfile)
