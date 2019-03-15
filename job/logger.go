package job

import (
	"io"
	"log"
	"os"
)

var MRLogPrefix = "[GOMR]"

type Logger interface {
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})
	Flags() int
	Output(calldepth int, s string) error
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})
	Prefix() string
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	SetFlags(flag int)
	SetOutput(w io.Writer)
	SetPrefix(prefix string)
}

// Log is an instance of go's logger which prefixes all log lines so they can be collected in the runner.
var Log Logger = log.New(os.Stderr, MRLogPrefix, log.Ldate|log.Ltime|log.Lshortfile)
