package job

import (
	"flag"
	"io"
	"os"
)

func initStage() string {
	var runStage = flag.String("stage", "", "specify the stage to run.  Can be 'mapper' or 'reducer'")
	flag.Parse()

	if *runStage == "" {
		flag.PrintDefaults()
		return ""
	}

	return *runStage
}

func InitRawJob(mapper func(), reducer func()) {
	switch initStage() {
	case "mapper":
		mapper()
	case "reducer":
		reducer()
	default:
		Log.Fatalln("stage must be either 'mapper' or 'reducer'")
	}
}

func InitStringJob(mapper func(*StringKVWriter, io.Reader), reducer func(io.Writer, *StringKVReader)) {
	switch initStage() {
	case "mapper":
		mapper(NewStringKVWriter(os.Stdout), os.Stdin)
	case "reducer":
		reducer(os.Stdout, NewStringKVReader(os.Stdin))
	default:
		Log.Fatalln("stage must be either 'mapper' or 'reducer'")
	}
}
