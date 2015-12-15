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

func InitByteJob(mapper func(*ByteKVWriter, io.Reader), reducer func(io.Writer, *ByteKVReader)) {
	switch initStage() {
	case "mapper":
		mapper(NewByteKVWriter(os.Stdout), os.Stdin)
	case "reducer":
		reducer(os.Stdout, NewByteKVReader(os.Stdin))
	default:
		Log.Fatalln("stage must be either 'mapper' or 'reducer'")
	}
}

func InitJsonJob(mapper func(*JsonKVWriter, io.Reader), reducer func(io.Writer, *JsonKVReader)) {
	switch initStage() {
	case "mapper":
		mapper(NewJsonKVWriter(os.Stdout), os.Stdin)
	case "reducer":
		reducer(os.Stdout, NewJsonKVReader(os.Stdin))
	default:
		Log.Fatalln("stage must be either 'mapper' or 'reducer'")
	}
}
