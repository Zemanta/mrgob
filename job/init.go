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

// InitRawJob initiates a raw mapreduce job, calling an appropriate function based on the mapreduce stage
func InitRawJob(mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader)) {
	switch initStage() {
	case "mapper":
		mapper(os.Stdout, os.Stdin)
	case "reducer":
		reducer(os.Stdout, os.Stdin)
	default:
		Log.Fatalln("stage must be either 'mapper' or 'reducer'")
	}
}

// InitByteJob initiates a byte reader/writer mapreduce job, calling an appropriate function based on the mapreduce stage
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

// InitByteJob initiates a json reader/writer mapreduce job, calling an appropriate function based on the mapreduce stage
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
