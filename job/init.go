package job

import "flag"

// Runs the mapper or reducer stage depending on the input
func InitRawJob(mapper func(), reducer func()) {
	var runStage = flag.String("stage", "", "specify the stage to run.  Can be 'mapper' or 'reducer'")
	flag.Parse()

	if *runStage == "" {
		flag.PrintDefaults()
		return
	}

	switch *runStage {
	case "mapper":
		mapper()
	case "reducer":
		reducer()
	default:
		Log.Fatalln("stage must be either 'mapper' or 'reducer'")
	}
}
