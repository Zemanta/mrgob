# Bidder's Hadoop Package

Tools and helpers for writing and running Hadoop jobs in b1

## Writing jobs

### Job initialization

job.InitRawJob method creates command line flags for triggering either mapper or reducer functions.

    example --stage=mapper
    example --stage=reducer

### Logging

job.Log is an instance of go's logger struct which logs each line with a prefix to stderr so we can extract our log lines in the runner.

    job.Log.Print("My log line")

### Counters

job.Count outputs a counter line to stderr to a predefined counter group so we can fetch them in the runner.

    job.Count("myCounter", 1)

### Example:

    package main

    import (
        "b1/common/hadoop/job"
        "bufio"
        "fmt"
        "io"
        "os"
    )

    func main() {
        job.InitRawJob(runMapper, runReducer)
    }

    func runMapper() {
        job.Log.Print("Mapper run")

        in := bufio.NewReader(os.Stdin)
        for {
            line, err := in.ReadString('\n')
            if err == io.EOF {
                break
            } else if err != nil {
                job.Log.Fatal(err)
            }

            job.Count("mapper_line", 1)
            fmt.Println("key\tvalue")
        }
    }

    func runReducer() {
        job.Log.Print("Reducer run")

        in := bufio.NewReader(os.Stdin)
        for {
            _, err := in.ReadString('\n')
            if err == io.EOF {
                break
            } else if err != nil {
                job.Log.Fatal(err)
            }
            job.Count("reducer_line", 1)
        }
    }

## Running Jobs

### Creating new Hadoop command

    cmd := runner.NewMapReduce(retries, args...)

### Running commands

    // Sync
    status = cmd.Run()

    // Async
    go cmd.Run()
    status = cmd.Wait()

Each command can be run only once.

### Fetching status, logs and counters

    // All the data
    mrd, err := cmd.FetchDebugData()

    // Hadoop command output
	stdOut, stdErr, cmdErr = cmd.CmdOutput()

    // Mapper and reducer logs (can only be called once the job is completed)
	logs, err = cmd.FetchApplicationLogs()

    // Counters
	counters, err = cmd.FetchJobCounters()

### Example:

	var args []string
	args = append(args, "hadoop-streaming")
	args = append(args, mapredConfig...)
	args = append(args,
		"-D", fmt.Sprintf("mapred.job.name=join-%s", timePath),
		"-D", "mapred.reduce.tasks=1",
		"-files", joinerPath,
		"-output", joinedTmpDest,
		"-mapper", "joiner --stage=mapper",
		"-reducer", "joiner --stage=reducer",
	)

	for _, et := range eventLogTypesJoin {
		inputFile := fmt.Sprintf(fileCombinedDest, et, timePath)
		args = append(args, "--input", inputFile)
	}

	cmd := runner.NewMapReduce(3, args...)
	if cmd.Run() != runner.HadoopStatusSuccess {
		mrd, derr := cmd.FetchDebugData()

		app.Log.WithFields(logrus.Fields{
			"err":         mrd.CmdErr,
			"stderr":      mrd.StdErr,
			"stdout":      mrd.StdOut,
			"logErr":      derr,
			"applogs":     mrd.Logs.AppLog(),
			"appcounters": mrd.Counters.AppCounters(),
		}).Warn("EventlogJoiner join failed")

		return
	}

## Executing non-mapreduce commands

	err := runner.ExecOnCluster(retries, "aws", "s3", "ls", "/path")

## FAQ

#### Where should I put MR executables?

Production jobs should be in "s3://b1-eventlog-sync/jobs/"
Non production jobs can be wherever except "s3://b1-eventlog-sync/jobs/"

#### How does this thing know where to execute commands?

All commands fetch EMR cluster configuration from AWS API and run jobs on the newest running cluster named "eventlog-processor".

#### What does Borat think about all this?

In past I having bigdata problem. Since deploy Hadoop now have bigdata problem and BigDataProblemMapTaskImpl.
