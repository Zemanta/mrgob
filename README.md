# Go MapReduce

Tools and helpers for writing and running MapReduce jobs on Hadoop and EMR.

## Writing jobs

### Job initialization

job.Init\*Job methods create command line flags for triggering either mapper or reducer functions.

    example --stage=mapper
    example --stage=reducer

Supported job types

    func InitRawJob(mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader))

    func InitByteJob(mapper func(*ByteKVWriter, io.Reader), reducer func(io.Writer, *ByteKVReader))

    func InitJsonJob(mapper func(*JsonKVWriter, io.Reader), reducer func(io.Writer, *JsonKVReader))

### Logging

job.Log is an instance of go's logger struct which logs each line with a prefix to stderr so we can extract our log lines in the runner.

    job.Log.Print("My log line")

### Counters

job.Count outputs a counter line to stderr to a predefined counter group so we can fetch them in the runner.

    job.Count("myCounter", 1)

### Testing jobs

For testing mappers and reducers use tester.Test\*Job function which simulate Hadoop mapreduce by streming input into mapper, sorting mapper's output, streaming it to reducer and writing reducer's output to output writer.

    func TestRawJob(input io.Reader, output io.Writer, mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader))

    func TestByteJob(input io.Reader, output io.Writer, mapper func(*ByteKVWriter, io.Reader), reducer func(io.Writer, *ByteKVReader))

    func TestJsonJob(input io.Reader, output io.Writer, mapper func(*JsonKVWriter, io.Reader), reducer func(io.Writer, *JsonKVReader))


### Examples:

- [Raw wordcount job](https://github.com/Zemanta/gomr/blob/master/_examples/wordcount_raw/wordcount.go)
- [Byte mr wordcount job](https://github.com/Zemanta/gomr/blob/master/_examples/wordcount_byte/wordcount.go)
- [Json mr wordcount job](https://github.com/Zemanta/gomr/blob/master/_examples/wordcount_json/wordcount.go)


[API reference for job package](https://github.com/Zemanta/gomr/blob/master/job/README.md)

## Running Jobs

## Configuring Hadoop provider

GOMR needs a way to connect to your Hadoop cluster via api and ssh.
At the moment it only supports EMR. It will query aws for EMR info and select the newest running cluster matching the name.

	awsConfig := &aws.Config{
		Region: &app.Env.AWS_REGION,
	}

	sshConfig := &ssh.ClientConfig{
		User: "hadoop",
		Auth: []ssh.AuthMethod{ssh.PublicKeys(sshKey)},
	}

	runner.SetDefaultHadoopProvider(runner.NewEmrProvider("eventlog-processor", sshConfig, awsConfig))

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

- [Raw job runner](https://github.com/Zemanta/gomr/blob/master/_examples/run_raw/run.go)

## Executing non-mapreduce commands

	err := runner.ExecOnCluster(retries, "aws", "s3", "ls", "/path")

[API reference for runner package](https://github.com/Zemanta/gomr/blob/master/runner/README.md)

## General life advice

In past I having bigdata problem. Since deploy Hadoop now have bigdata problem and BigDataProblemMapTaskImpl.
