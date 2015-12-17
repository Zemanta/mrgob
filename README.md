# The Go MapReduce library

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

job.Log is an instance of go's logger struct which logs each line with a prefix to stderr so the runner can extract them.

    job.Log.Print("My log line")

### Counters

job.Count writes a counter line to stderr with the predefined counter group so the runner can fetch them.

    job.Count("myCounter", 1)

### Job Config

job.Config retrieves and decodes the job config passed from the runner.

    func Config(target interface{}) error

### Testing jobs

For testing mappers and reducers use tester.Test\*Job functions which simulate mapreduce by streming input into mapper, sorting mapper's output, streaming it to the reducer and writing reducer's output to the defined output writer.

    func TestRawJob(input io.Reader, output io.Writer, mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader))

    func TestByteJob(input io.Reader, output io.Writer, mapper func(*ByteKVWriter, io.Reader), reducer func(io.Writer, *ByteKVReader))

    func TestJsonJob(input io.Reader, output io.Writer, mapper func(*JsonKVWriter, io.Reader), reducer func(io.Writer, *JsonKVReader))


### Examples:

- [Raw wordcount job](https://github.com/Zemanta/mrgob/blob/master/_examples/wordcount_raw/wordcount.go)
- [Byte mr wordcount job](https://github.com/Zemanta/mrgob/blob/master/_examples/wordcount_byte/wordcount.go)
- [Json mr wordcount job](https://github.com/Zemanta/mrgob/blob/master/_examples/wordcount_json/wordcount.go)


[API reference the for job package](https://github.com/Zemanta/mrgob/blob/master/job/README.md)

## Running Jobs

## Configuring Hadoop provider

MrGob requirest both api (for fetching job status) and ssh (for executing commands) access to the cluster. At the moment the only supported provider is AWS EMR.

	awsConfig := &aws.Config{
		Region: &app.Env.AWS_REGION,
	}

	sshConfig := &ssh.ClientConfig{
		User: "hadoop",
		Auth: []ssh.AuthMethod{ssh.PublicKeys(sshKey)},
	}

	runner.SetDefaultHadoopProvider(runner.NewEmrProvider("eventlog-processor", sshConfig, awsConfig))

### Creating new Hadoop command

Passing command line arguments directly

	cmd := runner.NewRawMapReduce(args...)


Using MapReduceConfig

	cmd, err := runner.NewMapReduce(&runner.MapReduceConfig{
		Name: "job-name",

		JobPath: "s3://bucket/jobFile",

		ReduceTasks: 1,
		MapTasks:    1,

		Input:  []string{"s3://bucket/files/"},
		Output: "s3://bucker/output/",

		CustomProperties: map[string]string{
			"mapreduce.job.queuename": "myqueue",
		},
	})


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

- [Raw job runner](https://github.com/Zemanta/mrgob/blob/master/_examples/run_raw/run.go)
- [Config job runner](https://github.com/Zemanta/mrgob/blob/master/_examples/run/run.go)

## Executing non-mapreduce commands

	err := runner.ExecOnCluster(retries, "aws", "s3", "ls", "/path")

[API reference for the runner package](https://github.com/Zemanta/mrgob/blob/master/runner/README.md)


## Right tool for the right gob

![Right tool for the right gob](https://media.giphy.com/media/iJWULINtShOnK/giphy.gif)
