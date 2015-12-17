package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"golang.org/x/crypto/ssh"

	"b1/app"

	"github.com/Zemanta/mrgob/runner"
	"github.com/Zemanta/mrgob/runner/provider"
)

func main() {
	app.ConfigureTest()

	awsConfig := &aws.Config{
		Region: &app.Env.AWS_REGION,

		Credentials: credentials.NewStaticCredentials(
			app.Env.AWS_ACCESS_KEY_ID,
			app.Env.AWS_SECRET_ACCESS_KEY,
			"",
		),
	}

	key, _ := ssh.ParsePrivateKey([]byte(app.Env.AWS_SSH_KEY))

	sshConfig := &ssh.ClientConfig{
		User: "hadoop",
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(key),
		},
	}

	runner.SetDefaultHadoopProvider(provider.NewEmrProvider("eventlog-processor", sshConfig, awsConfig))

	bin := "wordcount_json"
	cmd, err := runner.NewMapReduce(&runner.MapReduceConfig{
		Name: "hamax-text",

		JobPath: "s3://b1-eventlog-sync/tmp/" + bin,

		JobConfig: map[string]string{"test": "123"},

		ReduceTasks: 1,
		MapTasks:    1,

		Input:  []string{"s3://b1-eventlog-sync/tmp/monkeys.txt"},
		Output: "s3://b1-eventlog-sync/tmp/hamax-test1",

		CustomProperties: map[string]string{
			"mapreduce.job.queuename": "realtime",
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	cmd.Run()

	//cmd.FetchApplicationStatus()
	//cmd.FetchJobCounters()
	//cmd.FetchApplicationLogs()

	_, err = cmd.FetchDebugData()
	if err != nil {
		log.Fatal(err)
	}
}
