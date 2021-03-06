package main

import (
	"fmt"

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
	cmd := runner.NewRawMapReduce("hadoop-streaming",
		"-D", "mapred.job.name=hamax-text",
		"-D", "mapred.reduce.tasks=1",
		"-D", "mapred.map.tasks=1",
		"-D", "mapreduce.job.queuename=realtime",
		//"-D", "mapreduce.job.ubertask.enable=true",
		"-files", "s3://b1-eventlog-sync/tmp/"+bin,
		"-input", "s3://b1-eventlog-sync/tmp/monkeys.txt",
		"-output", "s3://b1-eventlog-sync/tmp/hamax-test1",
		"-mapper", bin+" --stage=mapper",
		"-reducer", bin+" --stage=reducer",
	)

	cmd.Run()

	//cmd.FetchApplicationStatus()
	//cmd.FetchJobCounters()
	//cmd.FetchApplicationLogs()

	_, err := cmd.FetchDebugData()
	fmt.Println(err)
}
