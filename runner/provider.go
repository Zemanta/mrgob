package runner

import "golang.org/x/crypto/ssh"

var hadoopProvider HadoopProvider

type HadoopProvider interface {
	GetMasterHost() (string, error)
	GetMasterSSHClient() (*ssh.Client, error)
	GetNextSSHClient() (*ssh.Client, error)
}

func SetDefaultHadoopProvider(p HadoopProvider) {
	hadoopProvider = p
}
