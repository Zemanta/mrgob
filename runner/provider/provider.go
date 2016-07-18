package provider

import "golang.org/x/crypto/ssh"

type HadoopProvider interface {
	GetMasterHost() (string, error)
	GetMasterSSHClient() (*ssh.Client, error)
}
