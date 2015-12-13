package runner

import (
	"strings"

	"golang.org/x/crypto/ssh"
)

func ExecOnCluster(retries int, arguments ...string) error {
	var err error

	success := false

	for i := 0; i < retries+1 && !success; i++ {
		var session *ssh.Session
		var client *ssh.Client

		client, err = hadoopProvider.GetNextSSHClient()
		if err != nil {
			continue
		}
		defer client.Close()

		session, err = client.NewSession()
		if err != nil {
			continue
		}
		defer session.Close()

		err = session.Run(`"` + strings.Join(arguments, `" "`) + `"`)
		if err != nil {
			continue
		}
		success = true
	}
	return err
}
