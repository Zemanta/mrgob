# provider
--
    import "github.com/Zemanta/mrgob/runner/provider"


## Usage

```go
var (
	ErrClusterNotFound  = fmt.Errorf("EMR cluster not found")
	ErrInstanceNotFound = fmt.Errorf("EMR instance not found")
)
```

#### type EmrProvider

```go
type EmrProvider struct {
}
```


#### func  NewEmrProvider

```go
func NewEmrProvider(clusterName string, sshConfig *ssh.ClientConfig, awsConfig *aws.Config) *EmrProvider
```

#### func (*EmrProvider) GetMasterHost

```go
func (e *EmrProvider) GetMasterHost() (master string, err error)
```

#### func (*EmrProvider) GetMasterSSHClient

```go
func (e *EmrProvider) GetMasterSSHClient() (*ssh.Client, error)
```

#### func (*EmrProvider) GetNextSSHClient

```go
func (e *EmrProvider) GetNextSSHClient() (*ssh.Client, error)
```

#### type HadoopProvider

```go
type HadoopProvider interface {
	GetMasterHost() (string, error)
	GetMasterSSHClient() (*ssh.Client, error)
	GetNextSSHClient() (*ssh.Client, error)
}
```
