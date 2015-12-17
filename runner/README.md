# runner
--
    import "github.com/Zemanta/mrgob/runner"


## Usage

```go
var (
	HadoopStatusIdle    HadoopStatus = 0
	HadoopStatusRunning HadoopStatus = 1
	HadoopStatusSuccess HadoopStatus = 2
	HadoopStatusFailed  HadoopStatus = -1

	ErrNotRunning            = fmt.Errorf("Command not running")
	ErrRunning               = fmt.Errorf("Application running")
	ErrStarted               = fmt.Errorf("Application can only be run once")
	ErrMissingApplicationId  = fmt.Errorf("Missing application id")
	ErrMissingHadoopProvider = fmt.Errorf("Missing Hadoop provider")
)
```

#### func  ExecOnCluster

```go
func ExecOnCluster(retries int, arguments ...string) error
```

#### func  SetDefaultHadoopProvider

```go
func SetDefaultHadoopProvider(p provider.HadoopProvider)
```

#### type HadoopApplicationLogs

```go
type HadoopApplicationLogs struct {
	Raw string

	ContainerLogs []*HadoopContainerLogs
}
```


#### func (*HadoopApplicationLogs) AppLog

```go
func (l *HadoopApplicationLogs) AppLog() string
```

#### func (*HadoopApplicationLogs) StdErr

```go
func (l *HadoopApplicationLogs) StdErr() string
```

#### func (*HadoopApplicationLogs) StdOut

```go
func (l *HadoopApplicationLogs) StdOut() string
```

#### func (*HadoopApplicationLogs) String

```go
func (l *HadoopApplicationLogs) String() string
```

#### func (*HadoopApplicationLogs) SysLog

```go
func (l *HadoopApplicationLogs) SysLog() string
```

#### type HadoopApplicationStatus

```go
type HadoopApplicationStatus struct {
	App struct {
		AllocatedMB                int     `json:"allocatedMB"`
		AllocatedVCores            int     `json:"allocatedVCores"`
		AmContainerLogs            string  `json:"amContainerLogs"`
		AmHostHTTPAddress          string  `json:"amHostHttpAddress"`
		ApplicationTags            string  `json:"applicationTags"`
		ApplicationType            string  `json:"applicationType"`
		ClusterID                  int     `json:"clusterId"`
		Diagnostics                string  `json:"diagnostics"`
		ElapsedTime                int     `json:"elapsedTime"`
		FinalStatus                string  `json:"finalStatus"`
		FinishedTime               int     `json:"finishedTime"`
		ID                         string  `json:"id"`
		MemorySeconds              int     `json:"memorySeconds"`
		Name                       string  `json:"name"`
		NumAMContainerPreempted    int     `json:"numAMContainerPreempted"`
		NumNonAMContainerPreempted int     `json:"numNonAMContainerPreempted"`
		PreemptedResourceMB        int     `json:"preemptedResourceMB"`
		PreemptedResourceVCores    int     `json:"preemptedResourceVCores"`
		Progress                   float64 `json:"progress"`
		Queue                      string  `json:"queue"`
		RunningContainers          int     `json:"runningContainers"`
		StartedTime                int     `json:"startedTime"`
		State                      string  `json:"state"`
		TrackingUI                 string  `json:"trackingUI"`
		TrackingURL                string  `json:"trackingUrl"`
		User                       string  `json:"user"`
		VcoreSeconds               int     `json:"vcoreSeconds"`
	} `json:"app"`
}
```


#### type HadoopCommand

```go
type HadoopCommand struct {
}
```


#### func  NewMapReduce

```go
func NewMapReduce(arguments ...string) *HadoopCommand
```

#### func (*HadoopCommand) ApplicationId

```go
func (hc *HadoopCommand) ApplicationId() (string, error)
```

#### func (*HadoopCommand) CmdOutput

```go
func (hc *HadoopCommand) CmdOutput() (stdOut string, stdErr string, cmdErr error)
```

#### func (*HadoopCommand) FetchApplicationLogs

```go
func (hc *HadoopCommand) FetchApplicationLogs() (*HadoopApplicationLogs, error)
```

#### func (*HadoopCommand) FetchApplicationStatus

```go
func (hc *HadoopCommand) FetchApplicationStatus() (*HadoopApplicationStatus, error)
```

#### func (*HadoopCommand) FetchDebugData

```go
func (hc *HadoopCommand) FetchDebugData() (*HadoopDebugData, error)
```

#### func (*HadoopCommand) FetchJobCounters

```go
func (hc *HadoopCommand) FetchJobCounters() (HadoopJobCounters, error)
```

#### func (*HadoopCommand) Run

```go
func (hc *HadoopCommand) Run() HadoopStatus
```

#### func (*HadoopCommand) SetRetries

```go
func (hc *HadoopCommand) SetRetries(n int)
```

#### func (*HadoopCommand) Status

```go
func (hc *HadoopCommand) Status() HadoopStatus
```

#### func (*HadoopCommand) Tries

```go
func (hc *HadoopCommand) Tries() []*HadoopRun
```

#### func (*HadoopCommand) Wait

```go
func (hc *HadoopCommand) Wait() HadoopStatus
```

#### type HadoopContainerLogs

```go
type HadoopContainerLogs struct {
	Container string
	Host      string

	StdOut string
	StdErr string
	SysLog string

	AppLog string
}
```


#### type HadoopDebugData

```go
type HadoopDebugData struct {
	Logs     *HadoopApplicationLogs
	Counters HadoopJobCounters
	Status   *HadoopApplicationStatus

	StdOut string
	StdErr string
	CmdErr error
}
```


#### type HadoopJobCounterData

```go
type HadoopJobCounterData struct {
	Name               string `json:"name"`
	MapCounterValue    int    `json:"mapCounterValue"`
	ReduceCounterValue int    `json:"reduceCounterValue"`
	TotalCounterValue  int    `json:"totalCounterValue"`
}
```


#### type HadoopJobCounters

```go
type HadoopJobCounters map[string]HadoopJobCountersGroup
```


#### func (HadoopJobCounters) AppCounters

```go
func (c HadoopJobCounters) AppCounters() HadoopJobCountersGroup
```

#### type HadoopJobCountersGroup

```go
type HadoopJobCountersGroup map[string]HadoopJobCounterData
```


#### func (HadoopJobCountersGroup) String

```go
func (c HadoopJobCountersGroup) String() string
```

#### type HadoopRun

```go
type HadoopRun struct {
}
```


#### func (*HadoopRun) ApplicationId

```go
func (hr *HadoopRun) ApplicationId() (string, error)
```

#### func (*HadoopRun) CmdOutput

```go
func (hr *HadoopRun) CmdOutput() (stdOut string, stdErr string, cmdErr error)
```

#### func (*HadoopRun) FetchApplicationLogs

```go
func (hr *HadoopRun) FetchApplicationLogs() (*HadoopApplicationLogs, error)
```

#### func (*HadoopRun) FetchApplicationStatus

```go
func (hr *HadoopRun) FetchApplicationStatus() (*HadoopApplicationStatus, error)
```

#### func (*HadoopRun) FetchDebugData

```go
func (hr *HadoopRun) FetchDebugData() (*HadoopDebugData, error)
```

#### func (*HadoopRun) FetchJobCounters

```go
func (hr *HadoopRun) FetchJobCounters() (HadoopJobCounters, error)
```

#### type HadoopStatus

```go
type HadoopStatus int
```
