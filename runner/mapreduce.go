package runner

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

type HadoopStatus int

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

var (
	hadoopApiPort     = 8088
	historyServerPort = 19888
	statusApiUrl      = "http://%s:%d/ws/v1/cluster/apps/%s"
	counterApiUrl     = "http://%s:%d/ws/v1/history/mapreduce/jobs/%s/counters"
	yarnLogsCommand   = "yarn logs -applicationId %s"

	waitForLogs = time.Duration(2) * time.Second
)

func debugLog(s string, a ...interface{}) {
	log.Printf(s, a...)
}

type HadoopCommand struct {
	args    []string
	retries int

	err       error
	tries     []*HadoopRun
	done      sync.Mutex
	started   bool
	startedMu sync.Mutex

	status HadoopStatus
}

func NewMapReduce(retries int, arguments ...string) *HadoopCommand {
	hd := &HadoopCommand{
		args:    arguments,
		retries: retries,
	}
	hd.done.Lock()
	return hd
}

func (hc *HadoopCommand) Run() HadoopStatus {
	if hadoopProvider == nil {
		hc.err = ErrMissingHadoopProvider
		return hc.status
	}

	hc.startedMu.Lock()
	if hc.started {
		hc.startedMu.Unlock()
		return hc.status
	}
	hc.started = true
	hc.status = HadoopStatusRunning
	hc.startedMu.Unlock()

	defer hc.done.Unlock()

	for i := 0; i < hc.retries+1 && hc.status == HadoopStatusRunning; i++ {
		hr := &HadoopRun{}

		hc.tries = append(hc.tries, hr)

		if ok := hr.exec(hc.args); ok {
			hc.status = HadoopStatusSuccess
		}
	}

	if hc.status != HadoopStatusSuccess {
		hc.status = HadoopStatusFailed
	}

	return hc.status
}

func (hc *HadoopCommand) Wait() HadoopStatus {
	hc.done.Lock()
	defer hc.done.Unlock()
	return hc.status
}

func (hc *HadoopCommand) Status() HadoopStatus {
	return hc.status
}

func (hc *HadoopCommand) FetchApplicationLogs() (*HadoopApplicationLogs, error) {
	if len(hc.tries) == 0 {
		return nil, ErrNotRunning
	}

	return hc.tries[len(hc.tries)-1].FetchApplicationLogs()
}

func (hc *HadoopCommand) FetchApplicationStatus() (*HadoopApplicationStatus, error) {
	if len(hc.tries) == 0 {
		return nil, ErrNotRunning
	}

	return hc.tries[len(hc.tries)-1].FetchApplicationStatus()
}

func (hc *HadoopCommand) FetchJobCounters() (HadoopJobCounters, error) {
	if len(hc.tries) == 0 {
		return nil, ErrNotRunning
	}

	return hc.tries[len(hc.tries)-1].FetchJobCounters()
}

func (hc *HadoopCommand) ApplicationId() (string, error) {
	if len(hc.tries) == 0 {
		return "", ErrNotRunning
	}

	return hc.tries[len(hc.tries)-1].ApplicationId()
}

func (hc *HadoopCommand) Tries() []*HadoopRun {
	return hc.tries
}

func (hc *HadoopCommand) CmdOutput() (stdOut string, stdErr string, cmdErr error) {
	if hc.err != nil {
		return "", "", hc.err
	}

	if len(hc.tries) == 0 {
		return "", "", ErrNotRunning
	}

	return hc.tries[len(hc.tries)-1].CmdOutput()
}

func (hc *HadoopCommand) FetchDebugData() (*HadoopDebugData, error) {
	if len(hc.tries) == 0 {
		return nil, hc.err
	}

	return hc.tries[len(hc.tries)-1].FetchDebugData()
}

type HadoopRun struct {
	applicationId string

	err    error
	stdErr []string
	stdOut []string

	done time.Time
}

func (hr *HadoopRun) runCommand(session *ssh.Session, command string) error {
	debugLog("Running command: `%s`", command)

	applicationPrefix := "Submitted application "

	go func() {
		pipe, err := session.StderrPipe()
		if err != nil {
			return
		}

		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			line := scanner.Text()
			debugLog(line)

			// find application id
			if idxStart := strings.Index(line, applicationPrefix); idxStart >= 0 {
				ss := line[idxStart+len(applicationPrefix):]
				if idxEnd := strings.Index(ss, " "); idxEnd >= 0 {
					ss = ss[:idxEnd]
				}

				hr.applicationId = strings.TrimSpace(ss)
			}

			hr.stdErr = append(hr.stdErr, line)
		}
	}()

	go func() {
		pipe, err := session.StdoutPipe()
		if err != nil {
			return
		}

		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			line := scanner.Text()
			debugLog(line)

			hr.stdOut = append(hr.stdOut, line)
		}
	}()

	return session.Run(command)
}

func (hr *HadoopRun) ApplicationId() (string, error) {
	if hr.applicationId != "" {
		return hr.applicationId, nil
	}
	return "", ErrMissingApplicationId
}

func (hr *HadoopRun) FetchApplicationLogs() (*HadoopApplicationLogs, error) {
	if hr.applicationId == "" {
		return nil, ErrMissingApplicationId
	}

	if hr.done.IsZero() {
		return nil, ErrRunning
	} else if hr.done.Add(waitForLogs).After(time.Now()) {
		time.Sleep(time.Now().Sub(hr.done))
	}

	debugLog("Fetching map reduce application logs")

	client, err := hadoopProvider.GetMasterSSHClient()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var runErr error
	var log []byte
	var session *ssh.Session

	for i := 0; i < 10; i++ {
		session, runErr = client.NewSession()
		if err != nil {
			time.Sleep(waitForLogs)
			continue
		}
		defer session.Close()

		command := fmt.Sprintf(yarnLogsCommand, hr.applicationId)

		log, runErr = session.Output(command)
		if runErr != nil {
			debugLog("Logs not ready yet")
			time.Sleep(waitForLogs)
			continue
		}
		break
	}

	if runErr != nil {
		return nil, runErr
	}

	return newHadoopApplicationLogs(string(log))
}

func (hr *HadoopRun) FetchApplicationStatus() (*HadoopApplicationStatus, error) {
	if hr.applicationId == "" {
		return nil, ErrMissingApplicationId
	}

	debugLog("Fetching map reduce application status")

	hadoopMaster, err := hadoopProvider.GetMasterHost()
	if err != nil {
		return nil, err
	}

	resp, err := http.Get(fmt.Sprintf(statusApiUrl, hadoopMaster, hadoopApiPort, hr.applicationId))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	as := &HadoopApplicationStatus{}
	err = json.NewDecoder(resp.Body).Decode(as)
	if err != nil {
		return nil, err
	}

	debugLog("%s: [state: %s, final status: %s, progress: %f, queue: %s, elapsed time: %d]",
		as.App.Name, as.App.State, as.App.FinalStatus, as.App.Progress, as.App.Queue, as.App.ElapsedTime,
	)

	return as, nil
}

func (hr *HadoopRun) FetchJobCounters() (HadoopJobCounters, error) {
	if hr.applicationId == "" {
		return nil, ErrMissingApplicationId
	}

	debugLog("Fetching map reduce application counters")

	hadoopMaster, err := hadoopProvider.GetMasterHost()
	if err != nil {
		return nil, err
	}

	jobId := strings.Replace(hr.applicationId, "application", "job", 1)

	resp, err := http.Get(fmt.Sprintf(counterApiUrl, hadoopMaster, historyServerPort, jobId))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	jc := &hadoopJobCountersRaw{}
	err = json.NewDecoder(resp.Body).Decode(jc)
	if err != nil {
		return nil, err
	}

	counters := HadoopJobCounters{}

	for _, group := range jc.JobCounters.CounterGroup {
		gr := map[string]HadoopJobCounterData{}
		for _, c := range group.Counter {
			gr[c.Name] = c
		}
		counters[group.CounterGroupName] = gr
	}

	debugLog(counters.AppCounters().String())

	return counters, nil
}

func (hr *HadoopRun) exec(arguments []string) bool {
	defer func() { hr.done = time.Now() }()

	client, err := hadoopProvider.GetNextSSHClient()
	if err != nil {
		hr.err = err
		return false
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		hr.err = err
		return false
	}
	defer session.Close()

	command := "\"" + strings.Join(arguments, `" "`) + "\""
	err = hr.runCommand(session, command)
	if err != nil {
		hr.err = err
		return false
	}

	return true
}

func (hr *HadoopRun) CmdOutput() (stdOut string, stdErr string, cmdErr error) {
	return strings.Join(hr.stdOut, "\n"), strings.Join(hr.stdErr, "\n"), hr.err
}

func (hr *HadoopRun) FetchDebugData() (*HadoopDebugData, error) {
	d := &HadoopDebugData{}
	var err error

	d.StdOut, d.StdErr, d.CmdErr = hr.CmdOutput()

	d.Logs, err = hr.FetchApplicationLogs()

	var cerr error
	d.Counters, cerr = hr.FetchJobCounters()
	if cerr != nil {
		err = cerr
	}

	var serr error
	d.Status, cerr = hr.FetchApplicationStatus()
	if serr != nil {
		err = serr
	}

	return d, err
}
