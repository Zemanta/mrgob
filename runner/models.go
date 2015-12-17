package runner

import (
	"fmt"
	"strings"

	"github.com/Zemanta/mrgob/job"
)

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

type hadoopJobCountersRaw struct {
	JobCounters struct {
		CounterGroup []hadoopJobCounterGroup `json:"counterGroup"`
		ID           string                  `json:"id"`
	} `json:"jobCounters"`
}

type hadoopJobCounterGroup struct {
	Counter          []HadoopJobCounterData `json:"counter"`
	CounterGroupName string                 `json:"counterGroupName"`
}

type HadoopJobCounterData struct {
	Name               string `json:"name"`
	MapCounterValue    int    `json:"mapCounterValue"`
	ReduceCounterValue int    `json:"reduceCounterValue"`
	TotalCounterValue  int    `json:"totalCounterValue"`
}

type HadoopJobCounters map[string]HadoopJobCountersGroup

func (c HadoopJobCounters) AppCounters() HadoopJobCountersGroup {
	return c[job.AppCounterGroup]
}

type HadoopJobCountersGroup map[string]HadoopJobCounterData

func (c HadoopJobCountersGroup) String() string {
	o := []string{}
	for k, v := range c {
		o = append(o, fmt.Sprintf("%s: %d", k, v.TotalCounterValue))
	}
	return "[" + strings.Join(o, ", ") + "]"
}

type HadoopDebugData struct {
	Logs     *HadoopApplicationLogs
	Counters HadoopJobCounters
	Status   *HadoopApplicationStatus

	StdOut string
	StdErr string
	CmdErr error
}
