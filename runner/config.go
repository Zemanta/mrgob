package runner

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
)

var (
	ErrMissingJobPath = fmt.Errorf("Missing job path")
	ErrMissingInput   = fmt.Errorf("Missing input")
	ErrMissingOutput  = fmt.Errorf("Missing output")
)

type MapReduceConfig struct {
	// Job name.
	Name string

	// Number of reducers.
	ReduceTasks int
	// Number of mappers.
	MapTasks int

	// S3 or HDFS path to the executable job implementing "Init*Job" interface.
	JobPath string

	// Job configuration that will be made available in mapper and reducer jobs.
	JobConfig interface{}

	// List of input files.
	Input []string
	// Output directory.
	Output string

	// Other custom -D properties passes to the job.
	CustomProperties map[string]string
	// Other files that will be downloaded next to the executable before running the job.
	AdditionalFiles []string
	// Environment options passed to the mapreduce jobs.
	Env map[string]string
}

func (c *MapReduceConfig) getFileArg(fn string) []string {
	return []string{
		"-files", fn,
	}
}

func (c *MapReduceConfig) getArg(k, v string) []string {
	return []string{
		k, v,
	}
}

func (c *MapReduceConfig) getEnvArg(k string, v interface{}) []string {
	return []string{"-cmdenv", fmt.Sprintf("%s=%s", k, v)}
}

func (c *MapReduceConfig) getProperyArg(k string, v interface{}) []string {
	switch v.(type) {
	case string:
		return []string{
			"-D", fmt.Sprintf("%s=%s", k, v),
		}
	case int:
		return []string{
			"-D", fmt.Sprintf("%s=%d", k, v),
		}
	case bool:
		return []string{
			"-D", fmt.Sprintf("%s=%t", k, v),
		}
	}
	return nil
}

func (c *MapReduceConfig) getConfigProperty() ([]string, error) {
	b, err := json.Marshal(c.JobConfig)
	if err != nil {
		return nil, err
	}

	str := string(b)
	str = strconv.Quote(str)

	args := []string{
		"-cmdenv", fmt.Sprintf("mrgob_config=\"%s\"", str),
	}

	return args, nil
}

func (c *MapReduceConfig) getArgs() ([]string, error) {
	// TODO config

	if c.JobPath == "" {
		return nil, ErrMissingJobPath
	}
	if len(c.Input) == 0 {
		return nil, ErrMissingInput
	}
	if c.Output == "" {
		return nil, ErrMissingOutput
	}

	args := []string{"hadoop-streaming"}

	if c.Name != "" {
		args = append(args, c.getProperyArg("mapreduce.job.name", c.Name)...)
	}

	args = append(args, c.getProperyArg("mapreduce.job.reduces", c.ReduceTasks)...)

	if c.MapTasks > 0 {
		args = append(args, c.getProperyArg("mapreduce.job.maps", c.MapTasks)...)
	}

	for k, v := range c.CustomProperties {
		args = append(args, c.getProperyArg(k, v)...)
	}

	args = append(args, c.getFileArg(c.JobPath)...)

	for _, fn := range c.AdditionalFiles {
		args = append(args, c.getFileArg(fn)...)
	}

	execFile := path.Base(c.JobPath)
	args = append(args, c.getArg("-mapper", fmt.Sprintf("%s -stage=mapper", execFile))...)
	args = append(args, c.getArg("-reducer", fmt.Sprintf("%s -stage=reducer", execFile))...)

	for _, f := range c.Input {
		args = append(args, c.getArg("-input", f)...)
	}

	args = append(args, c.getArg("-output", c.Output)...)

	for k, v := range c.Env {
		args = append(args, c.getEnvArg(k, v)...)
	}

	if c.JobConfig != nil {
		a, err := c.getConfigProperty()
		if err != nil {
			return nil, err
		}
		args = append(args, a...)
	}

	return args, nil
}
