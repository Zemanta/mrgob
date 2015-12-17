package runner

import (
	"fmt"
	"strings"

	"github.com/Zemanta/mrgob/job"
)

type HadoopContainerLogs struct {
	Container string
	Host      string

	StdOut string
	StdErr string
	SysLog string

	AppLog string
}

type HadoopApplicationLogs struct {
	Raw string

	ContainerLogs []*HadoopContainerLogs
}

func newHadoopApplicationLogs(r string) (*HadoopApplicationLogs, error) {
	l := &HadoopApplicationLogs{Raw: r}
	return l, l.parse()
}
func (l *HadoopApplicationLogs) String() string {
	if l == nil {
		return "<nil>"
	}

	out := []string{}
	for _, c := range l.ContainerLogs {
		out = append(out,
			fmt.Sprintf("Container: %s on %s", c.Container, c.Host),
			"---------------------------------",
			"stdout:",
			c.StdOut,
			"stderr:",
			c.StdErr,
			"syslog:",
			c.SysLog,
		)
	}

	return strings.Join(out, "\n")
}

func (l *HadoopApplicationLogs) StdErr() string {
	if l == nil {
		return "<nil>"
	}

	out := []string{}
	for _, c := range l.ContainerLogs {
		if c.StdErr != "" {
			out = append(out,
				fmt.Sprintf("Container: %s on %s", c.Container, c.Host),
				c.StdErr,
			)
		}
	}
	return strings.Join(out, "\n")
}

func (l *HadoopApplicationLogs) StdOut() string {
	if l == nil {
		return "<nil>"
	}

	out := []string{}
	for _, c := range l.ContainerLogs {
		if c.StdOut != "" {
			out = append(out,
				fmt.Sprintf("Container: %s on %s", c.Container, c.Host),
				c.StdOut,
			)
		}
	}
	return strings.Join(out, "\n")
}

func (l *HadoopApplicationLogs) SysLog() string {
	if l == nil {
		return "<nil>"
	}

	out := []string{}
	for _, c := range l.ContainerLogs {
		if c.SysLog != "" {
			out = append(out,
				fmt.Sprintf("Container: %s on %s", c.Container, c.Host),
				c.SysLog,
			)
		}
	}
	return strings.Join(out, "\n")
}

func (l *HadoopApplicationLogs) AppLog() string {
	if l == nil {
		return "<nil>"
	}

	out := []string{}
	for _, c := range l.ContainerLogs {
		if c.AppLog != "" {
			out = append(out,
				fmt.Sprintf("Container: %s on %s", c.Container, c.Host),
				c.AppLog,
			)
		}
	}
	return strings.Join(out, "\n")
}

func (l *HadoopApplicationLogs) parse() error {
	var container *HadoopContainerLogs
	logType := ""
	contents := false

	for _, line := range strings.Split(l.Raw, "\n") {
		if !contents && line == "" {
			continue
		}
		if strings.HasPrefix(line, "Container: ") {
			logType = ""
			contents = false

			if container != nil {
				l.ContainerLogs = append(l.ContainerLogs, container)
			}
			parts := strings.Split(line, " ")
			if len(parts) < 4 {
				return fmt.Errorf("Invalid new container line: \"%s\"", line)
			}
			container = &HadoopContainerLogs{
				Container: parts[1],
				Host:      parts[3],
			}
			debugLog("Container: %s on %s", container.Container, container.Host)
		} else if strings.HasPrefix(line, "LogType:") {
			contents = false
			logType = strings.TrimPrefix(line, "LogType:")
		} else if strings.HasPrefix(line, "Log Contents:") {
			contents = true
		} else if contents {
			if container == nil {
				return fmt.Errorf("Missing container info")
			}
			if logType == "stdout" {
				container.StdOut += line + "\n"
			} else if logType == "stderr" {
				container.StdErr += line + "\n"
				if strings.HasPrefix(line, job.MRLogPrefix) {
					appLog := line[len(job.MRLogPrefix):]
					container.AppLog += appLog + "\n"
					debugLog(appLog)
				}
			} else if logType == "syslog" {
				container.SysLog += line + "\n"
			} else {
				return fmt.Errorf("Invalid log type: %s", logType)
			}
		}
	}
	if container != nil {
		l.ContainerLogs = append(l.ContainerLogs, container)
	}
	return nil
}
