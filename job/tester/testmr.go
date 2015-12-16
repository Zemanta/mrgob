package job

import (
	"bytes"
	"io"
	"sort"
	"strings"

	"github.com/Zemanta/gomr/job"
)

type testSorter struct {
	bytes.Buffer
}

func (s *testSorter) sort() {
	lines := strings.Split(strings.TrimSpace(s.String()), "\n")
	sort.Strings(lines)
	s.Reset()
	s.WriteString(strings.Join(lines, "\n"))
}

func TestRawJob(input io.Reader, output io.Writer, mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader)) {
	sorter := &testSorter{}

	mapper(sorter, input)
	sorter.sort()
	reducer(output, sorter)
}

func TestByteJob(input io.Reader, output io.Writer, mapper func(*job.ByteKVWriter, io.Reader), reducer func(io.Writer, *job.ByteKVReader)) {
	sorter := &testSorter{}

	mapper(job.NewByteKVWriter(sorter), input)
	sorter.sort()
	reducer(output, job.NewByteKVReader(sorter))
}

func TestJsonJob(input io.Reader, output io.Writer, mapper func(*job.JsonKVWriter, io.Reader), reducer func(io.Writer, *job.JsonKVReader)) {
	sorter := &testSorter{}

	mapper(job.NewJsonKVWriter(sorter), input)
	sorter.sort()
	reducer(output, job.NewJsonKVReader(sorter))
}
