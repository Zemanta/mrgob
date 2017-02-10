package tester

import (
	"bytes"
	"io"
	"sort"
	"strings"

	"github.com/Zemanta/mrgob/job"
)

type testSorter struct {
	bytes.Buffer
}

func (s *testSorter) sort() {
	lines := strings.Split(strings.TrimSpace(s.String()), "\n")
	sort.Strings(lines)
	s.Reset()
	s.WriteString(strings.Join(lines, "\n"))
	s.WriteString("\n")
}

// TestRawJob simulates a raw mapreduce job by reading the data from the input reader and writing results to the output writer
func TestRawJob(input []io.Reader, output io.Writer, mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader)) {
	sorter := &testSorter{}

	for _, in := range input {
		setReaderEnv(in)
		mapper(sorter, in)
	}
	sorter.sort()
	reducer(output, sorter)
}

// TestRawJob simulates a byte mapreduce job by reading the data from the input reader and writing results to the output writer
func TestByteJob(input []io.Reader, output io.Writer, mapper func(*job.ByteKVWriter, io.Reader), reducer func(io.Writer, *job.ByteKVReader)) {
	sorter := &testSorter{}

	for _, in := range input {
		setReaderEnv(in)
		w := job.NewByteKVWriter(sorter)
		mapper(w, in)
		w.Flush()
	}
	sorter.sort()
	reducer(output, job.NewByteKVReader(sorter))
}

// TestRawJob simulates a json mapreduce job by reading the data from the input reader and writing results to the output writer
func TestJsonJob(input []io.Reader, output io.Writer, mapper func(*job.JsonKVWriter, io.Reader), reducer func(io.Writer, *job.JsonKVReader)) {
	sorter := &testSorter{}

	for _, in := range input {
		setReaderEnv(in)
		w := job.NewJsonKVWriter(sorter)
		mapper(w, in)
		w.Flush()
	}
	sorter.sort()
	reducer(output, job.NewJsonKVReader(sorter))
}
