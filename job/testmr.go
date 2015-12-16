package job

import (
	"bytes"
	"io"
	"sort"
	"strings"
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

func TestByteJob(input io.Reader, output io.Writer, mapper func(*ByteKVWriter, io.Reader), reducer func(io.Writer, *ByteKVReader)) {
	sorter := &testSorter{}

	mapper(NewByteKVWriter(sorter), input)
	sorter.sort()
	reducer(output, NewByteKVReader(sorter))
}

func TestJsonJob(input io.Reader, output io.Writer, mapper func(*JsonKVWriter, io.Reader), reducer func(io.Writer, *JsonKVReader)) {
	sorter := &testSorter{}

	mapper(NewJsonKVWriter(sorter), input)
	sorter.sort()
	reducer(output, NewJsonKVReader(sorter))
}
