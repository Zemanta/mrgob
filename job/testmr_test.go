package job

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestRawTester(t *testing.T) {
	in := &bytes.Buffer{}
	out := &bytes.Buffer{}

	in.WriteString("word1\n")
	in.WriteString("word2\n")
	in.WriteString("word1\n")

	expected := `word1	2
word2	1
`

	mapper := func(w io.Writer, r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			w.Write([]byte(line))
			w.Write([]byte{'\n'})
		}
	}
	reducer := func(w io.Writer, r io.Reader) {
		scanner := bufio.NewScanner(r)
		lp := ""
		c := 0
		for scanner.Scan() {
			line := scanner.Text()
			if lp == "" {
				lp = line
			}
			if line != lp {
				fmt.Fprintf(w, "%s\t%d\n", lp, c)

				lp = line
				c = 0
			}
			c++
		}
		fmt.Fprintf(w, "%s\t%d\n", lp, c)
	}

	TestRawJob(in, out, mapper, reducer)

	if expected != out.String() {
		t.Errorf("\n%s\n!=\n%s", out.String(), expected)
	}
}

func TestByteTester(t *testing.T) {
	in := &bytes.Buffer{}
	out := &bytes.Buffer{}

	in.WriteString("word1\n")
	in.WriteString("word2\n")
	in.WriteString("word1\n")

	expected := `word1	2
word2	1
`

	mapper := func(w *ByteKVWriter, r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			w.WriteKey([]byte(line))
		}
	}
	reducer := func(w io.Writer, r *ByteKVReader) {
		for r.Scan() {
			key, valueReader := r.Key()
			c := 0
			for valueReader.Scan() {
				c++
			}
			fmt.Fprintf(w, "%s\t%d\n", string(key), c)
		}
		if err := r.Err(); err != nil {
			t.Error(err)
		}
	}

	TestByteJob(in, out, mapper, reducer)

	if expected != out.String() {
		t.Errorf("\n%s\n!=\n%s", out.String(), expected)
	}
}

func TestJsonTester(t *testing.T) {
	in := &bytes.Buffer{}
	out := &bytes.Buffer{}

	in.WriteString("word1\n")
	in.WriteString("word2\n")
	in.WriteString("word1\n")

	expected := `word1	2
word2	1
`

	mapper := func(w *JsonKVWriter, r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			w.WriteKey(line)
		}
	}
	reducer := func(w io.Writer, r *JsonKVReader) {
		for r.Scan() {
			key := new(string)
			valueReader, err := r.Key(key)
			if err != nil {
				t.Error(err)
			}
			c := 0
			for valueReader.Scan() {
				c++
			}
			fmt.Fprintf(w, "%s\t%d\n", *key, c)
		}
		if err := r.Err(); err != nil {
			t.Error(err)
		}
	}

	TestJsonJob(in, out, mapper, reducer)

	if expected != out.String() {
		t.Errorf("\n%s\n!=\n%s", out.String(), expected)
	}
}
