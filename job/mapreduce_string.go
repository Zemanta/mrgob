package job

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

var (
	ErrInvalidLine = fmt.Errorf("Invalid line")
)

func encodeKey(k string) string {
	// TODO other special chars
	return strings.Replace(k, "\t", "\\t", -1)
}

func decodeKey(k string) string {
	return strings.Replace(k, "\\t", "\t", -1)
}

type StringKVWriter struct {
	w io.Writer
}

func NewStringKVWriter(w io.Writer) *StringKVWriter {
	return &StringKVWriter{
		w: w,
	}
}

func (w *StringKVWriter) Write(k string, v string) {
	fmt.Fprintf(w.w, "%s\t%s\n", encodeKey(k), v)
}

func (w *StringKVWriter) WriteKey(k string) {
	fmt.Println(k)
}

type StringKVReader struct {
	scanner *bufio.Scanner
	vr      *StringValueReader
}

func NewStringKVReader(r io.Reader) *StringKVReader {
	return &StringKVReader{
		scanner: bufio.NewScanner(r),
	}
}

func (r *StringKVReader) Scan() bool {
	if r.vr == nil {
		r.vr = &StringValueReader{scanner: r.scanner}
		sc := r.vr.Scan()
		r.vr.skip = 1
		return sc
	}

	if r.vr.Err() != nil {
		return false
	}

	r.vr.err = nil
	r.vr.skip = 1
	return !r.vr.done
}

func (r *StringKVReader) Read() (string, *StringValueReader) {
	return r.vr.key, r.vr
}

func (r *StringKVReader) Err() error {
	if r.vr != nil {
		return r.vr.Err()
	}
	return r.scanner.Err()
}

type StringValueReader struct {
	scanner *bufio.Scanner

	skip int
	done bool

	err   error
	key   string
	value string
}

func (r *StringValueReader) Scan() bool {
	if r.skip > 0 {
		r.skip--
		return true
	}

	// skip empty lines
	line := ""
	for line == "" {
		if !r.scanner.Scan() {
			r.done = true
			return false
		}

		line = r.scanner.Text()
	}

	parts := strings.SplitN(line, "\t", 2)
	if len(parts) != 2 {
		r.err = ErrInvalidLine
		return false
	}

	ok := true
	if r.key != "" && parts[0] != r.key {
		ok = false
	}

	r.key = decodeKey(parts[0])
	r.value = parts[1]

	return ok
}

func (r *StringValueReader) Read() string {
	return r.value
}

func (r *StringValueReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.scanner.Err()
}
