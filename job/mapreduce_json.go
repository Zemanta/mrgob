package job

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
)

// Json encoder appends new line after each value so we have to strip it
type keyJsonWriter struct {
	w io.Writer
}

func (w *keyJsonWriter) Write(b []byte) (int, error) {
	n := len(b)
	if b[n-1] == '\n' {
		return w.w.Write(b[:n-1])
	}
	return w.w.Write(b)
}

type JsonKVWriter struct {
	w io.Writer

	valuew *json.Encoder
	keyw   *json.Encoder
}

func NewJsonKVWriter(w io.Writer) *JsonKVWriter {
	return &JsonKVWriter{
		w:      w,
		valuew: json.NewEncoder(w),
		keyw:   json.NewEncoder(&keyJsonWriter{w}),
	}
}

func (w *JsonKVWriter) Write(k interface{}, v interface{}) error {
	if err := w.keyw.Encode(k); err != nil {
		return err
	}

	w.w.Write(tab)

	return w.valuew.Encode(v)
}

func (w *JsonKVWriter) WriteKey(k interface{}) error {
	return w.valuew.Encode(k)
}

type JsonKVReader struct {
	scanner *bufio.Scanner
	vr      *JsonValueReader
}

func NewJsonKVReader(r io.Reader) *JsonKVReader {
	return &JsonKVReader{
		scanner: bufio.NewScanner(r),
	}
}

func (r *JsonKVReader) Scan() bool {
	if r.vr == nil {
		r.vr = &JsonValueReader{scanner: r.scanner}
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

func (r *JsonKVReader) Key(target interface{}) (*JsonValueReader, error) {
	return r.vr, json.Unmarshal(r.vr.key, target)
}

func (r *JsonKVReader) Err() error {
	if r.vr != nil {
		return r.vr.Err()
	}
	return r.scanner.Err()
}

type JsonValueReader struct {
	scanner *bufio.Scanner

	skip int
	done bool

	err   error
	key   []byte
	value []byte
}

func (r *JsonValueReader) Scan() bool {
	if r.skip > 0 {
		r.skip--
		return true
	}

	// skip empty lines
	var line []byte
	for len(line) == 0 {
		if !r.scanner.Scan() {
			r.done = true
			return false
		}

		line = r.scanner.Bytes()
	}

	split := bytes.IndexByte(line, '\t')
	if split < 0 {
		split = len(line)
	}

	key := line[0:split]

	ok := true
	if len(r.key) != 0 && !bytes.Equal(key, r.key) {
		ok = false
	}

	r.key = key
	if len(line) > split {
		r.value = line[split+1:]
	} else {
		r.value = nil
	}

	return ok
}

func (r *JsonValueReader) Value(target interface{}) error {
	return json.Unmarshal(r.value, target)
}

func (r *JsonValueReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.scanner.Err()
}
