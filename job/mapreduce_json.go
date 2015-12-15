package job

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
)

var (
	nl  = []byte{'\n'}
	tab = []byte{'\t'}
)

type JsonKVWriter struct {
	w io.Writer

	js *json.Encoder
}

func NewJsonKVWriter(w io.Writer) *JsonKVWriter {
	return &JsonKVWriter{
		w:  w,
		js: json.NewEncoder(w),
	}
}

func (w *JsonKVWriter) Write(k string, v interface{}) error {
	w.w.Write([]byte(encodeKey(k)))
	w.w.Write(tab)

	return w.js.Encode(v)
}

func (w *JsonKVWriter) WriteKey(k string) {
	w.w.Write([]byte(k))
	w.w.Write(nl)
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

func (r *JsonKVReader) Read() (string, *JsonValueReader) {
	return decodeKey(string(r.vr.key)), r.vr
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
		r.err = ErrInvalidLine
		return false
	}

	key := line[0:split]

	ok := true
	if len(r.key) != 0 && !bytes.Equal(key, r.key) {
		ok = false
	}

	r.key = key
	r.value = line[split:]

	return ok
}

func (r *JsonValueReader) Read(target interface{}) error {
	return json.Unmarshal(r.value, target)
}

func (r *JsonValueReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.scanner.Err()
}
