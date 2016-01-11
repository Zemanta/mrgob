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

// JsonKVWriter encodes and writes key, value pairs to the writer
type JsonKVWriter struct {
	w *bufio.Writer

	valuew *json.Encoder
	keyw   *json.Encoder
}

func NewJsonKVWriter(w io.Writer) *JsonKVWriter {
	bufw := bufio.NewWriter(w)
	return &JsonKVWriter{
		w:      bufw,
		valuew: json.NewEncoder(bufw),
		keyw:   json.NewEncoder(&keyJsonWriter{bufw}),
	}
}

// Write encodes both key and value
func (w *JsonKVWriter) Write(k interface{}, v interface{}) error {
	if err := w.keyw.Encode(k); err != nil {
		return err
	}

	if _, err := w.w.Write(tab); err != nil {
		return err
	}

	return w.valuew.Encode(v)
}

// WriteKey only accepts a key in case your mapper doesn't require values
func (w *JsonKVWriter) WriteKey(k interface{}) error {
	return w.valuew.Encode(k)
}

func (w *JsonKVWriter) Flush() {
	w.w.Flush()
}

// JsonKVReader streams key, value pairs from the reader and merges them for easier consumption by the reducer
type JsonKVReader struct {
	reader *bufio.Reader
	vr     *JsonValueReader
	key    []byte
}

func NewJsonKVReader(r io.Reader) *JsonKVReader {
	return &JsonKVReader{
		reader: bufio.NewReader(r),
	}
}

// Scan advances the reader to the next key, which will then be available through the Key method. It returns false when the scan stops, either by reaching the end of the input or an error. After Scan returns false, the Err method will return any error that occurred during scanning, except that if it was io.EOF, Err will return nil.
func (r *JsonKVReader) Scan() bool {
	if r.vr == nil {
		r.vr = &JsonValueReader{reader: r.reader}
		sc := r.vr.Scan()
		r.vr.skip = 1
		r.key = copyResize(r.key, r.vr.key)
		return sc
	}

	if r.vr.Err() != nil {
		return false
	}

	r.vr.err = nil
	r.vr.skip = 1
	r.key = copyResize(r.key, r.vr.key)
	return !r.vr.done
}

// Key decodes the current key into the target interface and returns a reader for all values belonging to this key.
func (r *JsonKVReader) Key(target interface{}) (*JsonValueReader, error) {
	return r.vr, json.Unmarshal(r.key, target)
}

// Err returns the first non-EOF error that was encountered by the reader.
func (r *JsonKVReader) Err() error {
	if r.vr != nil {
		return r.vr.Err()
	}
	return nil
}

// JsonValueReader streams values for the specified key.
type JsonValueReader struct {
	reader *bufio.Reader

	skip int
	done bool

	err   error
	key   []byte
	value []byte
}

// Scan advances the reader to the next value, which will then be available through the Value method.
func (r *JsonValueReader) Scan() bool {
	if r.skip > 0 {
		r.skip--
		return true
	}

	// skip empty lines
	var line []byte
	var err error
	for len(line) == 0 {
		line, err = r.reader.ReadBytes('\n') // does allocs, TODO fix
		if err == io.EOF {
			r.done = true
			return false
		} else if err != nil {
			r.err = err
			return false
		}
		n := len(line)
		if n > 0 && line[n-1] == '\n' {
			line = line[:n-1]
		}
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

	if len(r.key) == 0 || !ok {
		r.key = copyResize(r.key, key)
	}

	if len(line) > split {
		r.value = line[split+1:]
	} else {
		r.value = nil
	}

	return ok
}

// Value decodes the current value into the target interface.
func (r *JsonValueReader) Value(target interface{}) error {
	return json.Unmarshal(r.value, target)
}

// Err returns the first non-EOF error that was encountered by the reader.
func (r *JsonValueReader) Err() error {
	return r.err
}
