package job

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

var (
	nl  = []byte{'\n'}
	tab = []byte{'\t'}

	escnl  = []byte{'\\', 'n'}
	esctab = []byte{'\\', 't'}
	escesc = []byte{'\\', '\\'}
)

var ErrInvalidLine = fmt.Errorf("Invalid line")

// ByteKVWriter encodes and writes key, value pairs to the writer
type ByteKVWriter struct {
	w    *bufio.Writer
	enck *encodeWriter
	encv *encodeWriter
}

func NewByteKVWriter(w io.Writer) *ByteKVWriter {
	bufw := bufio.NewWriter(w)
	return &ByteKVWriter{
		w:    bufw,
		enck: newEncodeWriter(bufw, true),
		encv: newEncodeWriter(bufw, false),
	}
}

// Write encodes both key and value
func (w *ByteKVWriter) Write(k []byte, vs ...[]byte) error {
	if _, err := w.enck.Write(k); err != nil {
		return err
	}
	if _, err := w.w.Write(tab); err != nil {
		return err
	}
	for _, v := range vs {
		if _, err := w.encv.Write(v); err != nil {
			return err
		}
	}
	if _, err := w.w.Write(nl); err != nil {
		return err
	}
	return nil
}

// WriteKey only accepts a key in case your mapper doesn't require values
func (w *ByteKVWriter) WriteKey(k []byte) error {
	if _, err := w.enck.Write(k); err != nil {
		return err
	}
	if _, err := w.w.Write(nl); err != nil {
		return err
	}
	return nil
}

func (w *ByteKVWriter) Flush() {
	w.w.Flush()
}

// ByteKVReader streams key, value pairs from the reader and merges them for easier consumption by the reducer
type ByteKVReader struct {
	reader *bufio.Reader
	vr     *ByteValueReader
	key    []byte
}

func NewByteKVReader(r io.Reader) *ByteKVReader {
	return &ByteKVReader{
		reader: bufio.NewReader(r),
	}
}

// Scan advances the reader to the next key, which will then be available through the Key method. It returns false when the scan stops, either by reaching the end of the input or an error. After Scan returns false, the Err method will return any error that occurred during scanning, except that if it was io.EOF, Err will return nil.
func (r *ByteKVReader) Scan() bool {
	if r.vr == nil {
		r.vr = &ByteValueReader{reader: r.reader}
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

// Key returns decoded key and reader for all values belonging to this key.
// The underlying array may point to data that will be overwritten by a subsequent call to Scan. It does no allocation.
func (r *ByteKVReader) Key() ([]byte, *ByteValueReader) {
	return decodeBytes(r.key), r.vr
}

// Err returns the first non-EOF error that was encountered by the reader.
func (r *ByteKVReader) Err() error {
	if r.vr != nil {
		return r.vr.Err()
	}
	return nil
}

// ByteValueReader streams values for the specified key.
type ByteValueReader struct {
	reader *bufio.Reader

	skip int
	done bool

	err   error
	key   []byte
	value []byte
}

// Scan advances the reader to the next value, which will then be available through the Value method.
func (r *ByteValueReader) Scan() bool {
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

// Value decodes the current value and returns it.
// The underlying array may point to data that will be overwritten by a subsequent call to Scan. It does no allocation.
func (r *ByteValueReader) Value() []byte {
	return decodeBytes(r.value)
}

// Err returns the first non-EOF error that was encountered by the reader.
func (r *ByteValueReader) Err() error {
	return r.err
}
