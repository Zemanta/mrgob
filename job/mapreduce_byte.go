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

	ErrInvalidLine = fmt.Errorf("Invalid line")
)

type ByteKVWriter struct {
	w    io.Writer
	encw *encodeWriter
}

func NewByteKVWriter(w io.Writer) *ByteKVWriter {
	return &ByteKVWriter{
		w:    w,
		encw: newEncodeWriter(w),
	}
}

func (w *ByteKVWriter) Write(k []byte, v []byte) error {
	if _, err := w.encw.Write(k); err != nil {
		return err
	}
	if _, err := w.w.Write(tab); err != nil {
		return err
	}
	if _, err := w.encw.Write(v); err != nil {
		return err
	}
	if _, err := w.w.Write(nl); err != nil {
		return err
	}
	return nil
}

func (w *ByteKVWriter) WriteKey(k []byte) error {
	if _, err := w.w.Write(encodeBytes(k)); err != nil {
		return err
	}
	if _, err := w.w.Write(nl); err != nil {
		return err
	}
	return nil
}

type ByteKVReader struct {
	scanner *bufio.Scanner
	vr      *ByteValueReader
}

func NewByteKVReader(r io.Reader) *ByteKVReader {
	return &ByteKVReader{
		scanner: bufio.NewScanner(r),
	}
}

func (r *ByteKVReader) Scan() bool {
	if r.vr == nil {
		r.vr = &ByteValueReader{scanner: r.scanner}
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

func (r *ByteKVReader) Key() ([]byte, *ByteValueReader) {
	return decodeBytes(r.vr.key), r.vr
}

func (r *ByteKVReader) Err() error {
	if r.vr != nil {
		return r.vr.Err()
	}
	return r.scanner.Err()
}

type ByteValueReader struct {
	scanner *bufio.Scanner

	skip int
	done bool

	err   error
	key   []byte
	value []byte
}

func (r *ByteValueReader) Scan() bool {
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

func (r *ByteValueReader) Value() []byte {
	return decodeBytes(r.value)
}

func (r *ByteValueReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.scanner.Err()
}
