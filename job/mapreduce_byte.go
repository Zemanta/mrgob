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

// Escape tab and new line
func encodeBytes(bs []byte) []byte {
	var repl []byte
	copied := false

	for i := 0; i < len(bs); i++ {
		repl = nil

		if bs[i] == '\t' {
			repl = esctab
		} else if bs[i] == '\n' {
			repl = escnl
		} else if bs[i] == '\\' {
			repl = escesc
		}

		if repl != nil {
			if !copied {
				newbs := make([]byte, len(bs), len(bs)*2)
				copy(newbs, bs)
				bs = newbs
				copied = true
			}
			bs = append(bs[:i+1], bs[i:]...)
			bs[i] = repl[0]
			bs[i+1] = repl[1]
			i++
		}
	}
	return bs
}
func decodeBytes(bs []byte) []byte {
	copied := false

	for i := 0; i < len(bs); i++ {
		if bs[i] != '\\' {
			continue
		}
		if !copied {
			newbs := make([]byte, len(bs))
			copy(newbs, bs)
			bs = newbs
			copied = true
		}
		bs = append(bs[:i], bs[i+1:]...)
		if bs[i] == 't' {
			bs[i] = '\t'
		} else if bs[i] == 'n' {
			bs[i] = '\n'
		}
	}
	return bs
}

type ByteKVWriter struct {
	w io.Writer
}

func NewByteKVWriter(w io.Writer) *ByteKVWriter {
	return &ByteKVWriter{
		w: w,
	}
}

func (w *ByteKVWriter) Write(k []byte, v []byte) error {
	if _, err := w.w.Write(encodeBytes(k)); err != nil {
		return err
	}
	if _, err := w.w.Write(tab); err != nil {
		return err
	}
	if _, err := w.w.Write(encodeBytes(v)); err != nil {
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

func (r *ByteKVReader) Read() ([]byte, *ByteValueReader) {
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
		r.err = ErrInvalidLine
		return false
	}

	key := line[0:split]

	ok := true
	if len(r.key) != 0 && !bytes.Equal(key, r.key) {
		ok = false
	}

	r.key = key
	r.value = line[split+1:]

	return ok
}

func (r *ByteValueReader) Read() []byte {
	return decodeBytes(r.value)
}

func (r *ByteValueReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.scanner.Err()
}
