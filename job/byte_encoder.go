package job

import "io"

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

type encodeWriter struct {
	w   io.Writer
	tab bool
}

func newEncodeWriter(w io.Writer, tab bool) *encodeWriter {
	return &encodeWriter{
		w:   w,
		tab: tab,
	}
}

func (e *encodeWriter) Write(bs []byte) (int, error) {
	n := 0
	pre := 0
	i := 0
	for ; i < len(bs); i++ {
		var repl []byte

		if e.tab && bs[i] == '\t' {
			repl = esctab
		} else if bs[i] == '\n' {
			repl = escnl
		} else if bs[i] == '\\' {
			repl = escesc
		}

		if repl == nil {
			continue
		}

		nd, err := e.w.Write(bs[pre:i])
		n += nd
		if err != nil {
			return n, err
		}

		pre = i + 1

		e.w.Write(repl)
	}

	if pre < i {
		nd, err := e.w.Write(bs[pre:i])
		n += nd
		if err != nil {
			return n, err
		}
	}

	return n, nil
}
