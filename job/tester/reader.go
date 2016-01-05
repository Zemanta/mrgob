package tester

import (
	"io"
	"os"
)

type Reader struct {
	Filename string
	Data     io.Reader
}

func (r *Reader) Read(p []byte) (n int, err error) {
	return r.Data.Read(p)
}

func setReaderEnv(rawr io.Reader) {
	os.Setenv("mapreduce_map_input_file", "")

	r, ok := rawr.(*Reader)
	if !ok {
		return
	}

	os.Setenv("mapreduce_map_input_file", r.Filename)
}
