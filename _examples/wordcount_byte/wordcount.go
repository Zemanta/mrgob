package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/Zemanta/mrgob/job"
)

func main() {
	job.InitByteJob(runMapper, runReducer)
}

func runMapper(w *job.ByteKVWriter, r io.Reader) {
	job.Log.Print("Mapper run")

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		for _, word := range strings.Fields(line) {
			w.Write([]byte(word), []byte("1"))
			job.Count("mapper_word", 1)
		}
	}

	if err := scanner.Err(); err != nil {
		job.Log.Fatal(err)
	}
}

func runReducer(w io.Writer, r *job.ByteKVReader) {
	job.Log.Print("Reducer run")

	for r.Scan() {
		key, vr := r.Key()

		count := 0
		for vr.Scan() {
			c, err := strconv.Atoi(string(vr.Value()))
			if err != nil {
				job.Log.Fatal(err)
			}
			count += c
		}

		if err := vr.Err(); err != nil {
			job.Log.Fatal(err)
		}

		fmt.Fprintf(w, "%s\t%d\n", key, count)
		job.Count("reducer_word", 1)
	}

	if err := r.Err(); err != nil {
		job.Log.Fatal(err)
	}
}
