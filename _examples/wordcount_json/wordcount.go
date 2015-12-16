package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/Zemanta/gomr/job"
)

func main() {
	job.InitJsonJob(runMapper, runReducer)
}

func runMapper(w *job.JsonKVWriter, r io.Reader) {
	job.Log.Print("Mapper run")

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		for _, word := range strings.Fields(line) {
			w.Write(word, 1)
			job.Count("mapper_word", 1)
		}
	}

	if err := scanner.Err(); err != nil {
		job.Log.Fatal(err)
	}
}

func runReducer(w io.Writer, r *job.JsonKVReader) {
	job.Log.Print("Reducer run")

	key := new(string)
	for r.Scan() {
		vr, err := r.Key(key)
		if err != nil {
			job.Log.Fatal(err)
		}

		count := 0
		c := new(int)
		for vr.Scan() {
			err := vr.Value(c)
			if err != nil {
				job.Log.Fatal(err)
			}
			count += *c
		}

		if err := vr.Err(); err != nil {
			job.Log.Fatal(err)
		}

		fmt.Fprintf(w, "%s\t%d\n", *key, count)
		job.Count("reducer_word", 1)
	}

	if err := r.Err(); err != nil {
		job.Log.Fatal(err)
	}
}
