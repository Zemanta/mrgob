package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/Zemanta/gomr/job"
)

func main() {
	job.InitStringJob(runMapper, runReducer)
}

func runMapper(w *job.StringKVWriter, r io.Reader) {
	job.Log.Print("Mapper run")

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		for _, word := range strings.Fields(line) {
			w.Write(word, "1")
			job.Count("mapper_word", 1)
		}
	}

	if err := scanner.Err(); err != nil {
		job.Log.Fatal(err)
	}
}

func runReducer(w io.Writer, r *job.StringKVReader) {
	job.Log.Print("Reducer run")

	for r.Scan() {
		key, vr := r.Read()

		count := 0
		for vr.Scan() {
			c, err := strconv.Atoi(vr.Read())
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
