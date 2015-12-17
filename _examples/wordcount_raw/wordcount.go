package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Zemanta/mrgob/job"
)

func main() {
	job.InitRawJob(runMapper, runReducer)
}

func runMapper(w io.Writer, r io.Reader) {
	job.Log.Print("Mapper run")

	in := bufio.NewReader(os.Stdin)
	for {
		line, err := in.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			job.Log.Fatal(err)
		}

		for _, word := range strings.Fields(line) {
			fmt.Println(word)
			job.Count("mapper_word", 1)
		}
	}
}

func runReducer(w io.Writer, r io.Reader) {
	job.Log.Print("Reducer run")

	words := map[string]int{}

	in := bufio.NewReader(r)
	for {
		word, err := in.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			job.Log.Fatal(err)
		}
		words[strings.TrimSpace(word)]++
		job.Count("reducer_line", 1)
	}

	for word, c := range words {
		fmt.Fprintf(w, "%s\t%d\n", word, c)
	}
}
