package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Zemanta/gomr/job"
)

func main() {
	job.InitRawJob(runMapper, runReducer)
}

func runMapper() {
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

func runReducer() {
	job.Log.Print("Reducer run")

	words := map[string]int{}

	in := bufio.NewReader(os.Stdin)
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

	for w, c := range words {
		fmt.Printf("%s\t%d\n", w, c)
	}
}
