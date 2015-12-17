# job
--
    import "github.com/Zemanta/mrgob/job/tester"


## Usage

#### func  TestByteJob

```go
func TestByteJob(input io.Reader, output io.Writer, mapper func(*job.ByteKVWriter, io.Reader), reducer func(io.Writer, *job.ByteKVReader))
```
TestRawJob simulates a byte mapreduce job by reading the data from the input
reader and writing results to the output writer

#### func  TestJsonJob

```go
func TestJsonJob(input io.Reader, output io.Writer, mapper func(*job.JsonKVWriter, io.Reader), reducer func(io.Writer, *job.JsonKVReader))
```
TestRawJob simulates a json mapreduce job by reading the data from the input
reader and writing results to the output writer

#### func  TestRawJob

```go
func TestRawJob(input io.Reader, output io.Writer, mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader))
```
TestRawJob simulates a raw mapreduce job by reading the data from the input
reader and writing results to the output writer
