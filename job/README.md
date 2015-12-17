# job
--
    import "github.com/Zemanta/mrgob/job"


## Usage

```go
var AppCounterGroup = "mrgob"
```

```go
var ErrInvalidLine = fmt.Errorf("Invalid line")
```

```go
var Log = log.New(os.Stderr, MRLogPrefix, log.Ldate|log.Ltime|log.Lshortfile)
```
Log is an instance of go's logger which prefixes all log lines so they can be
collected in the runner.

```go
var MRLogPrefix = "[mrgob]"
```

#### func  Count

```go
func Count(name string, c int)
```
Count increases hadoop counter for the running job. Use it for counting
processed lines, errors etc.

#### func  InitByteJob

```go
func InitByteJob(mapper func(*ByteKVWriter, io.Reader), reducer func(io.Writer, *ByteKVReader))
```
InitByteJob initiates a byte reader/writer mapreduce job, calling an appropriate
function based on the mapreduce stage

#### func  InitJsonJob

```go
func InitJsonJob(mapper func(*JsonKVWriter, io.Reader), reducer func(io.Writer, *JsonKVReader))
```
InitByteJob initiates a json reader/writer mapreduce job, calling an appropriate
function based on the mapreduce stage

#### func  InitRawJob

```go
func InitRawJob(mapper func(io.Writer, io.Reader), reducer func(io.Writer, io.Reader))
```
InitRawJob initiates a raw mapreduce job, calling an appropriate function based
on the mapreduce stage

#### type ByteKVReader

```go
type ByteKVReader struct {
}
```

ByteKVReader streams key, value pairs from the reader and merges them for easier
consumption by the reducer

#### func  NewByteKVReader

```go
func NewByteKVReader(r io.Reader) *ByteKVReader
```

#### func (*ByteKVReader) Err

```go
func (r *ByteKVReader) Err() error
```
Err returns the first non-EOF error that was encountered by the reader.

#### func (*ByteKVReader) Key

```go
func (r *ByteKVReader) Key() ([]byte, *ByteValueReader)
```
Key returns decoded key and reader for all values belonging to this key.

#### func (*ByteKVReader) Scan

```go
func (r *ByteKVReader) Scan() bool
```
Scan advances the reader to the next key, which will then be available through
the Key method. It returns false when the scan stops, either by reaching the end
of the input or an error. After Scan returns false, the Err method will return
any error that occurred during scanning, except that if it was io.EOF, Err will
return nil.

#### type ByteKVWriter

```go
type ByteKVWriter struct {
}
```

ByteKVWriter encodes and writes key, value pairs to the writer

#### func  NewByteKVWriter

```go
func NewByteKVWriter(w io.Writer) *ByteKVWriter
```

#### func (*ByteKVWriter) Write

```go
func (w *ByteKVWriter) Write(k []byte, v []byte) error
```
Write encodes both key and value

#### func (*ByteKVWriter) WriteKey

```go
func (w *ByteKVWriter) WriteKey(k []byte) error
```
WriteKey only accepts a key in case your mapper doesn't require values

#### type ByteValueReader

```go
type ByteValueReader struct {
}
```

ByteValueReader streams values for the specified key.

#### func (*ByteValueReader) Err

```go
func (r *ByteValueReader) Err() error
```
Err returns the first non-EOF error that was encountered by the reader.

#### func (*ByteValueReader) Scan

```go
func (r *ByteValueReader) Scan() bool
```
Scan advances the reader to the next value, which will then be available through
the Value method.

#### func (*ByteValueReader) Value

```go
func (r *ByteValueReader) Value() []byte
```
Value decodes the current value and returns it.

#### type JsonKVReader

```go
type JsonKVReader struct {
}
```

JsonKVReader streams key, value pairs from the reader and merges them for easier
consumption by the reducer

#### func  NewJsonKVReader

```go
func NewJsonKVReader(r io.Reader) *JsonKVReader
```

#### func (*JsonKVReader) Err

```go
func (r *JsonKVReader) Err() error
```
Err returns the first non-EOF error that was encountered by the reader.

#### func (*JsonKVReader) Key

```go
func (r *JsonKVReader) Key(target interface{}) (*JsonValueReader, error)
```
Key decodes the current key into the target interface and returns a reader for
all values belonging to this key.

#### func (*JsonKVReader) Scan

```go
func (r *JsonKVReader) Scan() bool
```
Scan advances the reader to the next key, which will then be available through
the Key method. It returns false when the scan stops, either by reaching the end
of the input or an error. After Scan returns false, the Err method will return
any error that occurred during scanning, except that if it was io.EOF, Err will
return nil.

#### type JsonKVWriter

```go
type JsonKVWriter struct {
}
```

JsonKVWriter encodes and writes key, value pairs to the writer

#### func  NewJsonKVWriter

```go
func NewJsonKVWriter(w io.Writer) *JsonKVWriter
```

#### func (*JsonKVWriter) Write

```go
func (w *JsonKVWriter) Write(k interface{}, v interface{}) error
```
Write encodes both key and value

#### func (*JsonKVWriter) WriteKey

```go
func (w *JsonKVWriter) WriteKey(k interface{}) error
```
WriteKey only accepts a key in case your mapper doesn't require values

#### type JsonValueReader

```go
type JsonValueReader struct {
}
```

JsonValueReader streams values for the specified key.

#### func (*JsonValueReader) Err

```go
func (r *JsonValueReader) Err() error
```
Err returns the first non-EOF error that was encountered by the reader.

#### func (*JsonValueReader) Scan

```go
func (r *JsonValueReader) Scan() bool
```
Scan advances the reader to the next value, which will then be available through
the Value method.

#### func (*JsonValueReader) Value

```go
func (r *JsonValueReader) Value(target interface{}) error
```
Value decodes the current value into the target interface.
