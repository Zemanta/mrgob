package job

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestEncodeDecodeBytes(t *testing.T) {
	in := "\nAA\tBB\\t\n\\"
	exp := "\\nAA\\tBB\\\\t\\n\\\\"
	out := string(encodeBytes([]byte(in)))

	if out != exp {
		t.Errorf("%s\n!=\n%s", out, exp)
	}

	dec := string(decodeBytes([]byte(out)))
	if dec != in {
		t.Errorf("%s\n!=\n%s", dec, in)
	}
}

func TestByteWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewByteKVWriter(buf)

	expected := `key1	string
key2
key\n2
key\n3	t\ts\t
`

	w.Write([]byte("key1"), []byte("string"))
	w.WriteKey([]byte("key2"))
	w.WriteKey([]byte("key\n2"))

	v1 := []byte("t\ts\t")
	v2 := []byte("t\ts\t")
	w.Write([]byte("key\n3"), v1)

	if buf.String() != expected {
		t.Errorf("Byte writer error:\n%s \n!=\n%s", buf.String(), expected)
	}

	if !bytes.Equal(v1, v2) {
		t.Errorf("Byte writer modified input")
	}
}

func TestByteReader(t *testing.T) {
	in := `key1	val1
key1	val2
key2	val3



key3	val4


`
	res := ""
	expected := "key1val1val2key2val3key3val4"

	r := NewByteKVReader(bytes.NewReader([]byte(in)))
	for r.Scan() {
		key, vr := r.Read()
		res += string(key)

		for vr.Scan() {
			val := vr.Read()
			res += string(val)
		}

		if err := vr.Err(); err != nil {
			t.Error(err)
		}
	}

	if err := r.Err(); err != nil {
		t.Error(err)
	}

	if res != expected {
		t.Errorf("Invalid result: %s != %s", expected, res)
	}
}

func TestJsonWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewJsonKVWriter(buf)

	expected := `"key1"	"string"
"key2"
"key3"	{"V":123}
`

	w.Write("key1", "string")
	w.WriteKey("key2")
	w.Write("key3", struct{ V int }{V: 123})

	if buf.String() != expected {
		t.Errorf("Json writer error:\n%s \n!=\n%s", buf.String(), expected)
	}
}

func TestJsonReader(t *testing.T) {
	in := `"key1"	{"V":1}
"key2"	{"V":2}

"key2"	{"V":2}


"key3"	{"V":3}

`

	type v struct {
		V int
	}

	keys := ""
	expectedKeys := "key1key2key3"
	res := 0
	expected := 8

	r := NewJsonKVReader(strings.NewReader(in))
	key := new(string)
	for r.Scan() {
		vr, err := r.Read(key)
		if err != nil {
			t.Error(err)
		}
		keys += *key

		val := &v{}
		for vr.Scan() {
			err := vr.Read(val)
			if err != nil {
				t.Error(err)
			}
			res += val.V
		}

		if err := vr.Err(); err != nil {
			t.Error(err)
		}
	}

	if err := r.Err(); err != nil {
		t.Error(err)
	}

	if res != expected {
		t.Errorf("Invalid result: %d != %d", expected, res)
	}
	if keys != expectedKeys {
		t.Errorf("Invalid result: %s != %s", expectedKeys, keys)
	}
}

func BenchmarkByteWriter(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewByteKVWriter(buf)

	k := []byte("MY NORMAL SIZED KEY")
	v := []byte("AAAAAAAAAAAAA BIIIIIIIIIIIIIIIIIIIIIT LOOOOOOOOOOOOOOOOOOOOOOOGER VAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALUE")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Write(k, v)
	}
}

func BenchmarkByteSpecialCharsWriter(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewByteKVWriter(buf)

	k := []byte("MY\tNORMAL\nSIZED\tKEY")
	v := []byte("AAAAAAAAAAAAA\nBIIIIIIIIIIIIIIIIIIIIIT\tLOOOOOOOOOOOOOOOOOOOOOOOGER\nVAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALUE")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Write(k, v)
	}
}

func BenchmarkJsonWriter(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewJsonKVWriter(buf)

	k := "MY NORMAL SIZED KEY"
	v := struct {
		V1 int
		V2 float64
		V3 string
	}{V1: 1, V2: 2.0, V3: "3"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Write(k, v)
	}
}

func BenchmarkByteReader(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewJsonKVWriter(buf)

	k := "MY NORMAL SIZED KEY"
	v := "AAAAAAAAAAAAA BIIIIIIIIIIIIIIIIIIIIIT LOOOOOOOOOOOOOOOOOOOOOOOGER VAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALUE"

	for i := 0; i < b.N; i++ {
		rk := fmt.Sprintf("%s%d", k, rand.Intn(5))
		w.Write(rk, v)
	}

	r := NewByteKVReader(buf)

	b.ResetTimer()

	for r.Scan() {
		_, vr := r.Read()

		for vr.Scan() {
			vr.Read()
		}

		if err := vr.Err(); err != nil {
			b.Error(err)
		}
	}

	if err := r.Err(); err != nil {
		b.Error(err)
	}
}

func BenchmarkByteSpecialCharsReader(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewJsonKVWriter(buf)

	k := "MY\tNORMAL\nSIZED\nKEY"
	v := "AAAAAAAAAAAAA\nBIIIIIIIIIIIIIIIIIIIIIT\tLOOOOOOOOOOOOOOOOOOOOOOOGER\nVAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALUE"

	for i := 0; i < b.N; i++ {
		rk := fmt.Sprintf("%s%d", k, rand.Intn(5))
		w.Write(rk, v)
	}

	r := NewByteKVReader(buf)

	b.ResetTimer()

	for r.Scan() {
		_, vr := r.Read()

		for vr.Scan() {
			vr.Read()
		}

		if err := vr.Err(); err != nil {
			b.Error(err)
		}
	}

	if err := r.Err(); err != nil {
		b.Error(err)
	}
}

func BenchmarkJsonReader(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewJsonKVWriter(buf)

	k := "MY NORMAL SIZED KEY"

	type str struct {
		V1 int
		V2 float64
		V3 string
	}
	v := &str{V1: 1, V2: 2.0, V3: "3"}

	for i := 0; i < b.N; i++ {
		rk := fmt.Sprintf("%s%d", k, rand.Intn(5))
		w.Write(rk, v)
	}

	key := new(string)

	r := NewJsonKVReader(buf)

	b.ResetTimer()

	for r.Scan() {
		vr, err := r.Read(key)
		if err != nil {
			b.Error(err)
		}

		for vr.Scan() {
			err := vr.Read(v)
			if err != nil {
				b.Error(err)
			}
		}

		if err := vr.Err(); err != nil {
			b.Error(err)
		}
	}

	if err := r.Err(); err != nil {
		b.Error(err)
	}
}
