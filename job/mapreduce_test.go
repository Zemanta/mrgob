package job

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestCopyResize(t *testing.T) {
	src := []byte{1, 2, 3}

	dst := make([]byte, 3)
	dst1 := copyResize(dst, src)
	if !bytes.Equal(src, dst1) {
		t.Error("Copy resize not equal")
	}
	if !bytes.Equal(dst, dst1) {
		t.Error("Copy resize shouldn't resize")
	}

	dst = make([]byte, 0, 3)
	dst1 = copyResize(dst, src)
	if !bytes.Equal(src, dst1) {
		t.Error("Copy resize not equal")
	}
	if !bytes.Equal(dst[0:3], dst1) {
		t.Error("Copy resize shouldn't resize")
	}

	dst = make([]byte, 2)
	dst1 = copyResize(dst, src)
	if !bytes.Equal(src, dst1) {
		t.Error("Copy resize not equal")
	}
}

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

func TestEncodeWriterTab(t *testing.T) {
	in := "\nAA\tBB\\t\n\\a"
	exp := "\\nAA\\tBB\\\\t\\n\\\\a"
	out := &bytes.Buffer{}

	w := newEncodeWriter(out, true)
	w.Write([]byte(in))

	if out.String() != exp {
		t.Errorf("%s\n!=\n%s", out, exp)
	}
}

func TestEncodeWriterNoTab(t *testing.T) {
	in := "\nAA\tBB\\t\n\\a"
	exp := "\\nAA\tBB\\\\t\\n\\\\a"
	out := &bytes.Buffer{}

	w := newEncodeWriter(out, false)
	w.Write([]byte(in))

	if out.String() != exp {
		t.Errorf("%s\n!=\n%s", out, exp)
	}
}

func TestByteWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	w := NewByteKVWriter(buf)

	expected := `key1	string
key2
key\n2
key\n3	t	s	
`

	w.Write([]byte("key1"), []byte("string"))
	w.WriteKey([]byte("key2"))
	w.WriteKey([]byte("key\n2"))

	v1 := []byte("t\ts\t")
	v2 := []byte("t\ts\t")
	w.Write([]byte("key\n3"), v1)

	w.Flush()

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
		key, vr := r.Key()
		res += string(key)

		key1str := string(key)

		for vr.Scan() {
			val := vr.Value()
			res += string(val)
		}

		if err := vr.Err(); err != nil {
			t.Error(err)
		}

		key2, _ := r.Key()
		if key1str != string(key2) {
			t.Error("Key changed before scan")
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

	w.Flush()

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
		vr, err := r.Key(key)
		if err != nil {
			t.Error(err)
		}
		keys += *key

		for vr.Scan() {
			val := &v{}
			err := vr.Value(val)
			if err != nil {
				t.Error(err)
			}
			res += val.V
		}

		if err := vr.Err(); err != nil {
			t.Error(err)
		}

		key2 := new(string)
		r.Key(key2)
		if *key != *key2 {
			t.Errorf("Key changed before scan: %s != %s", *key, *key2)
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
	w.Flush()
}

func BenchmarkByteSpecialCharsWriter(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewByteKVWriter(buf)

	k := []byte("MY\tNRMAL\nSZED\tKY")
	v := []byte("AAAAAAAAAAAAA\nBIIIIIIIIIIIIIIIIIIIIT\tLOOOOOOOOOOOOOOOOOOOOOOOGER\nVAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALUE")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Write(k, v)
	}
	w.Flush()
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
	w.Flush()
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
	w.Flush()

	r := NewByteKVReader(buf)

	b.ResetTimer()

	for r.Scan() {
		_, vr := r.Key()

		for vr.Scan() {
			vr.Value()
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
	w.Flush()

	r := NewByteKVReader(buf)

	b.ResetTimer()

	for r.Scan() {
		_, vr := r.Key()

		for vr.Scan() {
			vr.Value()
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
	w.Flush()

	key := new(string)

	r := NewJsonKVReader(buf)

	b.ResetTimer()

	for r.Scan() {
		vr, err := r.Key(key)
		if err != nil {
			b.Error(err)
		}

		for vr.Scan() {
			err := vr.Value(v)
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

func BenchmarkSimpleJsonReader(b *testing.B) {
	buf := &bytes.Buffer{}
	w := NewJsonKVWriter(buf)

	k := "MY NORMAL SIZED KEY"

	v := 123

	for i := 0; i < b.N; i++ {
		rk := fmt.Sprintf("%s%d", k, rand.Intn(5))
		w.Write(rk, v)
	}
	w.Flush()

	key := new(string)

	r := NewJsonKVReader(buf)

	b.ResetTimer()

	for r.Scan() {
		vr, err := r.Key(key)
		if err != nil {
			b.Error(err)
		}

		for vr.Scan() {
			err := vr.Value(&v)
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
