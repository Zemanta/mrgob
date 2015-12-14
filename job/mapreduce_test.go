package job

import (
	"strings"
	"testing"
)

func TestStringReader(t *testing.T) {
	in := `key1	val1
key1	val2
key2	val3



key3	val4


`
	res := ""
	expected := "key1val1val2key2val3key3val4"

	r := NewStringKVReader(strings.NewReader(in))
	for r.Scan() {
		key, vr := r.Read()
		res += key

		for vr.Scan() {
			val := vr.Read()
			res += val
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
