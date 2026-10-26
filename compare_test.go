//go:build goexperiment.jsonv2

// Comparison benchmarks against the standard library JSON packages.
// encoding/json/jsontext and encoding/json/v2 require GOEXPERIMENT=jsonv2:
//
//	GOEXPERIMENT=jsonv2 go test -run=Compare -bench=Compare -benchmem
package encoder

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

type record struct {
	ID    uint32  `json:"id"`
	Name  string  `json:"name"`
	Count uint64  `json:"count"`
	Ratio float64 `json:"ratio"`
}

var (
	keyID    = []byte("id")
	keyName  = []byte("name")
	keyCount = []byte("count")
	keyRatio = []byte("ratio")
)

const (
	smallDoc = 32    // fits in a single MAXBUFSIZE buffer.
	largeDoc = 10000 // forces many flushes to the pipe.
)

func makeRecords(n int) []record {
	recs := make([]record, n)
	for i := range recs {
		recs[i] = record{
			ID:    uint32(i),
			Name:  "/foo/bar",
			Count: uint64(i) * 1_000_003,
			// pre-rounded so every encoder produces identical output.
			Ratio: math.Round(float64(i)*0.123457*1e6) / 1e6,
		}
	}
	return recs
}

func encodeRecords(enc *Encoder, recs []record) {
	enc.ArrayStart()
	for i, r := range recs {
		if i > 0 {
			enc.Delim()
		}
		enc.ObjectStart()
		enc.WriteUint32Key(keyID, r.ID, true)
		enc.ObjectKey(keyName)
		enc.EncodeKey([]byte(r.Name))
		enc.Delim()
		enc.WriteUint64Key(keyCount, r.Count, true)
		enc.WriteFloat64Key(keyRatio, r.Ratio, false)
		enc.ObjectEnd()
	}
	enc.ArrayEnd()
}

func encodeRecordsJSONText(enc *jsontext.Encoder, recs []record) error {
	if err := enc.WriteToken(jsontext.BeginArray); err != nil {
		return err
	}
	for _, r := range recs {
		for _, tok := range [...]jsontext.Token{
			jsontext.BeginObject,
			jsontext.String("id"), jsontext.Uint(uint64(r.ID)),
			jsontext.String("name"), jsontext.String(r.Name),
			jsontext.String("count"), jsontext.Uint(r.Count),
			jsontext.String("ratio"), jsontext.Float(r.Ratio),
			jsontext.EndObject,
		} {
			if err := enc.WriteToken(tok); err != nil {
				return err
			}
		}
	}
	return enc.WriteToken(jsontext.EndArray)
}

func TestCompareOutputs(t *testing.T) {
	recs := makeRecords(smallDoc)

	enc := GetEncoder(nil)
	defer enc.Release()
	encodeRecords(enc, recs)
	want := string(enc.Bytes())

	var buf bytes.Buffer
	assert.NoError(t, encodeRecordsJSONText(jsontext.NewEncoder(&buf), recs))
	assert.Equal(t, want, string(bytes.TrimSpace(buf.Bytes())), "jsontext")

	v1, err := json.Marshal(recs)
	assert.NoError(t, err)
	assert.Equal(t, want, string(v1), "encoding/json")

	v2, err := jsonv2.Marshal(recs)
	assert.NoError(t, err)
	assert.Equal(t, want, string(v2), "encoding/json/v2")
}

// BenchmarkCompareMemory encodes a small document into a reused buffer.
func BenchmarkCompareMemory(b *testing.B) {
	recs := makeRecords(smallDoc)

	b.Run("Encoder", func(b *testing.B) {
		enc := GetEncoder(nil)
		defer enc.Release()
		for b.Loop() {
			enc.b.Reset()
			encodeRecords(enc, recs)
		}
		b.SetBytes(int64(enc.Len()))
	})

	b.Run("jsontext", func(b *testing.B) {
		var buf bytes.Buffer
		enc := jsontext.NewEncoder(&buf)
		for b.Loop() {
			buf.Reset()
			enc.Reset(&buf)
			if err := encodeRecordsJSONText(enc, recs); err != nil {
				b.Fatal(err)
			}
		}
		b.SetBytes(int64(buf.Len()))
	})

	b.Run("json/v2", func(b *testing.B) {
		var buf bytes.Buffer
		for b.Loop() {
			buf.Reset()
			if err := jsonv2.MarshalWrite(&buf, recs); err != nil {
				b.Fatal(err)
			}
		}
		b.SetBytes(int64(buf.Len()))
	})

	b.Run("json/v1", func(b *testing.B) {
		var n int
		for b.Loop() {
			out, err := json.Marshal(recs)
			if err != nil {
				b.Fatal(err)
			}
			n = len(out)
		}
		b.SetBytes(int64(n))
	})
}

// streamTo runs encode against a fresh pipe drained by a concurrent reader,
// mirroring a chunked HTTP response body. It returns the bytes received.
// encode must close the writer.
func streamTo(encode func(w *io.PipeWriter)) int64 {
	r, w := io.Pipe()
	done := make(chan int64)
	go func() {
		n, _ := io.Copy(io.Discard, r)
		done <- n
	}()
	encode(w)
	return <-done
}

// BenchmarkCompareStream encodes a large document through an io.Pipe.
func BenchmarkCompareStream(b *testing.B) {
	recs := makeRecords(largeDoc)

	b.Run("Encoder", func(b *testing.B) {
		var n int64
		for b.Loop() {
			n = streamTo(func(w *io.PipeWriter) {
				enc := GetEncoder(w)
				encodeRecords(enc, recs)
				enc.Close()
				enc.Release()
			})
		}
		b.SetBytes(n)
	})

	b.Run("jsontext", func(b *testing.B) {
		var n int64
		enc := jsontext.NewEncoder(io.Discard)
		for b.Loop() {
			n = streamTo(func(w *io.PipeWriter) {
				enc.Reset(w)
				if err := encodeRecordsJSONText(enc, recs); err != nil {
					b.Error(err)
				}
				w.Close()
			})
		}
		b.SetBytes(n)
	})

	b.Run("json/v2", func(b *testing.B) {
		var n int64
		for b.Loop() {
			n = streamTo(func(w *io.PipeWriter) {
				if err := jsonv2.MarshalWrite(w, recs); err != nil {
					b.Error(err)
				}
				w.Close()
			})
		}
		b.SetBytes(n)
	})

	b.Run("json/v1", func(b *testing.B) {
		var n int64
		for b.Loop() {
			n = streamTo(func(w *io.PipeWriter) {
				if err := json.NewEncoder(w).Encode(recs); err != nil {
					b.Error(err)
				}
				w.Close()
			})
		}
		b.SetBytes(n)
	})
}
