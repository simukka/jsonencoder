package encoder

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncoderNewEncoder(t *testing.T) {
	assert.NotPanics(t, func() {
		NewEncoder()
	})
}

func BenchmarkEncoderNewEncoder(b *testing.B) {
	for b.Loop() {
		NewEncoder()
	}
}

func TestEncoderGetEncoder(t *testing.T) {
	_, w := io.Pipe()

	assert.NotPanics(t, func() {
		GetEncoder(w)
	})
}

func BenchmarkEncoderGetEncoder(b *testing.B) {
	_, w := io.Pipe()

	for b.Loop() {
		enc := GetEncoder(w)
		enc.Close()
		enc.Release()
	}
}

func TestEncoderRelease(t *testing.T) {
	t.Run("resets encoder", func(t *testing.T) {
		_, w := io.Pipe()
		enc := GetEncoder(w)
		enc.Release()
		assert.Nil(t, enc.w)
		assert.Nil(t, enc.b)
	})

	t.Run("remaining writes", func(t *testing.T) {
		r, w := io.Pipe()
		enc := GetEncoder(w)
		enc.AppendBytes(make([]byte, 10))

		// concurrent reader
		buffer := new(bytes.Buffer)

		go func() {
			i, err := buffer.ReadFrom(r)
			assert.Equal(t, 10, i)
			assert.Empty(t, err)
		}()

		enc.Release()
	})

	t.Run("recovers", func(t *testing.T) {
		r, w := io.Pipe()

		assert.NotPanics(t, func() {
			enc := GetEncoder(w)
			defer enc.Release()

			// write some data
			enc.AppendBytes(make([]byte, 10))
			// close the pipe reader
			r.Close()
			// write to the closed pipe
			enc.Write()
		})
	})
}

func TestEncoderClose(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		r, w := io.Pipe()
		enc := GetEncoder(w)
		defer enc.Release()

		go r.Close()
		assert.NotPanics(t, func() {
			enc.Close()
		})
	})

	t.Run("writes remaining", func(t *testing.T) {
		r, w := io.Pipe()
		enc := GetEncoder(w)
		enc.AppendBytes(make([]byte, 10))

		// concurrent reader
		buffer := new(bytes.Buffer)
		go buffer.ReadFrom(r)

		assert.Equal(t, 10, enc.Len())
		enc.Close()
		assert.Equal(t, 0, enc.Len())

		// pipe should be closed, any write/read will error
		_, err := r.Read([]byte{})
		assert.Error(t, err)
	})
}

func TestEncoderWrite(t *testing.T) {
	t.Run("no data", func(t *testing.T) {
		_, w := io.Pipe()
		enc := GetEncoder(w)
		defer enc.Release()

		enc.Write()
		assert.Equal(t, int64(0), enc.n)
		assert.Equal(t, int64(0), enc.f)
	})

	t.Run("panics on error", func(t *testing.T) {
		r, w := io.Pipe()
		enc := GetEncoder(w)
		defer enc.Release()
		enc.AppendByte(byte(1))
		r.Close()

		assert.Panics(t, func() {
			enc.Write()
		})
	})

	t.Run("canceled", func(t *testing.T) {
		r, w := io.Pipe()
		enc := GetEncoder(w)
		defer enc.Release()
		enc.AppendByte(byte(1))
		r.Close()
		enc.cancel()
		assert.NotPanics(t, func() {
			enc.write()
		})
	})
}

func BenchmarkEncoderWrite(b *testing.B) {
	r, w := io.Pipe()
	enc := GetEncoder(w)
	defer enc.Release()
	buffer := new(bytes.Buffer)
	v := []byte{1}

	go func() {
		for i := 0; i < b.N; i++ {
			buffer.ReadFrom(r)
			buffer.Reset()
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		enc.b.Write(v)
	}
}

func TestEncoderFlush(t *testing.T) {
	r, w := io.Pipe()
	enc := GetEncoder(w)
	defer enc.Release()
	len := MAXBUFSIZE
	buffer := new(bytes.Buffer)

	enc.flush()
	assert.Equal(t, int64(0), enc.n)
	assert.Equal(t, int64(0), enc.f)

	enc.b.Write(make([]byte, len))
	assert.Equal(t, len, enc.Len())

	go func() {
		i, err := buffer.ReadFrom(r)
		assert.Empty(t, err)
		assert.Equal(t, len, i)
	}()

	enc.flush()

	assert.Equal(t, int64(len), enc.n)
	assert.Equal(t, int64(1), enc.f)
}

func TestEncoderEncodeKey(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := []byte("/foo/bar")
	enc.EncodeKey(value)
	assert.Equal(t, `"/foo/bar"`, enc.b.String())
}

func BenchmarkEncoderEncodeKey(b *testing.B) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := []byte("/foo/bar")

	for b.Loop() {
		enc.b.Reset()
		enc.EncodeKey(value)
	}
}

func TestEncoderObjectKey(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := []byte("/foo/bar")
	enc.ObjectKey(value)
	assert.Equal(t, `"/foo/bar":`, enc.b.String())
}

func BenchmarkEncoderObjectKey(b *testing.B) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := []byte("/foo/bar")

	for b.Loop() {
		enc.b.Reset()
		enc.ObjectKey(value)
	}
}

func TestEncoderWriteUint32Key(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := uint32(473829)
	key := []byte("/foo/bar")
	enc.WriteUint32Key(key, value, true)
	assert.Equal(t, `"/foo/bar":473829,`, enc.b.String())

	enc.b.Reset()
	enc.WriteUint32Key(key, value, false)
	assert.Equal(t, `"/foo/bar":473829`, enc.b.String())
}

func TestEncoderWriteEncodedUint32Key(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := uint32(473829)
	key := []byte("/foo/bar")
	enc.WriteEncodedUint32Key(key, value, true)
	assert.Equal(t, `"/foo/bar":"473829",`, enc.b.String())

	enc.b.Reset()
	enc.WriteEncodedUint32Key(key, value, false)
	assert.Equal(t, `"/foo/bar":"473829"`, enc.b.String())
}

func BenchmarkEncoderWriteUint32Key(b *testing.B) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := uint32(473829)
	key := []byte("/foo/bar")

	for b.Loop() {
		enc.b.Reset()
		enc.WriteUint32Key(key, value, true)
	}
}

func TestEncoderWriteUint64Key(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := uint64(473829)
	key := []byte("/foo/bar")
	enc.WriteUint64Key(key, value, true)
	assert.Equal(t, `"/foo/bar":473829,`, enc.b.String())

	enc.b.Reset()
	enc.WriteUint64Key(key, value, false)
	assert.Equal(t, `"/foo/bar":473829`, enc.b.String())
}

func TestEncoderWriteEncodedUint64Key(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := uint64(473829)
	key := []byte("/foo/bar")
	enc.WriteEncodedUint64Key(key, value, true)
	assert.Equal(t, `"/foo/bar":"473829",`, enc.b.String())

	enc.b.Reset()
	enc.WriteEncodedUint64Key(key, value, false)
	assert.Equal(t, `"/foo/bar":"473829"`, enc.b.String())
}

func BenchmarkEncoderWriteUint64Key(b *testing.B) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := uint64(473829)
	key := []byte("/foo/bar")

	for b.Loop() {
		enc.b.Reset()
		enc.WriteUint64Key(key, value, true)
	}
}

func TestEncoderWriteFloat64Key(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := float64(0.473829)
	key := []byte("/foo/bar")
	enc.WriteFloat64Key(key, value, true)
	assert.Equal(t, `"/foo/bar":0.473829,`, enc.b.String())

	enc.b.Reset()
	enc.WriteFloat64Key(key, value, false)
	assert.Equal(t, `"/foo/bar":0.473829`, enc.b.String())
}

func TestEncoderWriteEncodedFloat64Key(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := float64(0.473829)
	key := []byte("/foo/bar")
	enc.WriteEncodedFloat64Key(key, value, true)
	assert.Equal(t, `"/foo/bar":"0.473829",`, enc.b.String())

	enc.b.Reset()
	enc.WriteEncodedFloat64Key(key, value, false)
	assert.Equal(t, `"/foo/bar":"0.473829"`, enc.b.String())
}

func BenchmarkEncoderWriteFloat64Key(b *testing.B) {
	enc := GetEncoder(nil)
	defer enc.Release()
	value := float64(0.473829)
	key := []byte("/foo/bar")

	for b.Loop() {
		enc.b.Reset()
		enc.WriteFloat64Key(key, value, true)
	}
}

func TestEncoderArrayStart(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	enc.ArrayStart()
	assert.Equal(t, "[", enc.b.String())

}

func TestEncoderArrayEnd(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	enc.ArrayEnd()
	assert.Equal(t, "]", enc.b.String())
}

func TestEncoderObjectStart(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	enc.ObjectStart()
	assert.Equal(t, "{", enc.b.String())
}

func TestEncoderObjectEnd(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	enc.ObjectEnd()
	assert.Equal(t, "}", enc.b.String())
}

func TestEncoderPrettyPrint(t *testing.T) {
	t.Run("empty buffer", func(t *testing.T) {
		enc := GetEncoder(nil)
		defer enc.Release()
		enc.PrettyPrint()
		assert.Equal(t, 0, enc.Len())
	})

	t.Run("empty array", func(t *testing.T) {
		enc := GetEncoder(nil)
		defer enc.Release()

		enc.ObjectStart()
		enc.ObjectKey([]byte("keys"))
		enc.ArrayStart()
		enc.ArrayEnd()
		enc.ObjectEnd()
		enc.PrettyPrint()
		assert.Equal(t, "{\n    \"keys\": [\n        \n    ]\n}", string(enc.Bytes()))
	})

	t.Run("delim", func(t *testing.T) {
		enc := GetEncoder(nil)
		defer enc.Release()

		enc.ObjectStart()
		enc.ObjectKey([]byte("foo"))
		enc.ArrayStart()
		enc.ArrayEnd()
		enc.Delim()
		enc.ObjectKey([]byte("bar"))
		enc.ArrayStart()
		enc.ArrayEnd()
		enc.ObjectEnd()
		enc.PrettyPrint()
		assert.Equal(t, "{\n    \"foo\": [\n        \n    ],\n    \"bar\": [\n        \n    ]\n}", string(enc.Bytes()))
	})

	t.Run("nested", func(t *testing.T) {
		enc := GetEncoder(nil)
		defer enc.Release()

		enc.ObjectStart()
		enc.ObjectKey([]byte("foo"))
		enc.ArrayStart()
		enc.ObjectStart()
		enc.WriteUint32Key([]byte("bar"), 0, true)
		enc.WriteUint32Key([]byte("baz"), 0, false)
		enc.ObjectEnd()
		enc.ArrayEnd()
		enc.ObjectEnd()

		enc.PrettyPrint()
		assert.Equal(t, "{\n    \"foo\": [\n        {\n            \"bar\": 0,\n            \"baz\": 0\n        }\n    ]\n}", string(enc.Bytes()))
	})

	t.Run("tab mode", func(t *testing.T) {
		enc := GetEncoder(nil)
		enc.SetConfig(EncoderConfig{Indent: TAB_MODE})
		defer enc.Release()

		enc.ObjectStart()
		enc.ObjectKey([]byte("keys"))
		enc.ArrayStart()
		enc.ArrayEnd()
		enc.ObjectEnd()
		enc.PrettyPrint()
		assert.Equal(t, "{\n\t\"keys\": [\n\t\t\n\t]\n}", string(enc.Bytes()))
	})

	t.Run("split buffer", func(t *testing.T) {
		enc := GetEncoder(nil)
		defer enc.Release()

		enc.AppendBytes([]byte(`"timestamp`))
		enc.PrettyPrint()
		assert.Equal(t, `"timestamp`, string(enc.Bytes()))
		enc.b.Reset()

		enc.AppendBytes([]byte(`":"1970-01-01T01:00:00"`))
		enc.PrettyPrint()
		assert.Equal(t, `": "1970-01-01T01:00:00"`, string(enc.Bytes()))
	})

	t.Run("split string", func(t *testing.T) {
		enc := GetEncoder(nil)
		defer enc.Release()

		enc.AppendBytes([]byte(`"foo{}[],:`))
		enc.PrettyPrint()
		assert.Equal(t, `"foo{}[],:`, string(enc.Bytes()))
		enc.b.Reset()

		enc.AppendBytes([]byte(`bar{}[],:"`))
		enc.PrettyPrint()
		assert.Equal(t, `bar{}[],:"`, string(enc.Bytes()))
	})

	t.Run("string", func(t *testing.T) {
		enc := GetEncoder(nil)
		defer enc.Release()
		enc.EncodeKey([]byte("a{}/string[]:,"))
		enc.PrettyPrint()
		assert.Equal(t, `"a{}/string[]:,"`, string(enc.Bytes()))
	})
}

func BenchmarkEncoderPrettyPrint(b *testing.B) {
	enc := GetEncoder(nil)
	key := []byte("foo")
	defer enc.Release()

	for b.Loop() {
		enc.ObjectStart()
		enc.ObjectKey(key)
		enc.ArrayStart()
		enc.ArrayEnd()
		enc.ObjectEnd()
		enc.PrettyPrint()
		enc.b.Reset()
	}
}

func TestEncoderWriteUint32Timestamp(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	ts := uint32(0)

	t.Run("local", func(t *testing.T) {
		enc.WriteUint32Timestamp([]byte("timestamp"), ts, false)
		str := len(enc.Bytes())
		assert.Equal(t, 33, str)
		assert.Equal(t, `"timestamp":"1970-01-01T01:00:00"`, string(enc.Bytes()))
		enc.b.Reset()
	})

	t.Run("utc", func(t *testing.T) {
		enc.c.UTCTimestamps = true
		enc.WriteUint32Timestamp([]byte("timestamp"), ts, false)
		str := len(enc.Bytes())
		assert.Equal(t, 36, str)
		assert.Equal(t, `"timestamp":"1970-01-01T00:00:00+00"`, string(enc.Bytes()))
		enc.c.UTCTimestamps = false
		enc.b.Reset()
	})

	t.Run("pretty local", func(t *testing.T) {
		enc.WriteUint32Timestamp([]byte("timestamp"), ts, false)
		enc.PrettyPrint()
		str := len(enc.Bytes())
		assert.Equal(t, 34, str)
		assert.Equal(t, `"timestamp": "1970-01-01T01:00:00"`, string(enc.Bytes()))
		enc.b.Reset()
	})

	t.Run("pretty utc", func(t *testing.T) {
		enc.c.UTCTimestamps = true
		enc.WriteUint32Timestamp([]byte("timestamp"), ts, false)
		enc.PrettyPrint()
		str := len(enc.Bytes())
		assert.Equal(t, 37, str)
		assert.Equal(t, `"timestamp": "1970-01-01T00:00:00+00"`, string(enc.Bytes()))
		enc.c.UTCTimestamps = false
		enc.b.Reset()
	})
}

func BenchmarkWriteUint32Timestamp(b *testing.B) {
	enc := GetEncoder(nil)
	defer enc.Release()
	ts := uint32(1612530332)

	b.Run("unix", func(b *testing.B) {
		for b.Loop() {
			enc.b.Reset()
			enc.WriteUint32Timestamp([]byte("timestamp"), ts, false)
		}
	})

	b.Run("utc", func(b *testing.B) {
		enc.c.UTCTimestamps = true
		for b.Loop() {
			enc.b.Reset()
			enc.WriteUint32Timestamp([]byte("timestamp"), ts, false)
		}
		enc.c.UTCTimestamps = false
	})
}

func TestEncoderEscape(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()

	t.Run("backslash", func(t *testing.T) {
		bad := `/\`
		good := `/\\`
		assert.Equal(t, []byte(good), enc.Escape([]byte(bad)))
	})

	t.Run("newlines", func(t *testing.T) {
		bad := "\n"
		good := " "
		assert.Equal(t, []byte(good), enc.Escape([]byte(bad)))
	})
}

func TestEncoderRoundFloat(t *testing.T) {
	enc := GetEncoder(nil)
	defer enc.Release()
	f := float64(0.00010732467532467535)
	assert.Equal(t, float64(0.000107), enc.RoundFloat(f))
}
