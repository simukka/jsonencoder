package encoder

import (
	"bytes"
	"context"
	"io"
	"log"
	"math"
	"strconv"
	"sync"
	"time"
)

var (
	quoteMark = byte('"')
	delim     = byte(',')
	colon     = byte(':')
	lBrace    = byte('{')
	rBrace    = byte('}')
	lBracket  = byte('[')
	rBracket  = byte(']')
	newLine   = byte('\n')
	space     = byte(' ')
	tab       = byte('\t')
	backslash = byte('\\')
)

const (
	ISO8601    = "2006-01-02T15:04:05"
	ISO8601u   = "2006-01-02T15:04:05+00"
	MAXBUFSIZE = 4096
	INDENT     = 4
	SPACE_MODE = 0
	TAB_MODE   = 1
	PRECISION  = 6
)

var (
	bufPool = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
	prettyPool = sync.Pool{
		New: func() any {
			return new(bytes.Buffer)
		},
	}
	encPool = sync.Pool{
		New: func() any {
			return NewEncoder()
		},
	}
)

func Logf(format string, v ...any) {
	log.Printf(format, v...)
}

type Encoder struct {
	b                      *bytes.Buffer
	n                      int64 // internal: number of bytes written.
	f                      int64 // internal: number of writes to the pipe.
	d                      int   // internal: pretty print depth.
	s                      bool  // internal: pretty print string.
	recoveredPanicsCounter int64
	encoderTimeoutsCounter int64
	w                      *io.PipeWriter // pipe writer
	c                      EncoderConfig
	ctx                    context.Context
	cancel                 context.CancelFunc
}

type EncoderConfig struct {
	Indent        int
	Logging       bool
	UTCTimestamps bool
	Round         bool
	Precision     int
	Pretty        bool
}

// NewEncoder initializes and returns a pointer to an Encoder.
func NewEncoder() *Encoder {
	return &Encoder{
		c: EncoderConfig{
			Indent:        SPACE_MODE,
			Logging:       true,
			Round:         true,
			Precision:     PRECISION,
			UTCTimestamps: false,
			Pretty:        false,
		},
	}
}

// GetEncoder returns (or creates if none exists) an Encoder from the pool.
// The given PipeWriter will be attached to the returned Encoder.
// To return the Encoder back to the pool, call Release().
func GetEncoder(w *io.PipeWriter) *Encoder {
	enc := encPool.Get().(*Encoder)

	// get a buffer from the pool
	enc.b = bufPool.Get().(*bytes.Buffer)
	enc.b.Reset()
	if enc.b.Cap() < MAXBUFSIZE {
		enc.b.Grow(MAXBUFSIZE - enc.b.Cap())
	}

	enc.ctx, enc.cancel = context.WithCancel(context.Background())
	enc.w = w
	return enc
}

// Reset the Encoder.
// All internal structs and pointers are zeroed.
func (enc *Encoder) Reset() {
	enc.w = nil
	enc.c.Reset()
	enc.s = false
	enc.d = 0
	enc.n = 0
	enc.f = 0
	enc.b.Reset()
	// garbage collect buffers that overflow MAXBUFSIZE.
	if enc.b.Cap() <= MAXBUFSIZE {
		bufPool.Put(enc.b)
	}
	enc.b = nil
}

// Reset the encoder config back to the defaults.
// todo: dry this up
func (c *EncoderConfig) Reset() {
	c.Indent = SPACE_MODE
	c.Logging = true
	c.Round = true
	c.UTCTimestamps = false
	c.Pretty = false
}

// Close the writer. Blocks until all writes have finished.
func (enc *Encoder) Close() {
	enc.Write()
	enc.cancel()
	enc.w.Close()
}

// Bytes returns the current buffer.
func (enc *Encoder) Bytes() []byte {
	return enc.b.Bytes()
}

// SetConfig sets the given config for the encoder.
func (enc *Encoder) SetConfig(config EncoderConfig) {
	enc.c = config
}

func (enc *Encoder) Done() <-chan struct{} {
	return enc.ctx.Done()
}

func (enc *Encoder) WithTimeout(timeout time.Duration) {
	go func() {
		defer func() {
			r := recover()
			if r != nil {
				enc.recoveredPanicsCounter++
				Logf("Enc: timeout: pipe is already closed.")
			}
		}()

		select {
		case <-time.After(timeout):
			enc.encoderTimeoutsCounter++
			Logf("Enc: timeout: %v", timeout)
			enc.cancel()
			enc.w.Close()
			return
		case <-enc.Done():
			return
		}
	}()
}

// flush will check the size of the buffer and if the size reaches MAXBUFSIZE,
// it will write it to the underlying io.PipeWriter.
func (enc *Encoder) flush() {
	if enc.Len() >= MAXBUFSIZE {
		enc.write()
	}
}

// Release the encoder back to the pool. Release needs to be called as a deferred
// method: defer enc.Release()
//
// Recovers any panics that occur during encoding. We don't want to crash the
// server if any panics occur. Panics can occur when a pipe is closed as we
// are writing to it, i.e. the client terminated the request or the server
// terminated the connection.
//
// Returns the number of writes to the pipe, the number of bytes written,
// and the buffer size in bytes.
func (enc *Encoder) Release() (int64, int64, int) {
	r := recover()
	if r != nil {
		// if TraceLogLevel() {
		// 	Logf(TRACEENCODERPANIC, r)
		// }
		enc.cancel()  // cancel the context to stop any writers.
		enc.w.Close() // close the writer to terminate the reader.
	}

	cap := enc.b.Cap()
	n := enc.n
	f := enc.f

	// if enc.c.Logging && TraceLogLevel() {
	// 	Logf(TRACEENCODERRELEASE, f, n, cap)
	// }

	enc.Reset()
	return f, n, cap
}

// Write the current encoder buffer to the pipe writer.
func (enc *Encoder) Write() {
	enc.write()
}

// write the current buffer to the io.PipeWriter. when the write has finished
// the buffer will be reset. if a Write to the PipeWriter fails, a panic will
// be thrown.
func (enc *Encoder) write() {
	if enc.b.Len() == 0 {
		return
	}

	select {
	case <-enc.Done():
		return
	default:
		if enc.c.Pretty {
			enc.PrettyPrint()
		}

		// write the buffer
		n, err := enc.w.Write(enc.b.Bytes())
		if err != nil {
			panic(err)
		}

		// reset the encoder buffer
		enc.b.Reset()
		enc.f++           // number of writes.
		enc.n += int64(n) // number of bytes written.
	}
}

// PrettyPrint will prettify the JSON in the encoder buffer.
// It expects the buffer to have escaped strings and valid JSON.
//
// However, the buffer does not need the entire response object.
// We are dependent upon the parent caller to provide a valid
// json structure.
func (enc *Encoder) PrettyPrint() {
	// a buffer to write the pretty print.
	buf := prettyPool.Get().(*bytes.Buffer)
	buf.Reset()

	if buf.Cap() <= MAXBUFSIZE {
		buf.Grow(MAXBUFSIZE - buf.Cap())
	}

	defer func() {
		// cleanup the buffer.
		buf.Reset()
		if buf.Cap() <= MAXBUFSIZE {
			prettyPool.Put(buf)
		}
	}()

	for {
		c, err := enc.b.ReadByte()

		if err != nil || c == 0 {
			break
		}

		// todo: this can still break if the string contains quotes
		if c == quoteMark {
			enc.s = !enc.s
		}

		// if writing a string, do nothing
		if enc.s {
			buf.WriteByte(c)
			continue
		}

		switch c {
		case lBrace, lBracket: // {, [
			enc.d++
			buf.WriteByte(c)
			enc.indentNewLine(buf)
		case rBrace, rBracket: // }, ]
			enc.d--
			enc.indentNewLine(buf)
			buf.WriteByte(c)
		case delim: // ,
			buf.WriteByte(c)
			enc.indentNewLine(buf)
		case colon: // ,
			buf.WriteByte(c)
			buf.WriteByte(space)
		case quoteMark:
			buf.WriteByte(c)
		default:
			buf.WriteByte(c)
		}
	}

	if buf.Len() > 0 {
		// ensure the encoder buffer is reset.
		enc.b.Reset()
		// write the pretty print buffer into the encoder buffer.
		buf.WriteTo(enc.b)
	}
}

func (enc *Encoder) indentNewLine(buf *bytes.Buffer) {
	buf.WriteByte(newLine)
	spaces := INDENT

	if enc.c.Indent != 0 {
		spaces = 1
	}

	for d := 0; d < enc.d*spaces; d++ {
		switch enc.c.Indent {
		case 1:
			buf.WriteByte(tab)
		default:
			buf.WriteByte(space)
		}
	}

}

// Len returns the current size of the remaining encoding buffer.
func (enc *Encoder) Len() int {
	return enc.b.Len()
}

// AppendByte adds a single byte to the buffer.
func (enc *Encoder) AppendByte(value byte) {
	select {
	case <-enc.Done():
		return
	default:
		enc.b.WriteByte(value)
	}
}

// AppendBytes adds a byte slice to the buffer.
func (enc *Encoder) AppendBytes(value []byte) {
	select {
	case <-enc.Done():
		return
	default:
		enc.b.Write(value)
		enc.flush()
	}
}

func (enc *Encoder) ObjectKey(value []byte) {
	enc.EncodeKey(value)
	enc.AppendByte(colon)
}

// Escape will check every byte in the slice for characters that need to be
// escaped.
//
// \ => "\\"
// \n => " "
func (enc *Encoder) Escape(value []byte) []byte {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	for _, c := range value {
		switch c {
		case newLine:
			buf.WriteByte(space)
		case backslash:
			buf.WriteByte(backslash)
			buf.WriteByte(backslash)
		default:
			buf.WriteByte(c)
		}
	}

	return buf.Bytes()
}

func (enc *Encoder) ObjectStart() {
	enc.AppendByte(lBrace)
}

func (enc *Encoder) ObjectEnd() {
	enc.AppendByte(rBrace)
}

func (enc *Encoder) ArrayStart() {
	enc.AppendByte(lBracket)
}

func (enc *Encoder) ArrayEnd() {
	enc.AppendByte(rBracket)
}

func (enc *Encoder) Delim() {
	enc.AppendByte(delim)
}

func (enc *Encoder) EncodeKey(value []byte) {
	enc.AppendByte(quoteMark)
	enc.AppendBytes(value)
	enc.AppendByte(quoteMark)
}

func (enc *Encoder) WriteUint32Key(key []byte, value uint32, append_delim bool) {
	enc.WriteUint64Key(key, uint64(value), append_delim)
}

func (enc *Encoder) WriteEncodedUint32Key(key []byte, value uint32, append_delim bool) {
	enc.WriteEncodedUint64Key(key, uint64(value), append_delim)
}

func (enc *Encoder) WriteUint64Key(key []byte, value uint64, append_delim bool) {
	enc.writeUint64Key(key, value, append_delim, false)
}

func (enc *Encoder) WriteEncodedUint64Key(key []byte, value uint64, append_delim bool) {
	enc.writeUint64Key(key, value, append_delim, true)
}

func (enc *Encoder) WriteFloat64Key(key []byte, value float64, append_delim bool) {
	enc.writeFloat64Key(key, value, append_delim, false)
}

func (enc *Encoder) WriteEncodedFloat64Key(key []byte, value float64, append_delim bool) {
	enc.writeFloat64Key(key, value, append_delim, true)
}

func (enc *Encoder) WriteUint32Timestamp(key []byte, value uint32, append_delim bool) {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufPool.Put(b)

	t := time.Unix(int64(value), 0)

	if enc.c.UTCTimestamps {
		b.Write(t.UTC().AppendFormat(b.Bytes(), ISO8601u))
	} else {
		b.Write(t.AppendFormat(b.Bytes(), ISO8601))
	}
	enc.ObjectKey(key)
	enc.AppendByte(quoteMark)
	enc.AppendBytes(b.Bytes())
	enc.AppendByte(quoteMark)
	b.Reset()

	if append_delim {
		enc.Delim()
	}
}

func (enc *Encoder) RoundFloat(value float64) float64 {
	s := math.Pow(10, float64(enc.c.Precision))
	return math.Floor(value*s+.5) / s
}

func (enc *Encoder) writeUint64Key(key []byte, value uint64, delim, encode bool) {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufPool.Put(b)

	b.Write(strconv.AppendUint(b.Bytes(), value, 10))
	enc.ObjectKey(key)
	if encode {
		enc.EncodeKey(b.Bytes())
	} else {
		enc.AppendBytes(b.Bytes())
	}
	b.Reset()

	if delim {
		enc.Delim()
	}
}

func (enc *Encoder) writeFloat64Key(key []byte, value float64, delim, encode bool) {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufPool.Put(b)

	if enc.c.Round {
		value = enc.RoundFloat(value)
	}

	b.Write(strconv.AppendFloat(b.Bytes(), value, 'f', -1, 64))
	enc.ObjectKey(key)
	if encode {
		enc.EncodeKey(b.Bytes())
	} else {
		enc.AppendBytes(b.Bytes())
	}
	b.Reset()

	if delim {
		enc.Delim()
	}
}
