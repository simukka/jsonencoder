# JSON Encoder
This JSON Encoder provides a lightweight encoding interface for marshalling data to JSON.
Optimized for efficiently encoding streaming data at scale:
* zero allocations.
* minimal GC pressure.
* supports streaming writes to the HTTP response body reader.
* supports pretty printing (with a cost).
* architected for high performance environments.

The purpose of the Encoder is to buffer writes straight to the underlying
network connection (net.Conn) with minimal memory and GC overhead.

We accomplish that by reducing type coercion, heap allocations, and utilizing
buffer pools. The tradeoff is with flexibility. The data structure being
encoded must be deterministic. For example, encoding data from memory to be
sent over the wire.

## Todo
* Clean up the code (It was written a few years ago). 
* Document the API.
* Provide examples of how to use.
* Provide benchmark data of other encoding libraries.

## Benchmarks (Intel(R) Core(TM) i7-7500U CPU @ 2.70GHz)
```
BenchmarkEncoderNewEncoder-4        	1000000000	         0.9329 ns/op	       0 B/op	       0 allocs/op
BenchmarkEncoderGetEncoder-4        	 3538009	       346.6 ns/op	     224 B/op	       3 allocs/op
BenchmarkEncoderWrite-4             	184341187	         7.014 ns/op	       2 B/op	       0 allocs/op
BenchmarkEncoderEncodeKey-4         	21662215	        49.88 ns/op	       0 B/op	       0 allocs/op
BenchmarkEncoderObjectKey-4         	19987021	        62.74 ns/op	       0 B/op	       0 allocs/op
BenchmarkEncoderWriteUint32Key-4    	 8557674	       157.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkEncoderWriteUint64Key-4    	 9304784	       146.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkEncoderWriteFloat64Key-4   	 4387395	       280.0 ns/op	       0 B/op	       0 allocs/op
BenchmarkEncoderPrettyPrint-4       	 3760394	       323.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkWriteUint32Timestamp/unix-4         	 3384517	       367.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkWriteUint32Timestamp/utc-4          	 3393334	       365.2 ns/op	       0 B/op	       0 allocs/op
```

## Comparison with the standard library (Go 1.27.3, Intel(R) Core(TM) i7-1260P)
`compare_test.go` encodes the same document (an array of `{"id","name","count","ratio"}`
records, verified byte-identical across encoders) with this Encoder, `encoding/json/jsontext`,
`encoding/json/v2` and `encoding/json`. jsontext and json/v2 are still behind an experiment
flag in Go 1.27, so the file only builds with it:
```
GOEXPERIMENT=jsonv2 go test -run=Compare -bench=Compare -benchmem
```
Memory: 32 records into a reused buffer. Stream: 10,000 records through an `io.Pipe`.
```
                         sec/op      B/op     allocs/op
CompareMemory/Encoder    19.50µ      0        0
CompareMemory/jsontext   10.83µ      0        0
CompareMemory/json/v2    9.267µ      48       2
CompareMemory/json/v1    9.494µ      2.0Ki    3
CompareStream/Encoder    5.995m      13.2Ki   15
CompareStream/jsontext   4.421m      618      6
CompareStream/json/v2    4.097m      672      8
CompareStream/json/v1    2.983m      9.1Ki    8
```

## Example (todo: improve this example)
```
 func ApiHandler(ctx *fasthttp.RequestCtx) {
	r, w := io.Pipe()
	enc := GetEncoder(w)

	// calling SetBodyStream enables chunked transfers.
	ctx.SetBodyStream(r, -1)

	go func(){
		// release the encoder back to the pool.
		defer enc.Release()
		// Pass the encoder to your function.
		FunkyEncoder(enc)
		// close the encoder after writing has finished.
		enc.Close()
	}()
 }

 func FunkyEncoder(enc *Encoder) {
 	enc.ObjectStart()
 	enc.WriteUint32Timestamp(...)
 	enc.WriteUint32Key(...)
 	enc.ObjectEnd()
 }
```