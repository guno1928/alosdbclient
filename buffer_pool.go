package alosdbclient

import (
	"bytes"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

type buffer struct {
	buf []byte
}

func (b *buffer) Bytes() []byte { return b.buf }

func (b *buffer) Reset() { b.buf = b.buf[:0] }

func (b *buffer) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *buffer) WriteByte(c byte) error {
	b.buf = append(b.buf, c)
	return nil
}

func (b *buffer) Len() int { return len(b.buf) }

func (b *buffer) Cap() int { return cap(b.buf) }

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &buffer{buf: make([]byte, 0, 512)}
	},
}

func getBuffer() *buffer {
	b := bufferPool.Get().(*buffer)
	b.Reset()
	return b
}

func putBuffer(b *buffer) {
	if b.Cap() < 64*1024 {
		b.Reset()
		bufferPool.Put(b)
	}
}

var encoderPool = sync.Pool{
	New: func() interface{} {
		buf := getBuffer()
		enc := msgpack.NewEncoder(buf)
		return &encoderWithBuffer{encoder: enc, buffer: buf}
	},
}

type encoderWithBuffer struct {
	encoder *msgpack.Encoder
	buffer  *buffer
}

func getEncoder() *encoderWithBuffer {
	eb := encoderPool.Get().(*encoderWithBuffer)
	eb.buffer.Reset()
	eb.encoder.ResetWriter(eb.buffer)
	return eb
}

func putEncoder(eb *encoderWithBuffer) {
	if eb.buffer.Cap() < 64*1024 {
		eb.buffer.Reset()
		encoderPool.Put(eb)
	}
}

type decoderWithReader struct {
	decoder *msgpack.Decoder
	reader  *bytes.Reader
}

func (d *decoderWithReader) decode(v interface{}) error {
	return d.decoder.Decode(v)
}

var decoderPool = sync.Pool{
	New: func() interface{} {
		r := bytes.NewReader(nil)
		return &decoderWithReader{
			decoder: msgpack.NewDecoder(r),
			reader:  r,
		}
	},
}

func getDecoder(data []byte) *decoderWithReader {
	dr := decoderPool.Get().(*decoderWithReader)
	dr.reader.Reset(data)
	dr.decoder.ResetReader(dr.reader)
	return dr
}

func putDecoder(dr *decoderWithReader) {
	decoderPool.Put(dr)
}

var respChanPool = sync.Pool{New: func() any { return make(chan *response, 1) }}
