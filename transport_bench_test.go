package alosdbclient

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
)

func BenchmarkSendNoReconnect(b *testing.B) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	var requestCount atomic.Int32
	var totalBytes atomic.Int64
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var lenBuf [4]byte
			if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
				return
			}
			length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
			if length > 1024*1024 {
				return
			}
			payload := make([]byte, length)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
			requestCount.Add(1)
			totalBytes.Add(int64(length))

			resp := []byte{0x81, 0xa6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0xc0}
			respLen := uint32(len(resp))
			respBuf := []byte{
				byte(respLen >> 24), byte(respLen >> 16), byte(respLen >> 8), byte(respLen),
			}
			respBuf = append(respBuf, resp...)
			if _, err := conn.Write(respBuf); err != nil {
				return
			}
		}
	}()

	transport := newTCPClientTransport().(*tcpClientTransport)
	if err := transport.connect(addr); err != nil {
		b.Fatalf("initial connect failed: %v", err)
	}
	defer transport.close()

	req := []byte{0x81, 0xa2, 0x6f, 0x70, 0x01}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := transport.send(req)
		if err != nil {
			b.Fatalf("send failed: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkSendAsyncNoReconnect(b *testing.B) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	var requestCount atomic.Int32
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var lenBuf [4]byte
			if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
				return
			}
			length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
			if length > 1024*1024 {
				return
			}
			payload := make([]byte, length)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
			requestCount.Add(1)
		}
	}()

	transport := newTCPClientTransport().(*tcpClientTransport)
	if err := transport.connect(addr); err != nil {
		b.Fatalf("initial connect failed: %v", err)
	}
	defer transport.close()

	req := []byte{0x81, 0xa2, 0x6f, 0x70, 0x01}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := transport.sendAsync(req)
		if err != nil {
			b.Fatalf("sendAsync failed: %v", err)
		}
	}
	b.StopTimer()
}
