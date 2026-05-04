package alosdbclient

import (
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsNetworkError(t *testing.T) {
	if isNetworkError(nil) {
		t.Fatal("expected nil to not be a network error")
	}
	if !isNetworkError(io.EOF) {
		t.Fatal("expected io.EOF to be a network error")
	}
	if !isNetworkError(io.ErrUnexpectedEOF) {
		t.Fatal("expected ErrUnexpectedEOF to be a network error")
	}
	if !isNetworkError(&net.OpError{Op: "read", Err: errors.New("broken pipe")}) {
		t.Fatal("expected net.Error to be a network error")
	}
	if isNetworkError(errors.New("random error")) {
		t.Fatal("expected random error to not be a network error")
	}
}

func TestTCPTransportReconnection(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	requestCount := 0
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		for requestCount < 2 {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			for requestCount < 2 {
				var lenBuf [4]byte
				if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
					conn.Close()
					break
				}
				length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
				if length > 1024*1024 {
					conn.Close()
					break
				}
				payload := make([]byte, length)
				if _, err := io.ReadFull(conn, payload); err != nil {
					conn.Close()
					break
				}

				requestCount++

				if requestCount == 1 {
					conn.Close()
					break
				}

				resp := []byte{0x81, 0xa6, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0xc0}
				respLen := uint32(len(resp))
				respBuf := []byte{
					byte(respLen >> 24), byte(respLen >> 16), byte(respLen >> 8), byte(respLen),
				}
				respBuf = append(respBuf, resp...)
				if _, err := conn.Write(respBuf); err != nil {
					conn.Close()
					break
				}
			}
		}
	}()

	transport := newTCPClientTransport()
	if err := transport.connect(addr); err != nil {
		t.Fatalf("initial connect failed: %v", err)
	}
	defer transport.close()

	req := []byte{0x81, 0xa2, 0x6f, 0x70, 0x01}

	_, err = transport.send(req)
	if err == nil {
		t.Fatal("expected first send to fail after server closed connection")
	}

	_, err = transport.send(req)
	if err != nil {
		t.Fatalf("expected second send to succeed after reconnect, got: %v", err)
	}

	select {
	case <-serverDone:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not finish in time")
	}

	if requestCount != 2 {
		t.Fatalf("expected 2 requests, got %d", requestCount)
	}
}

func TestTCPTransportSendAsyncReconnection(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()
	var requestCount atomic.Int32
	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		for requestCount.Load() < 2 {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			for requestCount.Load() < 2 {
				var lenBuf [4]byte
				if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
					conn.Close()
					break
				}
				length := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
				if length > 1024*1024 {
					conn.Close()
					break
				}
				payload := make([]byte, length)
				if _, err := io.ReadFull(conn, payload); err != nil {
					conn.Close()
					break
				}

				requestCount.Add(1)
			}
			conn.Close()
		}
	}()

	transport := newTCPClientTransport().(*tcpClientTransport)
	if err := transport.connect(addr); err != nil {
		t.Fatalf("initial connect failed: %v", err)
	}
	defer transport.close()

	req := []byte{0x81, 0xa2, 0x6f, 0x70, 0x01}

	err = transport.sendAsync(req)
	if err != nil {
		t.Fatalf("expected first sendAsync to succeed, got: %v", err)
	}

	for requestCount.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	transport.resetConnection()

	err = transport.sendAsync(req)
	if err != nil {
		t.Fatalf("expected second sendAsync to succeed after manual disconnect, got: %v", err)
	}

	select {
	case <-serverDone:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not finish in time")
	}

	if requestCount.Load() != 2 {
		t.Fatalf("expected 2 requests, got %d", requestCount.Load())
	}
}
