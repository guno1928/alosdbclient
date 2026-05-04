package alosdbclient

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var readBufPool sync.Pool

func getNetBuf(size int) []byte {
	if bp := readBufPool.Get(); bp != nil {
		b := *bp.(*[]byte)
		if cap(b) >= size {
			return b[:size]
		}
	}
	return make([]byte, size)
}

func putNetBuf(b []byte) {
	if cap(b) == 0 {
		return
	}
	b = b[:cap(b)]
	readBufPool.Put(&b)
}

func writePrefixed(w io.Writer, writeBuf *[]byte, data []byte) error {
	needed := 4 + len(data)
	if cap(*writeBuf) < needed {
		*writeBuf = make([]byte, needed)
	}
	buf := (*writeBuf)[:needed]
	n := uint32(len(data))
	buf[0] = byte(n >> 24)
	buf[1] = byte(n >> 16)
	buf[2] = byte(n >> 8)
	buf[3] = byte(n)
	copy(buf[4:], data)
	return writeAll(w, buf)
}

type clientTransport interface {
	connect(addr string) error
	send(data []byte) ([]byte, error)
	sendAsync(data []byte) error
	close() error
}

type pooledTransport struct {
	transports []clientTransport
	counter    uint32
	mu         sync.Mutex
}

func newPooledTransport(serverAddr string, poolSize int, psk []byte) (clientTransport, error) {
	if poolSize <= 1 {
		transport := newTCPClientTransport()
		if tcp, ok := transport.(*tcpClientTransport); ok && psk != nil {
			tcp.psk = psk
		}
		if err := transport.connect(serverAddr); err != nil {
			clientLogf("newPooledTransport single connect FAILED: %v", err)
			return nil, err
		}
		clientLogf("newPooledTransport single transport created addr=%s", serverAddr)
		return transport, nil
	}

	transports := make([]clientTransport, poolSize)
	for i := 0; i < poolSize; i++ {
		transport := newTCPClientTransport()
		if tcp, ok := transport.(*tcpClientTransport); ok && psk != nil {
			tcp.psk = psk
		}
		if err := transport.connect(serverAddr); err != nil {
			for j := 0; j < i; j++ {
				transports[j].close()
			}
			clientLogf("newPooledTransport pool connect FAILED at index %d: %v", i, err)
			return nil, err
		}
		transports[i] = transport
	}

	clientLogf("newPooledTransport pool created size=%d addr=%s", poolSize, serverAddr)
	return &pooledTransport{
		transports: transports,
	}, nil
}

func (p *pooledTransport) connect(addr string) error {
	return nil
}

func (p *pooledTransport) send(data []byte) ([]byte, error) {
	p.mu.Lock()
	idx := p.counter % uint32(len(p.transports))
	p.counter++
	p.mu.Unlock()
	resp, err := p.transports[idx].send(data)
	if err != nil {
		clientLogf("pooledTransport.send poolIdx=%d FAILED: %v", idx, err)
	}
	return resp, err
}

func (p *pooledTransport) sendAsync(data []byte) error {
	p.mu.Lock()
	idx := p.counter % uint32(len(p.transports))
	p.counter++
	p.mu.Unlock()
	err := p.transports[idx].sendAsync(data)
	if err != nil {
		clientLogf("pooledTransport.sendAsync poolIdx=%d FAILED: %v", idx, err)
	}
	return err
}

func (p *pooledTransport) close() error {
	var lastErr error
	for i, t := range p.transports {
		if err := t.close(); err != nil {
			clientLogf("pooledTransport.close index=%d error: %v", i, err)
			lastErr = err
		}
	}
	return lastErr
}

func setTransportTimeout(transport clientTransport, timeout time.Duration) {
	switch t := transport.(type) {
	case *tcpClientTransport:
		t.timeout = timeout
	case *pooledTransport:
		for _, child := range t.transports {
			setTransportTimeout(child, timeout)
		}
	}
}

type tcpClientTransport struct {
	conn      net.Conn
	timeout   time.Duration
	mu        sync.Mutex
	writeBuf  []byte
	psk       []byte
	encCipher *connCipher
	decCipher *connCipher
	addr      string
	connected int32
	lastErr   error
}

func newTCPClientTransport() clientTransport {
	return &tcpClientTransport{
		timeout: defaultClientRequestTimeout,
	}
}

func (t *tcpClientTransport) connect(addr string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.connected == 1 && t.conn != nil {
		clientLogf("tcpTransport.connect ALREADY_CONNECTED addr=%s", addr)
		return nil
	}

	if t.conn != nil {
		clientLogf("tcpTransport.connect closing stale conn addr=%s", addr)
		t.conn.Close()
	}
	t.conn = nil
	t.encCipher = nil
	t.decCipher = nil
	atomic.StoreInt32(&t.connected, 0)

	clientLogf("tcpTransport.connect dialing addr=%s", addr)
	conn, err := net.DialTimeout("tcp4", addr, 10*time.Second)
	if err != nil {
		atomic.StoreInt32(&t.connected, 0)
		t.lastErr = err
		clientLogf("tcpTransport.connect DIAL_FAILED addr=%s err=%v", addr, err)
		return err
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetReadBuffer(256 * 1024)
		tcpConn.SetWriteBuffer(256 * 1024)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	t.conn = conn
	t.addr = addr
	clientLogf("tcpTransport.connect dial OK addr=%s local=%s remote=%s", addr, conn.LocalAddr(), conn.RemoteAddr())

	if t.psk != nil {
		clientLogf("tcpTransport.connect starting PSK handshake addr=%s", addr)
		enc, dec, err := performClientHandshake(conn, t.psk, 10*time.Second)
		if err != nil {
			conn.Close()
			t.conn = nil
			atomic.StoreInt32(&t.connected, 0)
			t.lastErr = err
			clientLogf("tcpTransport.connect PSK_HANDSHAKE_FAILED addr=%s err=%v", addr, err)
			return fmt.Errorf("encryption handshake failed: %w", err)
		}
		t.encCipher = enc
		t.decCipher = dec
		clientLogf("tcpTransport.connect PSK_HANDSHAKE_OK addr=%s", addr)
	}

	atomic.StoreInt32(&t.connected, 1)
	t.lastErr = nil
	clientLogf("tcpTransport.connect COMPLETE addr=%s connected=1", addr)
	return nil
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return false
}

func classifyNetworkError(err error) string {
	if err == nil {
		return "nil"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "net_timeout"
		}
		return "net_error"
	}
	if errors.Is(err, io.EOF) {
		return "io_eof"
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return "io_unexpected_eof"
	}
	return "other"
}

func (t *tcpClientTransport) resetConnection() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn != nil {
		clientLogf("tcpTransport.resetConnection closing conn addr=%s", t.addr)
		t.conn.Close()
	}
	t.conn = nil
	t.encCipher = nil
	t.decCipher = nil
	atomic.StoreInt32(&t.connected, 0)
	clientLogf("tcpTransport.resetConnection done addr=%s connected=0", t.addr)
}

func (t *tcpClientTransport) send(data []byte) ([]byte, error) {
	for attempt := 0; attempt < 3; attempt++ {
		clientLogf("tcpTransport.send START attempt=%d addr=%s dataLen=%d", attempt, t.addr, len(data))

		t.mu.Lock()

		if atomic.LoadInt32(&t.connected) == 0 || t.conn == nil {
			t.mu.Unlock()
			clientLogf("tcpTransport.send NOT_CONNECTED attempt=%d addr=%s connected=%d conn=%v", attempt, t.addr, atomic.LoadInt32(&t.connected), t.conn)
			if err := t.connect(t.addr); err != nil {
				clientLogf("tcpTransport.send CONNECT_FAILED attempt=%d addr=%s err=%v", attempt, t.addr, err)
				if attempt < 2 {
					clientLogf("tcpTransport.send CONNECT_FAILED retrying after backoff attempt=%d", attempt)
					time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
					continue
				}
				return nil, fmt.Errorf("send connect failed after %d attempts: %w", attempt+1, err)
			}
			clientLogf("tcpTransport.send CONNECT_OK attempt=%d addr=%s", attempt, t.addr)
			continue
		}

		clientLogf("tcpTransport.send WRITING attempt=%d addr=%s enc=%v", attempt, t.addr, t.encCipher != nil)
		if t.encCipher != nil {
			if err := t.encCipher.encryptAndWrite(t.conn, &t.writeBuf, data); err != nil {
				t.mu.Unlock()
				clientLogf("tcpTransport.send WRITE_ENCRYPT_FAILED attempt=%d addr=%s err=%v class=%s", attempt, t.addr, err, classifyNetworkError(err))
				if isNetworkError(err) && attempt < 2 {
					t.resetConnection()
					continue
				}
				return nil, err
			}
		} else {
			if err := writePrefixed(t.conn, &t.writeBuf, data); err != nil {
				t.mu.Unlock()
				clientLogf("tcpTransport.send WRITE_FAILED attempt=%d addr=%s err=%v class=%s", attempt, t.addr, err, classifyNetworkError(err))
				if isNetworkError(err) && attempt < 2 {
					t.resetConnection()
					continue
				}
				return nil, err
			}
		}

		clientLogf("tcpTransport.send READING attempt=%d addr=%s timeout=%v", attempt, t.addr, t.timeout)
		var respLenBuf [4]byte
		if err := t.conn.SetReadDeadline(time.Now().Add(t.timeout)); err != nil {
			t.mu.Unlock()
			clientLogf("tcpTransport.send SET_DEADLINE_FAILED attempt=%d addr=%s err=%v", attempt, t.addr, err)
			if isNetworkError(err) && attempt < 2 {
				t.resetConnection()
				continue
			}
			return nil, err
		}
		if _, err := io.ReadFull(t.conn, respLenBuf[:]); err != nil {
			t.mu.Unlock()
			clientLogf("tcpTransport.send READ_LEN_FAILED attempt=%d addr=%s err=%v class=%s", attempt, t.addr, err, classifyNetworkError(err))
			if isNetworkError(err) && attempt < 2 {
				t.resetConnection()
				continue
			}
			return nil, err
		}

		respLength := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])
		clientLogf("tcpTransport.send READ_LEN attempt=%d addr=%s respLength=%d", attempt, t.addr, respLength)
		if respLength > 50*1024*1024 {
			t.mu.Unlock()
			clientLogf("tcpTransport.send RESPONSE_TOO_LARGE attempt=%d addr=%s respLength=%d", attempt, t.addr, respLength)
			return nil, fmt.Errorf("response too large: %d", respLength)
		}

		resp := make([]byte, respLength)
		if _, err := io.ReadFull(t.conn, resp); err != nil {
			t.mu.Unlock()
			clientLogf("tcpTransport.send READ_BODY_FAILED attempt=%d addr=%s err=%v class=%s", attempt, t.addr, err, classifyNetworkError(err))
			if isNetworkError(err) && attempt < 2 {
				t.resetConnection()
				continue
			}
			return nil, err
		}

		clientLogf("tcpTransport.send READ_OK attempt=%d addr=%s respBytes=%d", attempt, t.addr, len(resp))
		t.mu.Unlock()

		if t.decCipher != nil {
			decrypted, err := t.decCipher.decryptFrame(resp)
			if err != nil {
				clientLogf("tcpTransport.send DECRYPT_FAILED attempt=%d addr=%s err=%v", attempt, t.addr, err)
				return nil, err
			}
			clientLogf("tcpTransport.send DECRYPT_OK attempt=%d addr=%s decryptedBytes=%d", attempt, t.addr, len(decrypted))
			return decrypted, nil
		}
		clientLogf("tcpTransport.send SUCCESS attempt=%d addr=%s", attempt, t.addr)
		return resp, nil
	}

	clientLogf("tcpTransport.send ALL_ATTEMPTS_FAILED addr=%s", t.addr)
	return nil, fmt.Errorf("send failed after 3 reconnect attempts")
}

func (t *tcpClientTransport) sendAsync(data []byte) error {
	for attempt := 0; attempt < 3; attempt++ {
		clientLogf("tcpTransport.sendAsync START attempt=%d addr=%s dataLen=%d", attempt, t.addr, len(data))

		t.mu.Lock()

		if atomic.LoadInt32(&t.connected) == 0 || t.conn == nil {
			t.mu.Unlock()
			clientLogf("tcpTransport.sendAsync NOT_CONNECTED attempt=%d addr=%s", attempt, t.addr)
			if err := t.connect(t.addr); err != nil {
				clientLogf("tcpTransport.sendAsync CONNECT_FAILED attempt=%d addr=%s err=%v", attempt, t.addr, err)
				if attempt < 2 {
					time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
					continue
				}
				return fmt.Errorf("sendAsync connect failed after %d attempts: %w", attempt+1, err)
			}
			clientLogf("tcpTransport.sendAsync CONNECT_OK attempt=%d addr=%s", attempt, t.addr)
			continue
		}

		clientLogf("tcpTransport.sendAsync WRITING attempt=%d addr=%s enc=%v", attempt, t.addr, t.encCipher != nil)
		var err error
		if t.encCipher != nil {
			err = t.encCipher.encryptAndWrite(t.conn, &t.writeBuf, data)
		} else {
			err = writePrefixed(t.conn, &t.writeBuf, data)
		}

		if err != nil {
			t.mu.Unlock()
			clientLogf("tcpTransport.sendAsync WRITE_FAILED attempt=%d addr=%s err=%v class=%s", attempt, t.addr, err, classifyNetworkError(err))
			if isNetworkError(err) && attempt < 2 {
				t.resetConnection()
				continue
			}
			return err
		}

		clientLogf("tcpTransport.sendAsync SUCCESS attempt=%d addr=%s", attempt, t.addr)
		t.mu.Unlock()
		return nil
	}

	clientLogf("tcpTransport.sendAsync ALL_ATTEMPTS_FAILED addr=%s", t.addr)
	return fmt.Errorf("sendAsync failed after 3 reconnect attempts")
}

func (t *tcpClientTransport) close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	atomic.StoreInt32(&t.connected, 0)
	if t.conn != nil {
		clientLogf("tcpTransport.close addr=%s", t.addr)
		err := t.conn.Close()
		t.conn = nil
		t.encCipher = nil
		t.decCipher = nil
		return err
	}
	return nil
}
