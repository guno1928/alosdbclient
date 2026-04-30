package alosdbclient

import (
	"fmt"
	"io"
	"net"
	"sync"
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
			return nil, err
		}
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
			return nil, err
		}
		transports[i] = transport
	}

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
	return p.transports[idx].send(data)
}

func (p *pooledTransport) sendAsync(data []byte) error {
	p.mu.Lock()
	idx := p.counter % uint32(len(p.transports))
	p.counter++
	p.mu.Unlock()
	return p.transports[idx].sendAsync(data)
}

func (p *pooledTransport) close() error {
	var lastErr error
	for _, t := range p.transports {
		if err := t.close(); err != nil {
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
}

func newTCPClientTransport() clientTransport {
	return &tcpClientTransport{
		timeout: defaultClientRequestTimeout,
	}
}

func (t *tcpClientTransport) connect(addr string) error {
	conn, err := net.DialTimeout("tcp4", addr, 10*time.Second)
	if err != nil {
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

	if t.psk != nil {
		enc, dec, err := performClientHandshake(conn, t.psk, 10*time.Second)
		if err != nil {
			conn.Close()
			return fmt.Errorf("encryption handshake failed: %w", err)
		}
		t.encCipher = enc
		t.decCipher = dec
	}

	return nil
}

func (t *tcpClientTransport) send(data []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.encCipher != nil {
		if err := t.encCipher.encryptAndWrite(t.conn, &t.writeBuf, data); err != nil {
			return nil, err
		}
	} else {
		if err := writePrefixed(t.conn, &t.writeBuf, data); err != nil {
			return nil, err
		}
	}

	var respLenBuf [4]byte
	t.conn.SetReadDeadline(time.Now().Add(t.timeout))
	if _, err := io.ReadFull(t.conn, respLenBuf[:]); err != nil {
		return nil, err
	}

	respLength := uint32(respLenBuf[0])<<24 | uint32(respLenBuf[1])<<16 | uint32(respLenBuf[2])<<8 | uint32(respLenBuf[3])
	if respLength > 50*1024*1024 {
		return nil, fmt.Errorf("response too large: %d", respLength)
	}

	resp := make([]byte, respLength)
	if _, err := io.ReadFull(t.conn, resp); err != nil {
		return nil, err
	}

	if t.decCipher != nil {
		return t.decCipher.decryptFrame(resp)
	}
	return resp, nil
}

func (t *tcpClientTransport) sendAsync(data []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.encCipher != nil {
		return t.encCipher.encryptAndWrite(t.conn, &t.writeBuf, data)
	}
	return writePrefixed(t.conn, &t.writeBuf, data)
}

func (t *tcpClientTransport) close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}
