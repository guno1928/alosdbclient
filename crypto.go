package alosdbclient

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/hkdf"
)

const (
	gcmTagSize       = 16
	counterSize      = 8
	frameOverhead    = counterSize + gcmTagSize
	saltSize         = 32
	proofSize        = 32
	handshakeMagic   = 0xAE
	handshakeVersion = 0x01
)

var (
	errFrameTooShort  = errors.New("encrypted frame too short")
	errReplayDetected = errors.New("replay detected: counter not monotonic")
	errBadProof       = errors.New("handshake proof verification failed")
)

type connCipher struct {
	aead         cipher.AEAD
	writeCounter atomic.Uint64
	readCounter  uint64
}

func newConnCipher(key [32]byte) (*connCipher, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &connCipher{aead: gcm}, nil
}

func (c *connCipher) encryptAndWrite(w io.Writer, writeBuf *[]byte, plaintext []byte) error {
	counter := c.writeCounter.Add(1)
	encSize := counterSize + len(plaintext) + gcmTagSize
	needed := 4 + encSize

	if cap(*writeBuf) < needed {
		*writeBuf = make([]byte, needed)
	}
	buf := (*writeBuf)[:4+counterSize]

	binary.BigEndian.PutUint32(buf[0:4], uint32(encSize))
	binary.BigEndian.PutUint64(buf[4:12], counter)

	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[4:], counter)

	sealed := c.aead.Seal(buf, nonce[:], plaintext, nil)
	return writeAll(w, sealed)
}

func (c *connCipher) decryptFrame(encData []byte) ([]byte, error) {
	if len(encData) < counterSize+gcmTagSize {
		return nil, errFrameTooShort
	}

	counter := binary.BigEndian.Uint64(encData[:counterSize])
	if counter <= c.readCounter {
		return nil, errReplayDetected
	}
	c.readCounter = counter

	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[4:], counter)

	plaintext, err := c.aead.Open(nil, nonce[:], encData[counterSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return plaintext, nil
}

func deriveKeys(psk, clientSalt, serverSalt []byte) (c2sKey, s2cKey, proofKey [32]byte, err error) {
	combined := make([]byte, 0, len(clientSalt)+len(serverSalt))
	combined = append(combined, clientSalt...)
	combined = append(combined, serverSalt...)

	extract := func(info string) ([32]byte, error) {
		var key [32]byte
		r := hkdf.New(sha256.New, psk, combined, []byte(info))
		if _, err := io.ReadFull(r, key[:]); err != nil {
			return key, err
		}
		return key, nil
	}

	c2sKey, err = extract("alosdb-c2s-v1")
	if err != nil {
		return
	}
	s2cKey, err = extract("alosdb-s2c-v1")
	if err != nil {
		return
	}
	proofKey, err = extract("alosdb-proof-v1")
	return
}

func computeProof(proofKey [32]byte, label string, clientSalt, serverSalt []byte) []byte {
	mac := hmac.New(sha256.New, proofKey[:])
	mac.Write([]byte(label))
	mac.Write(clientSalt)
	mac.Write(serverSalt)
	return mac.Sum(nil)
}

func derivePSK(username, password string) []byte {
	h := sha256.Sum256([]byte(username + ":" + password))
	psk := make([]byte, 32)
	copy(psk, h[:])
	return psk
}

func performClientHandshake(conn net.Conn, psk []byte, timeout time.Duration) (*connCipher, *connCipher, error) {
	conn.SetDeadline(time.Now().Add(timeout))
	defer conn.SetDeadline(time.Time{})

	var clientSalt [saltSize]byte
	if _, err := io.ReadFull(rand.Reader, clientSalt[:]); err != nil {
		return nil, nil, fmt.Errorf("generate client salt: %w", err)
	}

	var phase1 [2 + saltSize]byte
	phase1[0] = handshakeMagic
	phase1[1] = handshakeVersion
	copy(phase1[2:], clientSalt[:])
	if err := writeAll(conn, phase1[:]); err != nil {
		return nil, nil, fmt.Errorf("send phase1: %w", err)
	}

	var phase2 [saltSize + proofSize]byte
	if _, err := io.ReadFull(conn, phase2[:]); err != nil {
		return nil, nil, fmt.Errorf("read phase2: %w", err)
	}
	serverSalt := phase2[:saltSize]
	serverProof := phase2[saltSize:]

	c2sKey, s2cKey, proofKey, err := deriveKeys(psk, clientSalt[:], serverSalt)
	if err != nil {
		return nil, nil, fmt.Errorf("derive keys: %w", err)
	}

	expected := computeProof(proofKey, "server", clientSalt[:], serverSalt)
	if subtle.ConstantTimeCompare(serverProof, expected) != 1 {
		return nil, nil, errBadProof
	}

	clientProof := computeProof(proofKey, "client", clientSalt[:], serverSalt)
	if err := writeAll(conn, clientProof); err != nil {
		return nil, nil, fmt.Errorf("send phase3: %w", err)
	}

	enc, err := newConnCipher(c2sKey)
	if err != nil {
		return nil, nil, err
	}
	dec, err := newConnCipher(s2cKey)
	if err != nil {
		return nil, nil, err
	}
	return enc, dec, nil
}

func writeAll(w io.Writer, data []byte) error {
	_, err := w.Write(data)
	return err
}
