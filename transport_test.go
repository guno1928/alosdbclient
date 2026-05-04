package alosdbclient

import (
	"errors"
	"io"
	"net"
	"testing"
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
