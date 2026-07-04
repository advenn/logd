package loki

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
)

// The RFC 6455 §1.3 worked example.
func TestWSAcceptKey(t *testing.T) {
	got := wsAcceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	if want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="; got != want {
		t.Fatalf("accept key = %q, want %q", got, want)
	}
}

// A server text frame is FIN+text, unmasked, with a 7-bit length for small payloads.
func TestWSWriteTextFrame(t *testing.T) {
	var buf bytes.Buffer
	c := &wsConn{bw: bufio.NewWriter(&buf)}
	if err := c.writeText([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	got := buf.Bytes()
	want := []byte{0x81, 0x02, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Fatalf("frame = %v, want %v", got, want)
	}
}

// readFrame must unmask a client frame (client→server frames are always masked).
func TestWSReadMaskedFrame(t *testing.T) {
	mask := [4]byte{0x01, 0x02, 0x03, 0x04}
	payload := []byte("hi")
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	frame := []byte{0x81, 0x80 | byte(len(payload))}
	frame = append(frame, mask[:]...)
	frame = append(frame, masked...)

	c := &wsConn{br: bufio.NewReader(bytes.NewReader(frame))}
	op, got, err := c.readFrame()
	if err != nil {
		t.Fatal(err)
	}
	if op != 0x1 || string(got) != "hi" {
		t.Fatalf("readFrame = op %#x %q, want 0x1 \"hi\"", op, got)
	}
}

// A frame declaring an oversized payload is rejected, not allocated.
func TestWSReadFrameTooLarge(t *testing.T) {
	// 64-bit length header declaring 1 GiB.
	frame := []byte{0x81, 127, 0, 0, 0, 0, 0x40, 0, 0, 0}
	c := &wsConn{br: bufio.NewReader(bytes.NewReader(frame))}
	if _, _, err := c.readFrame(); err == nil {
		t.Fatal("expected an oversized-frame rejection")
	}
}

// A control frame (ping) larger than 125 bytes is rejected per RFC 6455.
func TestWSReadControlFrameTooLarge(t *testing.T) {
	// masked ping (opcode 0x9), 16-bit length 200.
	frame := []byte{0x89, 0x80 | 126, 0x00, 0xC8, 1, 2, 3, 4}
	frame = append(frame, make([]byte, 200)...)
	c := &wsConn{br: bufio.NewReader(bytes.NewReader(frame))}
	if _, _, err := c.readFrame(); err == nil {
		t.Fatal("expected an oversized control frame to be rejected")
	}
}

// Concurrent frame writes (as readLoop's pong races handleTail's data/keepalive) must be
// serialized — run under -race.
func TestWSConcurrentWritesSerialized(t *testing.T) {
	pr, pw := net.Pipe()
	defer pr.Close()
	defer pw.Close()
	c := &wsConn{conn: pw, bw: bufio.NewWriter(pw)}
	// Drain the reader so writes don't block on the synchronous pipe.
	go io.Copy(io.Discard, pr)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := c.writeText([]byte("frame")); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
}
