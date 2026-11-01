package loki_test

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// dialTail opens a WebSocket tail on {region="eu"} and returns the connection and a reader
// positioned after the handshake.
func dialTail(t *testing.T, ts *httptest.Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	host := strings.TrimPrefix(ts.URL, "http://")
	conn, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	req := "GET /loki/api/v1/tail?query=" + urlEncode(`{region="eu"}`) + " HTTP/1.1\r\nHost: " + host + "\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("handshake: %v", err)
		}
		if line == "\r\n" {
			return conn, br
		}
	}
}

// readServerFrame reads one unmasked server frame with a 7-bit length.
func readServerFrame(t *testing.T, conn net.Conn, br *bufio.Reader) (byte, []byte) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var hdr [2]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		t.Fatalf("read frame: %v (the server closed the socket without a Close frame)", err)
	}
	payload := make([]byte, hdr[1]&0x7F)
	if _, err := io.ReadFull(br, payload); err != nil {
		t.Fatal(err)
	}
	return hdr[0], payload
}

// A client's Close frame is answered with a Close frame echoing its status code, as
// RFC 6455 §5.5.1 requires; the server used to drop the socket without one.
func TestTailEchoesClientClose(t *testing.T) {
	srv, _ := setup(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	conn, br := dialTail(t, ts)

	payload := []byte{0x03, 0xE8, 'b', 'y', 'e'} // 1000 normal closure
	mask := [4]byte{1, 2, 3, 4}
	frame := []byte{0x88, 0x80 | byte(len(payload))}
	frame = append(frame, mask[:]...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	op, got := readServerFrame(t, conn, br)
	if op != 0x88 || len(got) < 2 || binary.BigEndian.Uint16(got) != 1000 {
		t.Fatalf("reply to Close: opcode byte %#x payload %v, want a Close (0x88) carrying 1000", op, got)
	}
}

// When the server ends a tail — here because the writer shut down — it says so with a
// Close frame (1001 going away) before closing the socket.
func TestTailSendsGoingAwayOnShutdown(t *testing.T) {
	srv, w := setup(t)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	conn, br := dialTail(t, ts)
	time.Sleep(150 * time.Millisecond) // let the subscription register
	w.Close()
	op, got := readServerFrame(t, conn, br)
	if op != 0x88 || len(got) < 2 || binary.BigEndian.Uint16(got) != 1001 {
		t.Fatalf("on shutdown: opcode byte %#x payload %v, want a Close (0x88) carrying 1001", op, got)
	}
}
