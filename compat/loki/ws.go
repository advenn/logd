package loki

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// A minimal server-side WebSocket (RFC 6455), just enough for live tail: the server writes
// unmasked text frames to the client and reads client frames only to detect close / answer
// pings. No third-party dependency — the surface a tail needs is small.

const (
	wsMagic         = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	maxWSFrame      = 1 << 20          // cap an inbound (client) data frame
	maxControlFrame = 125              // RFC 6455: control-frame payloads are ≤125 bytes
	wsWriteTimeout  = 10 * time.Second // bound a single frame write so a non-reading client can't pin the goroutine
)

type wsConn struct {
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer
	wmu  sync.Mutex // serializes frame writes: handleTail (data/keepalive) vs readLoop (pong)
}

// wsUpgrade performs the handshake and hijacks the connection. It returns an error WITHOUT
// writing a response if the request is not a valid WebSocket upgrade (so the caller can
// reply with a normal HTTP error).
func wsUpgrade(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") ||
		!headerHasToken(r.Header.Get("Connection"), "upgrade") {
		return nil, fmt.Errorf("not a websocket upgrade")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, fmt.Errorf("missing Sec-WebSocket-Key")
	}
	conn, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return nil, fmt.Errorf("hijack: %w", err)
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + wsAcceptKey(key) + "\r\n\r\n"
	if _, err := brw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := brw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, br: brw.Reader, bw: brw.Writer}, nil
}

// wsAcceptKey computes the Sec-WebSocket-Accept response value.
func wsAcceptKey(key string) string {
	h := sha1.New()
	io.WriteString(h, key+wsMagic)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func headerHasToken(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func (c *wsConn) writeText(payload []byte) error { return c.writeFrame(0x1, payload) }
func (c *wsConn) writePing() error               { return c.writeFrame(0x9, nil) }

// writeFrame writes a single final, unmasked frame (server→client frames are never
// masked). It holds wmu so the readLoop goroutine's pong writes cannot interleave with
// handleTail's data/keepalive writes on the shared buffer, and sets a write deadline so a
// non-reading client cannot pin the goroutine indefinitely.
func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.conn != nil {
		c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	}
	var hdr [10]byte
	hdr[0] = 0x80 | opcode // FIN + opcode
	n := len(payload)
	var hlen int
	switch {
	case n < 126:
		hdr[1] = byte(n)
		hlen = 2
	case n <= 0xFFFF:
		hdr[1] = 126
		binary.BigEndian.PutUint16(hdr[2:], uint16(n))
		hlen = 4
	default:
		hdr[1] = 127
		binary.BigEndian.PutUint64(hdr[2:], uint64(n))
		hlen = 10
	}
	if _, err := c.bw.Write(hdr[:hlen]); err != nil {
		return err
	}
	if _, err := c.bw.Write(payload); err != nil {
		return err
	}
	return c.bw.Flush()
}

// readLoop consumes client frames only to notice a close/EOF (closing done) and to answer
// pings with pongs. Client→server frames are masked.
func (c *wsConn) readLoop(done chan<- struct{}) {
	defer close(done)
	for {
		opcode, payload, err := c.readFrame()
		if err != nil {
			return
		}
		switch opcode {
		case 0x8: // close
			return
		case 0x9: // ping → pong
			if err := c.writeFrame(0xA, payload); err != nil {
				return
			}
		}
	}
}

func (c *wsConn) readFrame() (opcode byte, payload []byte, err error) {
	var h [2]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return 0, nil, err
	}
	opcode = h[0] & 0x0F
	masked := h[1]&0x80 != 0
	n := int(h[1] & 0x7F)
	switch n {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil, err
		}
		n = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return 0, nil, err
		}
		n = int(binary.BigEndian.Uint64(ext[:]))
	}
	if n < 0 || n > maxWSFrame {
		return 0, nil, fmt.Errorf("ws frame too large: %d", n)
	}
	// Control frames (opcode ≥ 0x8: close/ping/pong) must be ≤125 bytes (RFC 6455 §5.5);
	// reject an oversized one so a client can't force a large alloc + pong echo.
	if opcode >= 0x8 && n > maxControlFrame {
		return 0, nil, fmt.Errorf("ws control frame too large: %d", n)
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(c.br, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return opcode, payload, nil
}

func (c *wsConn) Close() error { return c.conn.Close() }
