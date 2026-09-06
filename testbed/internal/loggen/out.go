package loggen

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Sink accepts one formatted record at a time.
//
// A formatter produces the record payload with no terminator; framing belongs to the
// transport. stdout and files terminate with a newline, syslog over UDP sends one
// datagram, and syslog over TCP uses RFC 6587 octet counting.
//
// Nothing here buffers. That is deliberate: this lab measures how *agents* batch and
// flush, and a buffer on this side would put the generator's own timing into those
// measurements.
type Sink interface {
	WriteRecord(payload []byte) error
	Close() error
}

// NewSinks builds the sinks named by --out.
func NewSinks(specs []string, rotate Rotate) ([]Sink, error) {
	var sinks []Sink
	for _, spec := range specs {
		s, err := newSink(spec, rotate)
		if err != nil {
			CloseAll(sinks)
			return nil, err
		}
		sinks = append(sinks, s)
	}
	return sinks, nil
}

func newSink(spec string, rotate Rotate) (Sink, error) {
	switch {
	case spec == "stdout":
		return &writerSink{w: os.Stdout}, nil
	case strings.HasPrefix(spec, "file:"):
		return newFileSink(strings.TrimPrefix(spec, "file:"), rotate)
	case strings.HasPrefix(spec, "syslog+udp://"):
		return newSyslogSink("udp", strings.TrimPrefix(spec, "syslog+udp://"))
	case strings.HasPrefix(spec, "syslog+tcp://"):
		return newSyslogSink("tcp", strings.TrimPrefix(spec, "syslog+tcp://"))
	default:
		return nil, fmt.Errorf("unknown sink %q, want stdout, file:/path, syslog+udp://host:port or syslog+tcp://host:port", spec)
	}
}

// CloseAll closes every sink, ignoring errors from those already closed.
func CloseAll(sinks []Sink) {
	for _, s := range sinks {
		_ = s.Close()
	}
}

// writerSink writes to an already-open stream, such as stdout, which is what the
// container runtime captures.
type writerSink struct {
	mu sync.Mutex
	w  *os.File
}

func (s *writerSink) WriteRecord(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.w.Write(append(payload, '\n'))
	return err
}

// Close does not close stdout; the process owns it.
func (s *writerSink) Close() error { return nil }

// fileSink writes to a file on disk, optionally rotating it. Rotation matters here: it is
// what makes an agent follow a file across an inode change, and getting that wrong is a
// classic source of duplicated or dropped lines.
type fileSink struct {
	mu     sync.Mutex
	path   string
	rotate Rotate
	f      *os.File
	size   int64
}

func newFileSink(path string, rotate Rotate) (*fileSink, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create log dir %s: %w", dir, err)
		}
	}
	s := &fileSink{path: path, rotate: rotate}
	if err := s.open(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *fileSink) open() error {
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", s.path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat %s: %w", s.path, err)
	}
	s.f, s.size = f, info.Size()
	return nil
}

func (s *fileSink) WriteRecord(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := append(payload, '\n')
	if s.rotate.Enabled() && s.size+int64(len(buf)) > s.rotate.MaxBytes {
		if err := s.roll(); err != nil {
			return err
		}
	}
	n, err := s.f.Write(buf)
	s.size += int64(n)
	return err
}

// roll renames the current file out of the way and starts a new one, which is how
// logrotate behaves without copytruncate, and what changes the inode under a tailing
// agent.
func (s *fileSink) roll() error {
	if err := s.f.Close(); err != nil {
		return err
	}

	if s.rotate.Keep > 0 {
		// Drop the oldest, then shift each remaining file down one slot.
		oldest := fmt.Sprintf("%s.%d", s.path, s.rotate.Keep)
		if err := os.Remove(oldest); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", oldest, err)
		}
		for i := s.rotate.Keep - 1; i >= 1; i-- {
			from := fmt.Sprintf("%s.%d", s.path, i)
			to := fmt.Sprintf("%s.%d", s.path, i+1)
			if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("rename %s to %s: %w", from, to, err)
			}
		}
		if err := os.Rename(s.path, s.path+".1"); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("rename %s: %w", s.path, err)
		}
	} else if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove %s: %w", s.path, err)
	}

	return s.open()
}

func (s *fileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

// syslogSink sends records straight to the receiver's syslog port, with no agent in
// between. It is the control case: whatever arrives here was shaped by nothing but the
// transport.
type syslogSink struct {
	mu      sync.Mutex
	network string
	addr    string
	conn    net.Conn
}

func newSyslogSink(network, addr string) (*syslogSink, error) {
	s := &syslogSink{network: network, addr: addr}
	// A failed dial is not fatal: the receiver may still be starting. Records are
	// dropped until it answers, exactly as a real syslog client would drop them.
	if err := s.connect(); err != nil {
		return s, nil
	}
	return s, nil
}

func (s *syslogSink) connect() error {
	conn, err := net.DialTimeout(s.network, s.addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s %s: %w", s.network, s.addr, err)
	}
	s.conn = conn
	return nil
}

func (s *syslogSink) WriteRecord(payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil {
		if err := s.connect(); err != nil {
			return nil // stay quiet and keep trying; see comment in newSyslogSink
		}
	}

	framed := payload
	if s.network == "tcp" {
		// RFC 6587 octet counting: "<length> <message>". Unambiguous even when the
		// message itself contains newlines, which is why it is preferred over
		// non-transparent LF framing for anything structured.
		framed = append([]byte(fmt.Sprintf("%d ", len(payload))), payload...)
	}

	if _, err := s.conn.Write(framed); err != nil {
		// Drop the connection so the next record redials.
		_ = s.conn.Close()
		s.conn = nil
		return nil
	}
	return nil
}

func (s *syslogSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}
