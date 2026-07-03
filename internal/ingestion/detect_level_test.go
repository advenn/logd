package ingestion

import (
	"testing"

	"github.com/advenn/logd/internal/storage"
)

func TestDetectLevel(t *testing.T) {
	cases := []struct {
		msg   string
		want  storage.LogLevel
		found bool
	}{
		{"logger=http level=info msg=request", storage.LogLevelInfo, true},
		{"logger=db level=error msg=timeout", storage.LogLevelError, true},
		{`t=2026 lvl="warn" msg=x`, storage.LogLevelWarn, true},
		{"severity: DEBUG something", storage.LogLevelDebug, true},
		{"plain line with no level token", 0, false},
		{"user level=admin not a real level", 0, false}, // not a known level → no match
	}
	for _, c := range cases {
		got, ok := detectLevel(c.msg)
		if ok != c.found {
			t.Errorf("detectLevel(%q) found=%v, want %v", c.msg, ok, c.found)
			continue
		}
		if ok && got != c.want {
			t.Errorf("detectLevel(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}
