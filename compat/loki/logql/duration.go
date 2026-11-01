package logql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// durationUnits are the units LogQL accepts: Go's (ns, us/µs, ms, s, m, h) plus the
// Prometheus ones Loki also takes (d, w, y = 365d).
var durationUnits = map[string]int64{
	"ns": 1,
	"us": int64(time.Microsecond), "µs": int64(time.Microsecond), "μs": int64(time.Microsecond),
	"ms": int64(time.Millisecond),
	"s":  int64(time.Second),
	"m":  int64(time.Minute),
	"h":  int64(time.Hour),
	"d":  int64(24 * time.Hour),
	"w":  int64(7 * 24 * time.Hour),
	"y":  int64(365 * 24 * time.Hour),
}

// ParseDuration parses a duration as Loki does: one or more <number><unit> terms, such as
// "5m", "1h30m", "1d12h" or "1.5h". Loki tries Prometheus' parser (integer terms, units
// y w d h m s ms) and then Go's (fractions, ns us ms s m h); this accepts both forms,
// including their mixture. "0" is zero. A sign, an unknown unit or a value past the int64
// range of nanoseconds is an error — an overflow must never wrap into a negative or tiny
// duration that a caller then silently accepts.
func ParseDuration(s string) (time.Duration, error) {
	orig := s
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	if s == "0" {
		return 0, nil
	}
	var total int64
	for s != "" {
		i := 0
		for i < len(s) && (isDigit(s[i]) || s[i] == '.') {
			i++
		}
		num := s[:i]
		j := i
		for j < len(s) && !isDigit(s[j]) && s[j] != '.' {
			j++
		}
		unit := s[i:j]
		s = s[j:]
		scale, ok := durationUnits[unit]
		whole, frac, _ := strings.Cut(num, ".")
		if !ok || (whole == "" && frac == "") || strings.Contains(frac, ".") {
			return 0, fmt.Errorf("bad duration %q", orig)
		}
		var term int64
		if whole != "" {
			n, err := strconv.ParseInt(whole, 10, 64)
			if err != nil || n > math.MaxInt64/scale {
				return 0, fmt.Errorf("duration %q overflows", orig)
			}
			term = n * scale
		}
		if frac != "" {
			f, err := strconv.ParseFloat("0."+frac, 64)
			if err != nil {
				return 0, fmt.Errorf("bad duration %q", orig)
			}
			fracNs := int64(f * float64(scale)) // < scale
			if fracNs > math.MaxInt64-term {
				return 0, fmt.Errorf("duration %q overflows", orig)
			}
			term += fracNs
		}
		if term > math.MaxInt64-total {
			return 0, fmt.Errorf("duration %q overflows", orig)
		}
		total += term
	}
	return time.Duration(total), nil
}
