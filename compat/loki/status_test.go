package loki

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/advenn/logd/core/query"
)

// An engine error the query caused is a 400 however it is wrapped; any other is a 500. The
// matched-entry cap (20M entries) is too large to reach in an HTTP test, so its mapping is
// checked here.
func TestQueryErrorStatus(t *testing.T) {
	for _, c := range []struct {
		err  error
		want int
	}{
		{fmt.Errorf("%w (more than 20000000) — narrow the time range", query.ErrTooManyEntries), http.StatusBadRequest},
		{fmt.Errorf("%w: 12000 steps", query.ErrTooManySteps), http.StatusBadRequest},
		{fmt.Errorf("shard 2: %w", fmt.Errorf("%w: field %q", query.ErrInvalidPredicate, "x")), http.StatusBadRequest},
		{errors.New("reading segment: input/output error"), http.StatusInternalServerError},
	} {
		if got := queryErrorStatus(c.err); got != c.want {
			t.Errorf("queryErrorStatus(%v) = %d, want %d", c.err, got, c.want)
		}
	}
}
