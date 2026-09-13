// Package retry computes per-endpoint retry schedules.
package retry

import (
	"net/http"
	"strconv"
	"time"
)

// Policy is the per-endpoint retry configuration.
type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// Delay returns the wait before the next attempt after the n-th failure
// (n is 1-based). Exponential backoff: base * 2^(n-1), capped at MaxDelay,
// with +/-20% jitter when jitterFn is non-nil (jitterFn must return [0,1)).
func (p Policy) Delay(n int, jitterFn func() float64) time.Duration {
	if n < 1 {
		n = 1
	}
	d := p.BaseDelay
	for i := 1; i < n; i++ {
		d *= 2
		if d >= p.MaxDelay {
			d = p.MaxDelay
			break
		}
	}
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	if jitterFn != nil && d > 0 {
		// jitter in [-20%, +20%]
		j := (jitterFn() - 0.5) * 0.4
		d = time.Duration(float64(d) * (1 + j))
	}
	if d < 0 {
		d = 0
	}
	return d
}

// RetryAfter parses a Retry-After response header (delta-seconds or
// HTTP-date). ok is false when the header is absent or unparsable.
func RetryAfter(h http.Header, now time.Time) (d time.Duration, ok bool) {
	v := h.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			secs = 0
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		if t.Before(now) {
			return 0, true
		}
		return t.Sub(now), true
	}
	return 0, false
}
