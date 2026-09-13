package retry

import (
	"net/http"
	"testing"
	"time"
)

func TestDelayExponentialWithCap(t *testing.T) {
	p := Policy{MaxAttempts: 10, BaseDelay: time.Second, MaxDelay: 30 * time.Second}
	want := []time.Duration{1, 2, 4, 8, 16, 30, 30, 30}
	for n, w := range want {
		if got := p.Delay(n+1, nil); got != w*time.Second {
			t.Errorf("Delay(%d) = %v, want %v", n+1, got, w*time.Second)
		}
	}
}

func TestDelayJitterStaysInBand(t *testing.T) {
	p := Policy{MaxAttempts: 10, BaseDelay: 10 * time.Second, MaxDelay: time.Hour}
	for _, j := range []float64{0, 0.25, 0.5, 0.75, 0.999} {
		got := p.Delay(1, func() float64 { return j })
		if got < 8*time.Second || got > 12*time.Second {
			t.Errorf("Delay with jitter %v = %v, outside ±20%% band", j, got)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Now()
	h := http.Header{"Retry-After": []string{"3"}}
	if d, ok := RetryAfter(h, now); !ok || d != 3*time.Second {
		t.Errorf("delta-seconds: got %v,%v", d, ok)
	}
	h = http.Header{"Retry-After": []string{now.Add(5 * time.Second).UTC().Format(http.TimeFormat)}}
	if d, ok := RetryAfter(h, now); !ok || d < 4*time.Second || d > 6*time.Second {
		t.Errorf("http-date: got %v,%v", d, ok)
	}
	if _, ok := RetryAfter(http.Header{}, now); ok {
		t.Error("missing header: ok = true, want false")
	}
	h = http.Header{"Retry-After": []string{"not-a-date"}}
	if _, ok := RetryAfter(h, now); ok {
		t.Error("garbage header: ok = true, want false")
	}
}
