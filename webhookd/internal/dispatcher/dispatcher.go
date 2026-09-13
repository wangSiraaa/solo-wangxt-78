// Package dispatcher claims pending deliveries from PostgreSQL and performs
// the signed HTTP POST to each endpoint. All scheduling state lives in the
// database; the dispatcher itself is stateless and horizontally scalable.
package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/example/webhookd/internal/retry"
	"github.com/example/webhookd/internal/signature"
	"github.com/example/webhookd/internal/ssrf"
	"github.com/example/webhookd/internal/store"
)

// Envelope is the JSON body delivered to endpoints. The event id in "id" is
// the stable event identity; it is also sent as the Webhook-Id header.
type Envelope struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	CreatedAt time.Time       `json:"created_at"`
	Data      json.RawMessage `json:"data"`
}

type Dispatcher struct {
	store        *store.Store
	client       *http.Client
	workers      int
	pollInterval time.Duration
	staleAfter   time.Duration
}

func New(st *store.Store, guard *ssrf.Guard, workers int, pollInterval time.Duration) *Dispatcher {
	if workers <= 0 {
		workers = 4
	}
	if pollInterval <= 0 {
		pollInterval = 500 * time.Millisecond
	}
	transport := &http.Transport{
		// SSRF guard: re-validates the resolved IP at connect time.
		DialContext:           guard.DialContext(&net.Dialer{Timeout: 10 * time.Second}),
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	return &Dispatcher{
		store:   st,
		workers: workers,
		client: &http.Client{
			Transport: transport,
			// Never follow redirects: a 3xx is reported as a delivery
			// failure instead of silently bouncing to an unvalidated host.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		pollInterval: pollInterval,
		staleAfter:   2 * time.Minute,
	}
}

// Run polls the database until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	tick := time.NewTicker(d.pollInterval)
	defer tick.Stop()
	reaper := time.NewTicker(30 * time.Second)
	defer reaper.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			d.dispatchOnce(ctx)
		case <-reaper.C:
			if n, err := d.store.RequeueStale(ctx, d.staleAfter); err == nil && n > 0 {
				log.Printf("dispatcher: requeued %d stale deliveries", n)
			}
		}
	}
}

func (d *Dispatcher) dispatchOnce(ctx context.Context) {
	claimed, err := d.store.ClaimDeliveries(ctx, d.workers*2)
	if err != nil {
		log.Printf("dispatcher: claim: %v", err)
		return
	}
	var wg sync.WaitGroup
	sem := make(chan struct{}, d.workers)
	for _, c := range claimed {
		wg.Add(1)
		sem <- struct{}{}
		go func(c store.ClaimedDelivery) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := d.deliver(ctx, c); err != nil {
				log.Printf("dispatcher: deliver %s: %v", c.Delivery.ID, err)
			}
		}(c)
	}
	wg.Wait()
}

// deliver performs one attempt and records the outcome.
func (d *Dispatcher) deliver(ctx context.Context, c store.ClaimedDelivery) error {
	ep := c.Endpoint
	attemptNo := c.Delivery.AttemptCount + 1

	body, err := json.Marshal(Envelope{
		ID:        c.Event.ID,
		Type:      c.Event.EventType,
		CreatedAt: c.Event.CreatedAt.UTC(),
		Data:      c.Event.Payload,
	})
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(ep.HTTPTimeoutMs)*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, ep.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	ts := time.Now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhookd/1.0")
	// Event identity + signature headers. The event id never changes across
	// retries, so receivers can deduplicate on it.
	req.Header.Set(signature.HeaderID, c.Event.ID)
	req.Header.Set(signature.HeaderTimestamp, fmt.Sprintf("%d", ts))
	req.Header.Set(signature.HeaderSignature, signature.SignHeader(ep.Secret, ts, body))

	start := time.Now()
	resp, doErr := d.client.Do(req)
	durationMs := int(time.Since(start).Milliseconds())

	var (
		statusCode *int
		respHint   time.Duration
		hasHint    bool
		errMsg     string
	)
	if doErr != nil {
		// Timeout / reset / refused: the request MAY have been processed
		// (response lost). Retrying is safe because the event id is stable
		// and receivers deduplicate on it.
		errMsg = truncate(doErr.Error(), 500)
	} else {
		code := resp.StatusCode
		statusCode = &code
		if code == http.StatusTooManyRequests {
			respHint, hasHint = retry.RetryAfter(resp.Header, time.Now())
		}
		if code < 200 || code > 299 {
			errMsg = truncate(readSnippet(resp), 500)
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)) //nolint:errcheck
		resp.Body.Close()                                     //nolint:errcheck
	}

	policy := retry.Policy{
		MaxAttempts: ep.MaxAttempts,
		BaseDelay:   time.Duration(ep.BackoffBaseMs) * time.Millisecond,
		MaxDelay:    time.Duration(ep.BackoffMaxMs) * time.Millisecond,
	}
	outcome, nextAt := classify(statusCode, errMsg, attemptNo, policy, respHint, hasHint)

	return d.store.RecordAttempt(ctx, store.RecordAttemptParams{
		DeliveryID:    c.Delivery.ID,
		AttemptNo:     attemptNo,
		StatusCode:    statusCode,
		Err:           errMsg,
		DurationMs:    durationMs,
		Outcome:       outcome,
		NextAttemptAt: nextAt,
	})
}

// classify maps an attempt result to the next delivery state.
func classify(statusCode *int, errMsg string, attemptNo int, policy retry.Policy,
	hint time.Duration, hasHint bool) (store.Outcome, time.Time) {

	retryable := false
	switch {
	case statusCode == nil:
		retryable = true // transport failure: timeout, reset, refused, DNS
	case *statusCode >= 200 && *statusCode <= 299:
		return store.OutcomeSucceeded, time.Now()
	case *statusCode == http.StatusTooManyRequests:
		retryable = true // 429: honor Retry-After below
	case *statusCode >= 500:
		retryable = true // 5xx: receiver-side problem, try again later
	default:
		retryable = false // 3xx (redirects refused) and other 4xx: dead-letter
	}

	if !retryable {
		return store.OutcomeDead, time.Now()
	}
	if attemptNo >= policy.MaxAttempts {
		return store.OutcomeDead, time.Now()
	}
	delay := policy.Delay(attemptNo, rand.Float64)
	if hasHint {
		delay = hint
		if delay > policy.MaxDelay {
			delay = policy.MaxDelay
		}
	}
	return store.OutcomeRetry, time.Now().Add(delay)
}

func readSnippet(resp *http.Response) string {
	b, err := io.ReadAll(io.LimitReader(resp.Body, 512))
	if err != nil {
		return ""
	}
	return fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(b))
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
