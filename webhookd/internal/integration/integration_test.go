// Package integration runs the full stack against an embedded PostgreSQL:
// API -> transactional outbox -> dispatcher -> local test receiver.
//
// The receiver is only reachable because the test explicitly allows
// 127.0.0.1/32 in the SSRF guard — mirroring how local tests must opt in.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/example/webhookd/internal/dispatcher"
	"github.com/example/webhookd/internal/httpapi"
	"github.com/example/webhookd/internal/ssrf"
	"github.com/example/webhookd/internal/store"
	"github.com/example/webhookd/internal/testkit"
)

const testSecret = "TEST_ONLY_WEBHOOK_INTEGRATION_SECRET"

type env struct {
	t      *testing.T
	store  *store.Store
	server *httptest.Server
	cancel context.CancelFunc
}

func setup(t *testing.T) *env {
	t.Helper()
	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(15433).
			Database("webhookd_test").
			Logger(io.Discard),
	)
	if err := pg.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	t.Cleanup(func() { pg.Stop() }) //nolint:errcheck

	ctx := context.Background()
	st, err := store.New(ctx, "postgres://postgres:postgres@localhost:15433/webhookd_test?sslmode=disable")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Explicitly allow ONLY the local test receiver.
	guard, err := ssrf.New([]string{"127.0.0.1/32"})
	if err != nil {
		t.Fatalf("guard: %v", err)
	}

	dispCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	go dispatcher.New(st, guard, 4, 25*time.Millisecond).Run(dispCtx)

	tk := testkit.NewReceiver()
	tk.SleepDuration = 1500 * time.Millisecond
	srv := httptest.NewServer(httpapi.NewServer(st, guard, tk).Handler())
	t.Cleanup(srv.Close)

	return &env{t: t, store: st, server: srv, cancel: cancel}
}

// --- helpers ---

func (e *env) do(method, path string, body any, headers map[string]string) (int, map[string]any) {
	e.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			e.t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.server.URL+path, rdr)
	if err != nil {
		e.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			e.t.Fatalf("response not JSON: %s", raw)
		}
	}
	return resp.StatusCode, out
}

func (e *env) createEndpoint(key, mode, eventType string, maxAttempts, timeoutMs int) string {
	e.t.Helper()
	url := fmt.Sprintf("%s/testkit/receive?secret=%s&key=%s", e.server.URL, testSecret, key)
	if mode != "" {
		url += "&mode=" + mode
	}
	code, resp := e.do(http.MethodPost, "/v1/endpoints", map[string]any{
		"url":               url,
		"secret":            testSecret,
		"subscribed_events": []string{eventType},
		"retry_policy": map[string]any{
			"max_attempts":    maxAttempts,
			"backoff_base_ms": 50,
			"backoff_max_ms":  500,
			"http_timeout_ms": timeoutMs,
		},
	}, nil)
	if code != http.StatusCreated {
		e.t.Fatalf("create endpoint: code=%d resp=%v", code, resp)
	}
	return resp["id"].(string)
}

func (e *env) publish(idemKey, eventType, bizKey string) (int, map[string]any) {
	e.t.Helper()
	return e.do(http.MethodPost, "/v1/events", map[string]any{
		"type": eventType,
		"data": map[string]any{"business_key": bizKey, "amount": 100},
	}, map[string]string{"Idempotency-Key": idemKey})
}

func (e *env) deliveryOf(eventID string) map[string]any {
	e.t.Helper()
	_, resp := e.do(http.MethodGet, "/v1/events/"+eventID, nil, nil)
	dels := resp["deliveries"].([]any)
	if len(dels) == 0 {
		e.t.Fatalf("no deliveries for event %s", eventID)
	}
	return dels[0].(map[string]any)
}

func (e *env) waitDeliveryStatus(eventID, want string) map[string]any {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		d := e.deliveryOf(eventID)
		if d["status"] == want {
			return d
		}
		time.Sleep(50 * time.Millisecond)
	}
	d := e.deliveryOf(eventID)
	e.t.Fatalf("delivery for event %s: status=%v, want %s (last_error=%v)", eventID, d["status"], want, d["last_error"])
	return nil
}

func (e *env) receiverState() map[string]any {
	e.t.Helper()
	_, st := e.do(http.MethodGet, "/testkit/state", nil, nil)
	return st
}

func (e *env) setMode(key, mode string) {
	e.t.Helper()
	code, resp := e.do(http.MethodPost, "/testkit/mode", map[string]any{"key": key, "mode": mode}, nil)
	if code != http.StatusOK {
		e.t.Fatalf("set mode: %d %v", code, resp)
	}
}

// --- tests ---

func TestEndToEnd(t *testing.T) {
	e := setup(t)

	t.Run("HappyPath", func(t *testing.T) {
		e.createEndpoint("happy", "ok", "t.happy", 5, 2000)
		code, resp := e.publish("happy-1", "t.happy", "order-happy")
		if code != http.StatusCreated {
			t.Fatalf("publish: %d %v", code, resp)
		}
		eventID := resp["event"].(map[string]any)["id"].(string)
		e.waitDeliveryStatus(eventID, "succeeded")

		st := e.receiverState()
		if got := st["deliveries"].(map[string]any)[eventID]; got != float64(1) {
			t.Errorf("receiver deliveries[%s] = %v, want 1", eventID, got)
		}
		if got := st["business_effects"].(map[string]any)["order-happy"]; got != float64(1) {
			t.Errorf("business_effects[order-happy] = %v, want 1", got)
		}
	})

	t.Run("IdempotentPublishKeepsIdentity", func(t *testing.T) {
		e.createEndpoint("idem", "ok", "t.idem", 5, 2000)
		_, first := e.publish("idem-1", "t.idem", "order-idem")
		eventID := first["event"].(map[string]any)["id"].(string)
		e.waitDeliveryStatus(eventID, "succeeded")

		code, second := e.publish("idem-1", "t.idem", "order-idem")
		if code != http.StatusOK || second["deduplicated"] != true {
			t.Fatalf("republish: code=%d resp=%v, want 200 deduplicated", code, second)
		}
		if second["event"].(map[string]any)["id"] != eventID {
			t.Errorf("republish changed event id: %v vs %v", second["event"], eventID)
		}
		time.Sleep(300 * time.Millisecond)
		st := e.receiverState()
		if got := st["deliveries"].(map[string]any)[eventID]; got != float64(1) {
			t.Errorf("event delivered %v times after idempotent republish, want 1", got)
		}
		if got := st["business_effects"].(map[string]any)["order-idem"]; got != float64(1) {
			t.Errorf("business applied %v times, want 1", got)
		}
	})

	t.Run("ServerErrorRetriesThenDead", func(t *testing.T) {
		e.createEndpoint("err", "fail500", "t.err", 3, 2000)
		_, resp := e.publish("err-1", "t.err", "order-err")
		eventID := resp["event"].(map[string]any)["id"].(string)
		d := e.waitDeliveryStatus(eventID, "dead")
		if got := d["attempt_count"]; got != float64(3) {
			t.Errorf("attempt_count = %v, want 3", got)
		}
		if got := d["last_status_code"]; got != float64(500) {
			t.Errorf("last_status_code = %v, want 500", got)
		}
		_, det := e.do(http.MethodGet, "/v1/deliveries/"+d["id"].(string), nil, nil)
		if got := len(det["attempts"].([]any)); got != 3 {
			t.Errorf("attempt log rows = %d, want 3", got)
		}
	})

	t.Run("RateLimitedHonorsRetryAfter", func(t *testing.T) {
		e.createEndpoint("rl", "", "t.rl", 5, 2000)
		e.setMode("rl", "flaky429:2") // two 429s (Retry-After: 0), then success
		_, resp := e.publish("rl-1", "t.rl", "order-rl")
		eventID := resp["event"].(map[string]any)["id"].(string)
		d := e.waitDeliveryStatus(eventID, "succeeded")
		if got := d["attempt_count"]; got != float64(3) {
			t.Errorf("attempt_count = %v, want 3 (2x429 + 1x200)", got)
		}
		st := e.receiverState()
		if got := st["business_effects"].(map[string]any)["order-rl"]; got != float64(1) {
			t.Errorf("business applied %v times, want 1", got)
		}
	})

	t.Run("ResponseLostDuplicateDeliveryNotDuplicateBusiness", func(t *testing.T) {
		// mode=drop:1: receiver PROCESSES the event then drops the connection
		// once. The dispatcher sees a transport error and retries with the
		// SAME event id; the receiver deduplicates on it.
		e.createEndpoint("drop", "", "t.drop", 5, 500)
		e.setMode("drop", "drop:1")
		_, resp := e.publish("drop-1", "t.drop", "order-drop")
		eventID := resp["event"].(map[string]any)["id"].(string)
		d := e.waitDeliveryStatus(eventID, "succeeded")
		if got := d["attempt_count"]; got != float64(2) {
			t.Errorf("attempt_count = %v, want 2 (drop + retry)", got)
		}
		_, det := e.do(http.MethodGet, "/v1/deliveries/"+d["id"].(string), nil, nil)
		attempts := det["attempts"].([]any)
		if attempts[0].(map[string]any)["status_code"] != nil {
			t.Errorf("first attempt should have no status code (connection dropped), got %v", attempts[0])
		}
		st := e.receiverState()
		if got := st["deliveries"].(map[string]any)[eventID]; got != float64(2) {
			t.Errorf("receiver saw %v deliveries, want 2 (duplicate delivery)", got)
		}
		if got := st["business_effects"].(map[string]any)["order-drop"]; got != float64(1) {
			t.Errorf("business applied %v times, want 1 (no duplicate business effect)", got)
		}
	})

	t.Run("RedirectNotFollowed", func(t *testing.T) {
		e.createEndpoint("redir", "redirect", "t.redir", 3, 2000)
		_, resp := e.publish("redir-1", "t.redir", "order-redir")
		eventID := resp["event"].(map[string]any)["id"].(string)
		d := e.waitDeliveryStatus(eventID, "dead")
		if got := d["last_status_code"]; got != float64(302) {
			t.Errorf("last_status_code = %v, want 302 (redirect refused, not followed)", got)
		}
	})

	t.Run("DeadLetterReplayKeepsOriginalEvent", func(t *testing.T) {
		e.createEndpoint("dlq", "", "t.dlq", 2, 2000)
		e.setMode("dlq", "fail500")
		_, resp := e.publish("dlq-1", "t.dlq", "order-dlq")
		eventID := resp["event"].(map[string]any)["id"].(string)
		d := e.waitDeliveryStatus(eventID, "dead")
		deliveryID := d["id"].(string)

		eventsBefore, err := e.store.CountEvents(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		// Fix the receiver, then replay the DEAD delivery.
		e.setMode("dlq", "ok")
		code, replay := e.do(http.MethodPost, "/v1/deliveries/"+deliveryID+"/replay", nil, nil)
		if code != http.StatusOK {
			t.Fatalf("replay: %d %v", code, replay)
		}
		if replay["delivery"].(map[string]any)["event_id"] != eventID {
			t.Errorf("replay changed event id")
		}
		e.waitDeliveryStatus(eventID, "succeeded")

		eventsAfter, err := e.store.CountEvents(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if eventsAfter != eventsBefore {
			t.Errorf("replay manufactured a new event: before=%d after=%d", eventsBefore, eventsAfter)
		}
		st := e.receiverState()
		if got := st["business_effects"].(map[string]any)["order-dlq"]; got != float64(1) {
			t.Errorf("business applied %v times after replay, want 1", got)
		}

		// Replaying a non-dead delivery is rejected.
		code, _ = e.do(http.MethodPost, "/v1/deliveries/"+deliveryID+"/replay", nil, nil)
		if code != http.StatusConflict {
			t.Errorf("replay of succeeded delivery: code=%d, want 409", code)
		}
	})

	t.Run("PublishRequiresIdempotencyKey", func(t *testing.T) {
		code, _ := e.do(http.MethodPost, "/v1/events", map[string]any{
			"type": "order.created", "data": map[string]any{},
		}, nil)
		if code != http.StatusBadRequest {
			t.Errorf("publish without Idempotency-Key: code=%d, want 400", code)
		}
	})
}

// TestSSRFAtRegistration uses a STRICT guard (no allowlist) to prove that
// local admin ports and internal reserved addresses cannot be registered.
func TestSSRFAtRegistration(t *testing.T) {
	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().Port(15434).Database("webhookd_ssrf").Logger(io.Discard))
	if err := pg.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	defer pg.Stop() //nolint:errcheck

	ctx := context.Background()
	st, err := store.New(ctx, "postgres://postgres:postgres@localhost:15434/webhookd_ssrf?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	guard, err := ssrf.New(nil) // no allowlist at all
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(httpapi.NewServer(st, guard, nil).Handler())
	defer srv.Close()

	blocked := []string{
		"http://127.0.0.1:8080/admin",          // local admin port
		"http://localhost:9090/metrics",        // local admin via name
		"http://10.0.0.8/hook",                 // RFC1918
		"http://192.168.1.1/hook",              // RFC1918
		"http://169.254.169.254/latest/meta-data", // cloud metadata
		"http://[::1]:8080/hook",               // IPv6 loopback
		"file:///etc/passwd",                   // non-http scheme
	}
	for _, u := range blocked {
		resp, err := http.Post(srv.URL+"/v1/endpoints", "application/json",
			bytes.NewReader(mustJSON(map[string]any{
				"url": u, "subscribed_events": []string{"order.created"},
			})))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("register %q: code=%d, want 400", u, resp.StatusCode)
		}
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
