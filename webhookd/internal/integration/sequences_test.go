package integration

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/example/webhookd/internal/signature"
)

// --- additional helpers ---

func (e *env) createEndpointRaw(url, secret, eventType string, maxAttempts, timeoutMs int) string {
	e.t.Helper()
	code, resp := e.do(http.MethodPost, "/v1/endpoints", map[string]any{
		"url":               url,
		"secret":            secret,
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

func (e *env) migrateEndpoint(t *testing.T, id, newURL string) string {
	t.Helper()
	code, resp := e.do(http.MethodPost, "/v1/endpoints/"+id+"/migrate", map[string]any{"url": newURL}, nil)
	if code != http.StatusCreated {
		t.Fatalf("migrate: code=%d resp=%v", code, resp)
	}
	if resp["generation"].(float64) != 2 {
		t.Fatalf("generation = %v, want 2", resp["generation"])
	}
	return resp["id"].(string)
}

func (e *env) skipDelivery(id, reason string) (int, map[string]any) {
	e.t.Helper()
	return e.do(http.MethodPost, "/v1/deliveries/"+id+"/skip", map[string]any{
		"reason": reason, "resolved_by": "ops@example.com",
	}, nil)
}

func (e *env) setSecret(key, secret, prev string, prevExpiresUnix int64) {
	e.t.Helper()
	code, resp := e.do(http.MethodPost, "/testkit/secret", map[string]any{
		"key": key, "secret": secret,
		"previous_secret": prev, "previous_expires_unix": prevExpiresUnix,
	}, nil)
	if code != http.StatusOK {
		e.t.Fatalf("set secret: %d %v", code, resp)
	}
}

func (e *env) waitFor(msg string, cond func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("timeout waiting for: %s", msg)
}

func (e *env) processedSeqs(key string) []int64 {
	e.t.Helper()
	st := e.receiverState()
	seqs, _ := st["processed_sequences"].(map[string]any)[key].([]any)
	out := []int64{}
	for _, s := range seqs {
		out = append(out, int64(s.(float64)))
	}
	return out
}

func (e *env) attemptSeqs(key string) []int64 {
	e.t.Helper()
	st := e.receiverState()
	seqs, _ := st["attempt_sequences"].(map[string]any)[key].([]any)
	out := []int64{}
	for _, s := range seqs {
		out = append(out, int64(s.(float64)))
	}
	return out
}

// --- per-key ordering, parallelism, poison messages ---

// Same-key retries must span subsequent events: a retried earlier event
// holds back later events of the same key until it succeeds.
func TestPerKeyOrderingAndRetries(t *testing.T) {
	e := setup(t, 15435)
	e.createEndpoint("ord", "", "t.ord", 5, 2000)
	e.setMode("ord", "flaky:2") // seq 1 fails twice, then succeeds

	for i := 1; i <= 3; i++ {
		e.publish(fmt.Sprintf("ord-%d", i), "t.ord", "order-1")
	}
	e.waitFor("all 3 events processed", func() bool { return len(e.processedSeqs("order-1")) == 3 })

	if got, want := e.attemptSeqs("order-1"), []int64{1, 1, 1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("attempt sequence = %v, want %v (retries of seq 1 must precede seq 2,3)", got, want)
	}
	if got, want := e.processedSeqs("order-1"), []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Errorf("processed sequence = %v, want %v", got, want)
	}
}

// Different business keys are delivered in parallel; within one key the
// receiver never sees more than one in-flight request.
func TestParallelAcrossKeys(t *testing.T) {
	e := setup(t, 15436)
	e.createEndpoint("par", "slow", "t.par", 5, 2000) // slow = 100ms processing

	keys := []string{"k1", "k2", "k3", "k4"}
	for i := 1; i <= 3; i++ {
		for _, k := range keys {
			e.publish(fmt.Sprintf("par-%s-%d", k, i), "t.par", k)
		}
	}
	e.waitFor("all 12 events processed", func() bool {
		total := 0
		for _, k := range keys {
			total += len(e.processedSeqs(k))
		}
		return total == 12
	})

	st := e.receiverState()
	if got := st["max_concurrent_per_key"].(float64); got != 1 {
		t.Errorf("max_concurrent_per_key = %v, want 1 (serial within a key)", got)
	}
	if got := st["max_concurrent_total"].(float64); got < 2 {
		t.Errorf("max_concurrent_total = %v, want >= 2 (parallel across keys)", got)
	}
	for _, k := range keys {
		if got, want := e.processedSeqs(k), []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
			t.Errorf("key %s processed = %v, want %v", k, got, want)
		}
	}
}

// A poison message blocks only its own business key. Skipping it requires a
// human decision with a reason; the skipped seq stays a visible gap.
func TestPoisonMessageSkipAndGaps(t *testing.T) {
	e := setup(t, 15437)
	e.createEndpoint("poison", "", "t.poison", 2, 2000)
	e.createEndpoint("fine", "ok", "t.fine", 2, 2000)
	e.setMode("poison", "fail500")

	_, r1 := e.publish("poi-1", "t.poison", "key-A")
	a1 := r1["event"].(map[string]any)["id"].(string)
	e.publish("poi-2", "t.poison", "key-A")
	_, rb := e.publish("fine-1", "t.fine", "key-B")
	b1 := rb["event"].(map[string]any)["id"].(string)

	d1 := e.waitDeliveryStatus(a1, "dead") // poison: blocks key A
	e.waitDeliveryStatus(b1, "succeeded")  // key B unaffected

	// A2 stays pending (head-of-line blocked behind the dead A1).
	time.Sleep(400 * time.Millisecond)
	_, list := e.do(http.MethodGet, "/v1/deliveries?business_key=key-A&status=pending", nil, nil)
	pending := list["deliveries"].([]any)
	if len(pending) != 1 {
		t.Fatalf("key-A pending deliveries = %d, want 1 (A2 blocked behind poison)", len(pending))
	}
	a2DeliveryID := pending[0].(map[string]any)["id"].(string)

	// Skipping requires a human decision WITH a reason.
	if code, _ := e.do(http.MethodPost, "/v1/deliveries/"+d1["id"].(string)+"/skip",
		map[string]any{"resolved_by": "ops@example.com"}, nil); code != http.StatusBadRequest {
		t.Errorf("skip without reason: code=%d, want 400", code)
	}
	// Only dead deliveries can be skipped.
	if code, _ := e.skipDelivery(a2DeliveryID, "not dead yet"); code != http.StatusConflict {
		t.Errorf("skip of pending delivery: code=%d, want 409", code)
	}

	// Human skips the poison message; receiver recovers, A2 flows.
	e.setMode("poison", "ok")
	code, resp := e.skipDelivery(d1["id"].(string), "poison payload confirmed with customer; discarding")
	if code != http.StatusOK {
		t.Fatalf("skip with reason: code=%d resp=%v", code, resp)
	}
	skipped := resp["delivery"].(map[string]any)
	if skipped["status"] != "skipped" || skipped["skip_reason"] == nil {
		t.Errorf("skipped delivery missing status/reason: %v", skipped)
	}
	e.waitDeliveryStatus(pending[0].(map[string]any)["event_id"].(string), "succeeded")

	// Downstream sees the gap: key A processed seq 2, seq 1 is missing.
	if got, want := e.processedSeqs("key-A"), []int64{2}; !reflect.DeepEqual(got, want) {
		t.Errorf("key-A processed = %v, want %v", got, want)
	}
	st := e.receiverState()
	gaps := st["sequence_gaps"].(map[string]any)["key-A"].([]any)
	if len(gaps) != 1 || gaps[0].(float64) != 1 {
		t.Errorf("key-A gaps = %v, want [1]", gaps)
	}
}

// --- endpoint generation migration ---

// In-flight requests at migration time complete against their OWN
// (old-generation) delivery row; the new generation gets no task for that
// event, so a late success receipt from the old domain cannot confirm a
// new-generation delivery.
func TestMigrationInFlightCompletes(t *testing.T) {
	e := setup(t, 15438)
	gen1 := e.createEndpoint("migold", "", "t.mig", 5, 3000)
	e.setMode("migold", "timeout:1") // processes, sleeps 1.5s, then 200

	_, r1 := e.publish("mig-1", "t.mig", "migkey")
	e1 := r1["event"].(map[string]any)["id"].(string)
	// Wait until the receiver is actually processing E1 (attempt in flight).
	e.waitFor("receiver processing e1", func() bool {
		st := e.receiverState()
		return st["deliveries"].(map[string]any)[e1] != nil
	})
	// E2 (same key) is queued behind E1.
	_, r2 := e.publish("mig-2", "t.mig", "migkey")
	e2 := r2["event"].(map[string]any)["id"].(string)

	// Customer switches domain mid-flight.
	newURL := fmt.Sprintf("%s/testkit/receive?secret=%s&key=mignew", e.server.URL, testSecret)
	gen2 := e.migrateEndpoint(t, gen1, newURL)

	// E1's in-flight attempt completes on the OLD generation's row.
	d1 := e.waitDeliveryStatus(e1, "succeeded")
	if d1["endpoint_id"] != gen1 {
		t.Errorf("e1 delivery endpoint_id = %v, want old generation %v", d1["endpoint_id"], gen1)
	}
	// No new-generation task exists for E1: exactly one delivery total.
	_, ev1 := e.do(http.MethodGet, "/v1/events/"+e1, nil, nil)
	if got := len(ev1["deliveries"].([]any)); got != 1 {
		t.Errorf("e1 has %d deliveries, want 1 (late receipt cannot confirm a new-generation task)", got)
	}
	// E2 was re-pointed to the new generation and delivered there, in order.
	d2 := e.waitDeliveryStatus(e2, "succeeded")
	if d2["endpoint_id"] != gen2 {
		t.Errorf("e2 delivery endpoint_id = %v, want new generation %v", d2["endpoint_id"], gen2)
	}
	if got, want := e.processedSeqs("migkey"), []int64{1, 2}; !reflect.DeepEqual(got, want) {
		t.Errorf("migkey processed = %v, want %v (order preserved across migration)", got, want)
	}
}

// An in-flight attempt that FAILS after migration retries on the new
// generation (new domain), with the same event id; the receiver dedups.
func TestMigrationInFlightFailureContinues(t *testing.T) {
	e := setup(t, 15439)
	gen1 := e.createEndpoint("migfold", "", "t.migf", 5, 800) // 800ms client timeout
	e.setMode("migfold", "timeout:1")                         // receiver sleeps 1.5s > timeout

	_, r1 := e.publish("migf-1", "t.migf", "migfkey")
	f1 := r1["event"].(map[string]any)["id"].(string)
	e.waitFor("receiver processing f1", func() bool {
		st := e.receiverState()
		return st["deliveries"].(map[string]any)[f1] != nil
	})
	newURL := fmt.Sprintf("%s/testkit/receive?secret=%s&key=migfnew", e.server.URL, testSecret)
	gen2 := e.migrateEndpoint(t, gen1, newURL)

	// The first attempt times out (response lost); retry goes to gen2.
	d1 := e.waitDeliveryStatus(f1, "succeeded")
	if d1["endpoint_id"] != gen2 {
		t.Errorf("f1 delivery endpoint_id = %v, want new generation %v after retry", d1["endpoint_id"], gen2)
	}
	if got := d1["attempt_count"].(float64); got != 2 {
		t.Errorf("attempt_count = %v, want 2", got)
	}
	st := e.receiverState()
	if got := st["deliveries"].(map[string]any)[f1]; got != float64(2) {
		t.Errorf("receiver saw %v deliveries, want 2 (response lost + retry)", got)
	}
	if got := st["business_effects"].(map[string]any)["migfkey"]; got != float64(1) {
		t.Errorf("business applied %v times, want 1 (dedup across generations)", got)
	}
}

// --- secret rotation ---

func TestSecretRotationWindow(t *testing.T) {
	e := setup(t, 15440)
	// Endpoint verifies via the testkit secret store (no inline secret).
	url := fmt.Sprintf("%s/testkit/receive?key=rot", e.server.URL)
	epID := e.createEndpointRaw(url, "whsec_v1", "t.rot", 3, 2000)
	e.setSecret("rot", "whsec_v1", "", 0)

	_, r1 := e.publish("rot-1", "t.rot", "rotkey")
	e.waitDeliveryStatus(r1["event"].(map[string]any)["id"].(string), "succeeded")

	// Rotate: new secret signs from now on; old one stays valid for 10min.
	code, rot := e.do(http.MethodPost, "/v1/endpoints/"+epID+"/rotate-secret",
		map[string]any{"window_seconds": 600}, nil)
	if code != http.StatusOK {
		t.Fatalf("rotate: %d %v", code, rot)
	}
	newSecret := rot["secret"].(string)
	if newSecret == "whsec_v1" {
		t.Fatal("rotation returned the same secret")
	}
	// Receiver learns both: current=new, previous=old (bounded window).
	e.setSecret("rot", newSecret, "whsec_v1", time.Now().Add(10*time.Minute).Unix())

	// New deliveries are signed with the NEW secret and accepted.
	_, r2 := e.publish("rot-2", "t.rot", "rotkey")
	e.waitDeliveryStatus(r2["event"].(map[string]any)["id"].(string), "succeeded")

	// During the window a request signed with the OLD secret is accepted.
	body := []byte(`{"id":"evt_crafted_old","type":"t.rot","business_key":"rotkey","seq":99,"data":{}}`)
	header := signature.SignHeader("whsec_v1", time.Now().Unix(), body)
	if code := e.craftedReceive("rot", "evt_crafted_old", header, body); code != http.StatusOK {
		t.Errorf("old secret within window: code=%d, want 200", code)
	}

	// After the window closes the old secret is rejected; new one still ok.
	e.setSecret("rot", newSecret, "whsec_v1", time.Now().Add(-time.Minute).Unix())
	header = signature.SignHeader("whsec_v1", time.Now().Unix(), body)
	if code := e.craftedReceive("rot", "evt_crafted_old2", header, body); code != http.StatusUnauthorized {
		t.Errorf("old secret after window: code=%d, want 401", code)
	}
	bodyNew := []byte(`{"id":"evt_crafted_new","type":"t.rot","business_key":"rotkey","seq":9,"data":{}}`)
	header = signature.SignHeader(newSecret, time.Now().Unix(), bodyNew)
	if code := e.craftedReceive("rot", "evt_crafted_new", header, bodyNew); code != http.StatusOK {
		t.Errorf("new secret after window: code=%d, want 200", code)
	}
}

// craftedReceive sends a manually signed request to the testkit receiver.
func (e *env) craftedReceive(key, eventID, sigHeader string, body []byte) int {
	e.t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.server.URL+"/testkit/receive?key="+key, bytesReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(signature.HeaderID, eventID)
	req.Header.Set(signature.HeaderSignature, sigHeader)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
