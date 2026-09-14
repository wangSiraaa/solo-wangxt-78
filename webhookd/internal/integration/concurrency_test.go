package integration

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"testing"

	"github.com/example/webhookd/internal/store"
)

// Concurrent duplicate publishes must never burn a sequence number: the
// idempotency check and the seq allocation are serialized per key, so the
// next real event continues the sequence without fake gaps.
func TestConcurrentIdempotentPublishNoSeqBurn(t *testing.T) {
	e := setup(t, 15442)
	ctx := context.Background()

	const n = 16
	results := make([]*store.CreateEventResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = e.store.CreateEvent(ctx, store.CreateEventParams{
				ID:             fmt.Sprintf("ccccccc1-0000-0000-0000-%012d", i),
				IdempotencyKey: "dup-key",
				EventType:      "t.conc",
				BusinessKey:    "conc-key",
				Payload:        []byte(`{"n":1}`),
			})
		}(i)
	}
	wg.Wait()

	var eventID string
	created := 0
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("concurrent publish %d: %v", i, errs[i])
		}
		if !results[i].Duplicate {
			created++
		}
		if eventID == "" {
			eventID = results[i].Event.ID
		} else if results[i].Event.ID != eventID {
			t.Errorf("publish %d returned different event id %s vs %s", i, results[i].Event.ID, eventID)
		}
		if results[i].Event.KeySeq != 1 {
			t.Errorf("publish %d: seq = %d, want 1 (duplicates must not burn numbers)", i, results[i].Event.KeySeq)
		}
	}
	if created != 1 {
		t.Errorf("created = %d, want exactly 1 (rest are idempotent replays)", created)
	}

	// The NEXT event of the same key continues the sequence with no gap.
	next, err := e.store.CreateEvent(ctx, store.CreateEventParams{
		ID:             "ccccccc2-0000-0000-0000-000000000000",
		IdempotencyKey: "next-key",
		EventType:      "t.conc",
		BusinessKey:    "conc-key",
		Payload:        []byte(`{"n":2}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if next.Event.KeySeq != 2 {
		t.Errorf("next event seq = %d, want 2 (continuous, no fake gap)", next.Event.KeySeq)
	}
}

// Concurrent publishes of DIFFERENT idempotency keys to the same business
// key get unique, gap-free sequence numbers.
func TestConcurrentDistinctKeysUniqueSeqs(t *testing.T) {
	e := setup(t, 15443)
	ctx := context.Background()

	const n = 8
	seqs := make([]int64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := e.store.CreateEvent(ctx, store.CreateEventParams{
				ID:             fmt.Sprintf("ddddddd1-0000-0000-0000-%012d", i),
				IdempotencyKey: fmt.Sprintf("distinct-%d", i),
				EventType:      "t.conc2",
				BusinessKey:    "conc2-key",
				Payload:        []byte(`{"n":1}`),
			})
			if err == nil {
				seqs[i] = res.Event.KeySeq
			}
			errs[i] = err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	sort.Slice(seqs, func(a, b int) bool { return seqs[a] < seqs[b] })
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("seqs = %v, want [1..%d] unique and gap-free", seqs, n)
		}
	}
}

// A superseded generation must not be reactivatable: one lineage always has
// exactly one active generation, and the old domain gets no new tasks.
func TestSupersededCannotReactivate(t *testing.T) {
	e := setup(t, 15444)
	gen1 := e.createEndpoint("olddom", "ok", "t.reac", 3, 2000)
	newURL := ""
	_ = newURL
	newURL = e.server.URL + "/testkit/receive?secret=" + testSecret + "&key=newdom"
	gen2 := e.migrateEndpoint(t, gen1, newURL)

	// Reactivating the old generation is rejected.
	code, resp := e.do(http.MethodPatch, "/v1/endpoints/"+gen1, map[string]any{"status": "active"}, nil)
	if code != http.StatusConflict {
		t.Fatalf("reactivate superseded: code=%d resp=%v, want 409", code, resp)
	}
	// It stays superseded.
	_, cur := e.do(http.MethodGet, "/v1/endpoints/"+gen1, nil, nil)
	if cur["status"] != "superseded" {
		t.Errorf("old generation status = %v, want superseded", cur["status"])
	}

	// New events create tasks ONLY on the active (new) generation.
	_, pub := e.publish("reac-1", "t.reac", "reac-key")
	eventID := pub["event"].(map[string]any)["id"].(string)
	d := e.waitDeliveryStatus(eventID, "succeeded")
	if d["endpoint_id"] != gen2 {
		t.Errorf("delivery endpoint_id = %v, want active generation %v", d["endpoint_id"], gen2)
	}
	_, list := e.do(http.MethodGet, "/v1/deliveries?endpoint_id="+gen1, nil, nil)
	if got := len(list["deliveries"].([]any)); got != 0 {
		t.Errorf("old generation has %d deliveries, want 0 (no tasks on the old domain)", got)
	}
}
