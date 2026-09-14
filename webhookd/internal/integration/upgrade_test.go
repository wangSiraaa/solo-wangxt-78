package integration

import (
	"context"
	"io"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	"github.com/example/webhookd/internal/store"
)

// TestUpgradeFromBcc7c845 starts a database with the ORIGINAL bcc7c845
// schema (plus data in every delivery state), runs the incremental
// migrator, and verifies the schema is upgraded in place with all rows and
// delivery states preserved.
func TestUpgradeFromBcc7c845(t *testing.T) {
	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().Port(15441).Database("webhookd_old").Logger(io.Discard))
	if err := pg.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}
	defer pg.Stop() //nolint:errcheck

	ctx := context.Background()
	st, err := store.New(ctx, "postgres://postgres:postgres@localhost:15441/webhookd_old?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// 1. Lay down the OLD schema exactly as bcc7c845 created it.
	oldSchema, err := os.ReadFile("../store/testdata/schema_bcc7c845.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool().Exec(ctx, string(oldSchema)); err != nil {
		t.Fatalf("apply old schema: %v", err)
	}

	// 2. Seed old-format data: one endpoint, three events with deliveries in
	//    succeeded / pending / dead states.
	if _, err := st.Pool().Exec(ctx, `
		INSERT INTO endpoints (id, url, secret, subscribed_events)
		VALUES ('11111111-1111-1111-1111-111111111111', 'https://old.example.com/hook', 'whsec_old', '{order.created}');
		INSERT INTO events (id, idempotency_key, event_type, payload) VALUES
		  ('aaaaaaa1-0000-0000-0000-000000000001', 'old-1', 'order.created', '{"n":1}'),
		  ('aaaaaaa2-0000-0000-0000-000000000002', 'old-2', 'order.created', '{"n":2}'),
		  ('aaaaaaa3-0000-0000-0000-000000000003', 'old-3', 'order.created', '{"n":3}');
		INSERT INTO deliveries (event_id, endpoint_id, status, attempt_count, last_status_code) VALUES
		  ('aaaaaaa1-0000-0000-0000-000000000001', '11111111-1111-1111-1111-111111111111', 'succeeded', 1, 200),
		  ('aaaaaaa2-0000-0000-0000-000000000002', '11111111-1111-1111-1111-111111111111', 'pending', 0, NULL),
		  ('aaaaaaa3-0000-0000-0000-000000000003', '11111111-1111-1111-1111-111111111111', 'dead', 8, 500);
	`); err != nil {
		t.Fatalf("seed old data: %v", err)
	}

	// 3. Run the migrator twice: upgrade must be repeatable.
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate (1st): %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate (2nd, must be a no-op): %v", err)
	}

	// 4. Schema upgraded: both versions recorded.
	var versions int
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM schema_migrations`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions < 2 {
		t.Errorf("schema_migrations has %d rows, want >= 2", versions)
	}

	// 5. Data preserved and backfilled.
	var (
		lineageOK  bool
		generation int
		status     string
	)
	if err := st.Pool().QueryRow(ctx,
		`SELECT (lineage_id = id), generation, status FROM endpoints
		 WHERE id = '11111111-1111-1111-1111-111111111111'`).
		Scan(&lineageOK, &generation, &status); err != nil {
		t.Fatal(err)
	}
	if !lineageOK || generation != 1 || status != "active" {
		t.Errorf("endpoint after upgrade: lineage_ok=%v generation=%d status=%s", lineageOK, generation, status)
	}

	rows, err := st.Pool().Query(ctx,
		`SELECT d.status, d.attempt_count, (d.business_key = e.business_key), d.key_seq,
		        (d.lineage_id = '11111111-1111-1111-1111-111111111111')
		 FROM deliveries d JOIN events e ON e.id = d.event_id ORDER BY e.id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type check struct {
		status   string
		attempts int
		keyOK    bool
		seq      int64
		linOK    bool
	}
	var got []check
	for rows.Next() {
		var c check
		if err := rows.Scan(&c.status, &c.attempts, &c.keyOK, &c.seq, &c.linOK); err != nil {
			t.Fatal(err)
		}
		got = append(got, c)
	}
	if len(got) != 3 {
		t.Fatalf("deliveries after upgrade = %d, want 3", len(got))
	}
	wantStatus := []string{"succeeded", "pending", "dead"}
	wantAttempts := []int{1, 0, 8}
	for i, c := range got {
		if c.status != wantStatus[i] || c.attempts != wantAttempts[i] {
			t.Errorf("delivery %d: status=%s attempts=%d, want %s/%d (state must be preserved)",
				i, c.status, c.attempts, wantStatus[i], wantAttempts[i])
		}
		if !c.keyOK || c.seq != 1 || !c.linOK {
			t.Errorf("delivery %d: backfill keyOK=%v seq=%d linOK=%v", i, c.keyOK, c.seq, c.linOK)
		}
	}

	// 6. The upgraded database is fully functional: a new event gets a
	//    sequence number, matches the migrated endpoint, and the OLD pending
	//    delivery is still claimable (delivery state preserved).
	res, err := st.CreateEvent(ctx, store.CreateEventParams{
		ID:             "bbbbbbb1-0000-0000-0000-000000000001",
		IdempotencyKey: "new-1",
		EventType:      "order.created",
		BusinessKey:    "order-new",
		Payload:        []byte(`{"n":4}`),
	})
	if err != nil {
		t.Fatalf("create event on upgraded db: %v", err)
	}
	if res.Event.KeySeq != 1 || len(res.Deliveries) != 1 {
		t.Errorf("new event: seq=%d deliveries=%d, want 1/1", res.Event.KeySeq, len(res.Deliveries))
	}
	claimed, err := st.ClaimDeliveries(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	var sawOldPending, sawNew bool
	for _, c := range claimed {
		if c.Delivery.EventID == "aaaaaaa2-0000-0000-0000-000000000002" {
			sawOldPending = true
		}
		if c.Delivery.EventID == res.Event.ID {
			sawNew = true
		}
	}
	if !sawOldPending || !sawNew {
		t.Errorf("claim after upgrade: old pending=%v new=%v (both must be claimable)", sawOldPending, sawNew)
	}
}
