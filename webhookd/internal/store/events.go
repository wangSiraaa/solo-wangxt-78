package store

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// CreateEventParams is the input for publishing an event.
type CreateEventParams struct {
	ID             string
	IdempotencyKey string
	EventType      string
	BusinessKey    string
	Payload        json.RawMessage
}

// CreateEventResult is the outcome of CreateEvent.
type CreateEventResult struct {
	Event      Event
	Deliveries []Delivery
	// Duplicate is true when the idempotency key already existed: the
	// original event is returned unchanged and no new deliveries are made.
	Duplicate bool
}

const eventCols = `id, idempotency_key, event_type, business_key, key_seq, payload, created_at`

// CreateEvent is the transactional outbox: in ONE transaction it assigns the
// per-business-key sequence number, inserts the event, matches active
// subscriptions and writes the pending delivery rows. Re-publishing with the
// same idempotency key returns the stored event (event identity and sequence
// are stable) without creating new deliveries.
func (s *Store) CreateEvent(ctx context.Context, p CreateEventParams) (*CreateEventResult, error) {
	res := &CreateEventResult{}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Serialize concurrent publishers of the SAME idempotency key
		// (database-scoped lock, works across service instances). The
		// duplicate check below then runs before any sequence number is
		// allocated, so concurrent duplicates never burn a key_seq and
		// never create fake downstream gaps.
		if _, err := tx.Exec(ctx,
			`SELECT pg_advisory_xact_lock(hashtext($1))`, p.IdempotencyKey); err != nil {
			return err
		}

		// Fast path: idempotent replay returns the original fact without
		// burning a sequence number (gaps should mean skips, not replays).
		err := tx.QueryRow(ctx,
			`SELECT `+eventCols+` FROM events WHERE idempotency_key = $1`, p.IdempotencyKey).
			Scan(&res.Event.ID, &res.Event.IdempotencyKey, &res.Event.EventType,
				&res.Event.BusinessKey, &res.Event.KeySeq, &res.Event.Payload, &res.Event.CreatedAt)
		if err == nil {
			res.Duplicate = true
			dels, err := deliveriesForEvent(ctx, tx, res.Event.ID)
			if err != nil {
				return err
			}
			res.Deliveries = dels
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		// Per-key monotonic sequence, assigned inside the transaction.
		var seq int64
		if err := tx.QueryRow(ctx,
			`INSERT INTO key_sequences (business_key, next_seq) VALUES ($1, 1)
			 ON CONFLICT (business_key) DO UPDATE SET next_seq = key_sequences.next_seq + 1
			 RETURNING next_seq`, p.BusinessKey).Scan(&seq); err != nil {
			return err
		}

		err = tx.QueryRow(ctx,
			`INSERT INTO events (id, idempotency_key, event_type, business_key, key_seq, payload)
			 VALUES ($1,$2,$3,$4,$5,$6)
			 ON CONFLICT (idempotency_key) DO NOTHING
			 RETURNING `+eventCols,
			p.ID, p.IdempotencyKey, p.EventType, p.BusinessKey, seq, p.Payload).
			Scan(&res.Event.ID, &res.Event.IdempotencyKey, &res.Event.EventType,
				&res.Event.BusinessKey, &res.Event.KeySeq, &res.Event.Payload, &res.Event.CreatedAt)

		if errors.Is(err, pgx.ErrNoRows) {
			// Lost a concurrent-publish race: return the winner's event.
			res.Duplicate = true
			if err := tx.QueryRow(ctx,
				`SELECT `+eventCols+` FROM events WHERE idempotency_key = $1`, p.IdempotencyKey).
				Scan(&res.Event.ID, &res.Event.IdempotencyKey, &res.Event.EventType,
					&res.Event.BusinessKey, &res.Event.KeySeq, &res.Event.Payload, &res.Event.CreatedAt); err != nil {
				return err
			}
			dels, err := deliveriesForEvent(ctx, tx, res.Event.ID)
			if err != nil {
				return err
			}
			res.Deliveries = dels
			return nil
		}
		if err != nil {
			return err
		}

		// Subscription matching: exact event type or '*' wildcard.
		rows, err := tx.Query(ctx,
			`SELECT id, lineage_id FROM endpoints
			 WHERE status = 'active'
			   AND (subscribed_events && $1::text[] OR subscribed_events @> ARRAY['*']::text[])`,
			[]string{p.EventType})
		if err != nil {
			return err
		}
		type epRef struct{ id, lineage string }
		var eps []epRef
		for rows.Next() {
			var r epRef
			if err := rows.Scan(&r.id, &r.lineage); err != nil {
				rows.Close()
				return err
			}
			eps = append(eps, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, ep := range eps {
			if _, err := tx.Exec(ctx,
				`INSERT INTO deliveries (event_id, endpoint_id, business_key, key_seq, lineage_id)
				 VALUES ($1,$2,$3,$4,$5)
				 ON CONFLICT (event_id, endpoint_id) DO NOTHING`,
				res.Event.ID, ep.id, res.Event.BusinessKey, res.Event.KeySeq, ep.lineage); err != nil {
				return err
			}
		}
		dels, err := deliveriesForEvent(ctx, tx, res.Event.ID)
		if err != nil {
			return err
		}
		res.Deliveries = dels
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *Store) GetEvent(ctx context.Context, id string) (*Event, error) {
	var e Event
	err := s.pool.QueryRow(ctx,
		`SELECT `+eventCols+` FROM events WHERE id = $1`, id).
		Scan(&e.ID, &e.IdempotencyKey, &e.EventType, &e.BusinessKey, &e.KeySeq, &e.Payload, &e.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// CountEvents is used by tests and ops checks.
func (s *Store) CountEvents(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&n)
	return n, err
}
