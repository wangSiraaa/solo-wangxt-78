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

// CreateEvent is the transactional outbox: in ONE transaction it inserts the
// event, matches active subscriptions and writes the pending delivery rows.
// Re-publishing with the same idempotency key returns the stored event
// (event identity is stable) without creating new deliveries.
func (s *Store) CreateEvent(ctx context.Context, p CreateEventParams) (*CreateEventResult, error) {
	res := &CreateEventResult{}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx,
			`INSERT INTO events (id, idempotency_key, event_type, payload)
			 VALUES ($1,$2,$3,$4)
			 ON CONFLICT (idempotency_key) DO NOTHING
			 RETURNING id, idempotency_key, event_type, payload, created_at`,
			p.ID, p.IdempotencyKey, p.EventType, p.Payload).
			Scan(&res.Event.ID, &res.Event.IdempotencyKey, &res.Event.EventType,
				&res.Event.Payload, &res.Event.CreatedAt)

		if errors.Is(err, pgx.ErrNoRows) {
			// Idempotent replay: return the original fact, no new deliveries.
			res.Duplicate = true
			if err := tx.QueryRow(ctx,
				`SELECT id, idempotency_key, event_type, payload, created_at
				 FROM events WHERE idempotency_key = $1`, p.IdempotencyKey).
				Scan(&res.Event.ID, &res.Event.IdempotencyKey, &res.Event.EventType,
					&res.Event.Payload, &res.Event.CreatedAt); err != nil {
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
			`SELECT id FROM endpoints
			 WHERE status = 'active'
			   AND (subscribed_events && $1::text[] OR subscribed_events @> ARRAY['*']::text[])`,
			[]string{p.EventType})
		if err != nil {
			return err
		}
		var endpointIDs []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			endpointIDs = append(endpointIDs, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, epID := range endpointIDs {
			if _, err := tx.Exec(ctx,
				`INSERT INTO deliveries (event_id, endpoint_id) VALUES ($1,$2)
				 ON CONFLICT (event_id, endpoint_id) DO NOTHING`,
				res.Event.ID, epID); err != nil {
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
		`SELECT id, idempotency_key, event_type, payload, created_at FROM events WHERE id = $1`, id).
		Scan(&e.ID, &e.IdempotencyKey, &e.EventType, &e.Payload, &e.CreatedAt)
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
