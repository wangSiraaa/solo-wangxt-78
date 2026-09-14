package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const deliveryCols = `id, event_id, endpoint_id, business_key, key_seq, lineage_id, status,
	attempt_count, next_attempt_at, last_status_code, last_error,
	skip_reason, skipped_by, skipped_at, created_at, updated_at`

func scanDelivery(row pgx.Row) (*Delivery, error) {
	var d Delivery
	err := row.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.BusinessKey, &d.KeySeq, &d.LineageID,
		&d.Status, &d.AttemptCount, &d.NextAttemptAt, &d.LastStatusCode, &d.LastError,
		&d.SkipReason, &d.SkippedBy, &d.SkippedAt, &d.CreatedAt, &d.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func scanDeliveries(rows pgx.Rows) ([]Delivery, error) {
	defer rows.Close()
	out := []Delivery{}
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.ID, &d.EventID, &d.EndpointID, &d.BusinessKey, &d.KeySeq, &d.LineageID,
			&d.Status, &d.AttemptCount, &d.NextAttemptAt, &d.LastStatusCode, &d.LastError,
			&d.SkipReason, &d.SkippedBy, &d.SkippedAt, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func deliveriesForEvent(ctx context.Context, tx pgx.Tx, eventID string) ([]Delivery, error) {
	rows, err := tx.Query(ctx,
		`SELECT `+deliveryCols+` FROM deliveries WHERE event_id = $1 ORDER BY created_at`, eventID)
	if err != nil {
		return nil, err
	}
	return scanDeliveries(rows)
}

func (s *Store) GetDelivery(ctx context.Context, id string) (*Delivery, error) {
	return scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryCols+` FROM deliveries WHERE id = $1`, id))
}

// DeliveryFilter narrows ListDeliveries; empty values are ignored.
type DeliveryFilter struct {
	EventID     string
	EndpointID  string
	BusinessKey string
	Status      string
	Limit       int
}

func (s *Store) ListDeliveries(ctx context.Context, f DeliveryFilter) ([]Delivery, error) {
	where := []string{"1=1"}
	args := []any{}
	add := func(clause string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if f.EventID != "" {
		add("event_id = $%d", f.EventID)
	}
	if f.EndpointID != "" {
		add("endpoint_id = $%d", f.EndpointID)
	}
	if f.BusinessKey != "" {
		add("business_key = $%d", f.BusinessKey)
	}
	if f.Status != "" {
		add("status = $%d", f.Status)
	}
	limit := 100
	if f.Limit > 0 && f.Limit <= 500 {
		limit = f.Limit
	}
	args = append(args, limit)
	q := `SELECT ` + deliveryCols + ` FROM deliveries WHERE ` + strings.Join(where, " AND ") +
		fmt.Sprintf(` ORDER BY business_key, key_seq LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanDeliveries(rows)
}

// ClaimedDelivery is a delivery joined with everything needed to attempt it.
type ClaimedDelivery struct {
	Delivery Delivery
	Event    Event
	Endpoint Endpoint
}

// ClaimDeliveries atomically marks up to limit due deliveries as
// 'delivering' (FOR UPDATE SKIP LOCKED). Per-business-key ordering: a
// delivery is only claimable when it is the HEAD of its key — every earlier
// delivery of the same (lineage, business_key) is already resolved
// (succeeded or skipped). A dead (poison) delivery therefore blocks only its
// own key; other keys proceed in parallel.
func (s *Store) ClaimDeliveries(ctx context.Context, limit int) ([]ClaimedDelivery, error) {
	var out []ClaimedDelivery
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`WITH c AS (
			   SELECT d.id FROM deliveries d
			   JOIN endpoints ep ON ep.id = d.endpoint_id
			   WHERE d.status = 'pending' AND d.next_attempt_at <= now()
			     AND ep.status = 'active'
			     AND NOT EXISTS (
			       SELECT 1 FROM deliveries p
			       WHERE p.lineage_id = d.lineage_id
			         AND p.business_key = d.business_key
			         AND p.key_seq < d.key_seq
			         AND p.status IN ('pending', 'delivering', 'dead')
			     )
			   ORDER BY d.business_key, d.key_seq
			   LIMIT $1
			   FOR UPDATE OF d SKIP LOCKED
			 )
			 UPDATE deliveries d SET status = 'delivering', updated_at = now()
			 FROM c WHERE d.id = c.id
			 RETURNING d.id`, limit)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		jrows, err := tx.Query(ctx,
			`SELECT d.id, d.event_id, d.endpoint_id, d.business_key, d.key_seq, d.lineage_id,
			        d.status, d.attempt_count, d.next_attempt_at, d.last_status_code, d.last_error,
			        d.skip_reason, d.skipped_by, d.skipped_at, d.created_at, d.updated_at,
			        e.id, e.idempotency_key, e.event_type, e.business_key, e.key_seq, e.payload, e.created_at,
			        ep.id, ep.lineage_id, ep.generation, ep.url, ep.description, ep.secret,
			        ep.previous_secret, ep.previous_secret_expires_at, ep.subscribed_events, ep.status,
			        ep.max_attempts, ep.backoff_base_ms, ep.backoff_max_ms, ep.http_timeout_ms,
			        ep.created_at, ep.updated_at
			 FROM deliveries d
			 JOIN events e ON e.id = d.event_id
			 JOIN endpoints ep ON ep.id = d.endpoint_id
			 WHERE d.id = ANY($1)`, ids)
		if err != nil {
			return err
		}
		defer jrows.Close()
		for jrows.Next() {
			var c ClaimedDelivery
			if err := jrows.Scan(
				&c.Delivery.ID, &c.Delivery.EventID, &c.Delivery.EndpointID, &c.Delivery.BusinessKey,
				&c.Delivery.KeySeq, &c.Delivery.LineageID, &c.Delivery.Status, &c.Delivery.AttemptCount,
				&c.Delivery.NextAttemptAt, &c.Delivery.LastStatusCode, &c.Delivery.LastError,
				&c.Delivery.SkipReason, &c.Delivery.SkippedBy, &c.Delivery.SkippedAt,
				&c.Delivery.CreatedAt, &c.Delivery.UpdatedAt,
				&c.Event.ID, &c.Event.IdempotencyKey, &c.Event.EventType, &c.Event.BusinessKey,
				&c.Event.KeySeq, &c.Event.Payload, &c.Event.CreatedAt,
				&c.Endpoint.ID, &c.Endpoint.LineageID, &c.Endpoint.Generation, &c.Endpoint.URL,
				&c.Endpoint.Description, &c.Endpoint.Secret, &c.Endpoint.PreviousSecret,
				&c.Endpoint.PreviousSecretExpiresAt, &c.Endpoint.SubscribedEvents, &c.Endpoint.Status,
				&c.Endpoint.MaxAttempts, &c.Endpoint.BackoffBaseMs, &c.Endpoint.BackoffMaxMs,
				&c.Endpoint.HTTPTimeoutMs, &c.Endpoint.CreatedAt, &c.Endpoint.UpdatedAt,
			); err != nil {
				return err
			}
			out = append(out, c)
		}
		return jrows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Outcome of a delivery attempt.
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeRetry     Outcome = "retry"
	OutcomeDead      Outcome = "dead"
)

// RecordAttemptParams describes a finished attempt.
type RecordAttemptParams struct {
	DeliveryID    string
	AttemptNo     int
	StatusCode    *int // nil = transport failure (timeout, reset, DNS, ...)
	Err           string
	DurationMs    int
	Outcome       Outcome
	NextAttemptAt time.Time // used when Outcome == OutcomeRetry
}

// RecordAttempt writes the attempt row and updates the delivery state in one
// transaction, keeping the attempt log consistent with the delivery status.
// When the delivery's endpoint was superseded mid-attempt (domain
// migration), a retry is re-pointed to the ACTIVE generation of the same
// lineage — the next try goes to the new receiver domain. A success is
// recorded on the old generation's own row and confirms nothing else.
func (s *Store) RecordAttempt(ctx context.Context, p RecordAttemptParams) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO delivery_attempts (delivery_id, attempt_no, status_code, error, duration_ms)
			 VALUES ($1,$2,$3,$4,$5)`,
			p.DeliveryID, p.AttemptNo, p.StatusCode, p.Err, p.DurationMs); err != nil {
			return err
		}
		var status string
		var next time.Time
		switch p.Outcome {
		case OutcomeSucceeded:
			status = "succeeded"
			next = time.Now()
		case OutcomeRetry:
			status = "pending"
			next = p.NextAttemptAt
		case OutcomeDead:
			status = "dead"
			next = time.Now()
		default:
			return fmt.Errorf("unknown outcome %q", p.Outcome)
		}

		// Check the endpoint's CURRENT status (it may have been superseded
		// while this attempt was in flight).
		repoint := false
		if p.Outcome == OutcomeRetry {
			var curStatus string
			if err := tx.QueryRow(ctx,
				`SELECT e.status FROM endpoints e
				 JOIN deliveries d ON d.endpoint_id = e.id WHERE d.id = $1`,
				p.DeliveryID).Scan(&curStatus); err != nil {
				return err
			}
			repoint = curStatus == "superseded"
		}

		var q string
		if repoint {
			q = `UPDATE deliveries d
			     SET status = $2, attempt_count = attempt_count + 1, next_attempt_at = $3,
			         last_status_code = $4, last_error = NULLIF($5, ''), updated_at = now(),
			         endpoint_id = COALESCE(
			           (SELECT e2.id FROM endpoints e2
			             WHERE e2.lineage_id = d.lineage_id AND e2.status = 'active' LIMIT 1),
			           d.endpoint_id)
			     WHERE d.id = $1`
		} else {
			q = `UPDATE deliveries
			     SET status = $2, attempt_count = attempt_count + 1, next_attempt_at = $3,
			         last_status_code = $4, last_error = NULLIF($5, ''), updated_at = now()
			     WHERE id = $1`
		}
		tag, err := tx.Exec(ctx, q, p.DeliveryID, status, next, p.StatusCode, p.Err)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// RequeueStale returns deliveries stuck in 'delivering' (worker crashed
// mid-attempt) to 'pending' so they are retried. Rows still pointing at a
// superseded generation are re-pointed to the active one. This is what makes
// delivery at-least-once even across worker restarts.
func (s *Store) RequeueStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE deliveries d
		 SET status = 'pending', updated_at = now(),
		     endpoint_id = COALESCE(
		       (SELECT e2.id FROM endpoints e2
		         WHERE e2.lineage_id = d.lineage_id AND e2.status = 'active' LIMIT 1),
		       d.endpoint_id)
		 WHERE d.status = 'delivering' AND d.updated_at < now() - make_interval(secs => $1)`,
		olderThan.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ReplayDeadDelivery re-queues a dead delivery. It does NOT create a new
// event: the same event id (the original business fact) is delivered again.
// If the delivery's generation was superseded, the replay goes to the active
// generation (current receiver domain).
func (s *Store) ReplayDeadDelivery(ctx context.Context, id string) (*Delivery, error) {
	return scanDelivery(s.pool.QueryRow(ctx,
		`UPDATE deliveries d
		 SET status = 'pending', next_attempt_at = now(), updated_at = now(),
		     endpoint_id = COALESCE(
		       (SELECT e2.id FROM endpoints e2
		         WHERE e2.lineage_id = d.lineage_id AND e2.status = 'active' LIMIT 1),
		       d.endpoint_id)
		 WHERE d.id = $1 AND d.status = 'dead'
		 RETURNING `+deliveryCols, id))
}

// SkipDeadDelivery is the HUMAN decision to skip a poison message: the dead
// delivery is marked skipped with a mandatory reason, unblocking the rest of
// its business key. The skipped sequence number stays a visible gap
// downstream — the event is never delivered.
func (s *Store) SkipDeadDelivery(ctx context.Context, id, reason, actor string) (*Delivery, error) {
	if strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("skip reason is required")
	}
	return scanDelivery(s.pool.QueryRow(ctx,
		`UPDATE deliveries
		 SET status = 'skipped', skip_reason = $2, skipped_by = NULLIF($3, ''),
		     skipped_at = now(), updated_at = now()
		 WHERE id = $1 AND status = 'dead'
		 RETURNING `+deliveryCols, id, reason, actor))
}

func (s *Store) ListAttempts(ctx context.Context, deliveryID string) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, delivery_id, attempt_no, status_code, error, duration_ms, created_at
		 FROM delivery_attempts WHERE delivery_id = $1 ORDER BY attempt_no`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.DeliveryID, &a.AttemptNo, &a.StatusCode,
			&a.Error, &a.DurationMs, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
