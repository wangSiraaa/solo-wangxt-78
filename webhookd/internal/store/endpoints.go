package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

var ErrNotFound = errors.New("not found")

const endpointCols = `id, lineage_id, generation, url, description, secret, previous_secret,
	previous_secret_expires_at, subscribed_events, status,
	max_attempts, backoff_base_ms, backoff_max_ms, http_timeout_ms, created_at, updated_at`

func scanEndpoint(row pgx.Row) (*Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.LineageID, &e.Generation, &e.URL, &e.Description, &e.Secret,
		&e.PreviousSecret, &e.PreviousSecretExpiresAt, &e.SubscribedEvents,
		&e.Status, &e.MaxAttempts, &e.BackoffBaseMs, &e.BackoffMaxMs, &e.HTTPTimeoutMs,
		&e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

// CreateEndpoint inserts generation 1 of a new endpoint lineage.
func (s *Store) CreateEndpoint(ctx context.Context, e *Endpoint) (*Endpoint, error) {
	return scanEndpoint(s.pool.QueryRow(ctx,
		`INSERT INTO endpoints (url, description, secret, subscribed_events,
		        max_attempts, backoff_base_ms, backoff_max_ms, http_timeout_ms)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		 RETURNING `+endpointCols,
		e.URL, e.Description, e.Secret, e.SubscribedEvents,
		e.MaxAttempts, e.BackoffBaseMs, e.BackoffMaxMs, e.HTTPTimeoutMs))
}

func (s *Store) GetEndpoint(ctx context.Context, id string) (*Endpoint, error) {
	return scanEndpoint(s.pool.QueryRow(ctx,
		`SELECT `+endpointCols+` FROM endpoints WHERE id = $1`, id))
}

func (s *Store) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+endpointCols+` FROM endpoints ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Endpoint
	for rows.Next() {
		var e Endpoint
		if err := rows.Scan(&e.ID, &e.LineageID, &e.Generation, &e.URL, &e.Description, &e.Secret,
			&e.PreviousSecret, &e.PreviousSecretExpiresAt, &e.SubscribedEvents,
			&e.Status, &e.MaxAttempts, &e.BackoffBaseMs, &e.BackoffMaxMs, &e.HTTPTimeoutMs,
			&e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EndpointPatch carries optional updates; nil fields are left unchanged.
// Note: the URL cannot be patched — changing the receiver domain is a
// migration (new generation), see MigrateEndpoint.
type EndpointPatch struct {
	Description      *string
	SubscribedEvents *[]string
	Status           *string
	MaxAttempts      *int
	BackoffBaseMs    *int
	BackoffMaxMs     *int
	HTTPTimeoutMs    *int
}

func (s *Store) UpdateEndpoint(ctx context.Context, id string, p EndpointPatch) (*Endpoint, error) {
	sets := []string{}
	args := []any{id}
	add := func(clause string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf(clause, len(args)))
	}
	if p.Description != nil {
		add("description = $%d", *p.Description)
	}
	if p.SubscribedEvents != nil {
		add("subscribed_events = $%d", *p.SubscribedEvents)
	}
	if p.Status != nil {
		add("status = $%d", *p.Status)
	}
	if p.MaxAttempts != nil {
		add("max_attempts = $%d", *p.MaxAttempts)
	}
	if p.BackoffBaseMs != nil {
		add("backoff_base_ms = $%d", *p.BackoffBaseMs)
	}
	if p.BackoffMaxMs != nil {
		add("backoff_max_ms = $%d", *p.BackoffMaxMs)
	}
	if p.HTTPTimeoutMs != nil {
		add("http_timeout_ms = $%d", *p.HTTPTimeoutMs)
	}
	if len(sets) == 0 {
		return s.GetEndpoint(ctx, id)
	}
	q := `UPDATE endpoints SET ` + strings.Join(sets, ", ") + `, updated_at = now()
	      WHERE id = $1 RETURNING ` + endpointCols
	return scanEndpoint(s.pool.QueryRow(ctx, q, args...))
}

// RotateSecret installs newSecret as the current secret and keeps the old
// one acceptable only until now+window (bounded dual-secret window).
func (s *Store) RotateSecret(ctx context.Context, id, newSecret string, windowSeconds int) (*Endpoint, error) {
	return scanEndpoint(s.pool.QueryRow(ctx,
		`UPDATE endpoints
		 SET previous_secret = secret,
		     previous_secret_expires_at = now() + make_interval(secs => $2),
		     secret = $3, updated_at = now()
		 WHERE id = $1 RETURNING `+endpointCols, id, windowSeconds, newSecret))
}

// DisableEndpoint soft-deletes: pending deliveries stay queued but the
// endpoint no longer matches new events and is skipped by the dispatcher.
func (s *Store) DisableEndpoint(ctx context.Context, id string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE endpoints SET status = 'disabled', updated_at = now() WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MigrateEndpoint switches a lineage to a new receiver URL in ONE
// transaction:
//
//   - the old generation is superseded (never edited in place);
//   - a new generation row is created with the same lineage, subscriptions,
//     secret and retry policy, pointing at newURL;
//   - deliveries already SUCCEEDED stay untouched (processed before
//     migration — not re-sent);
//   - deliveries IN FLIGHT (status 'delivering') stay attached to the old
//     generation so their late receipt confirms only their own row; if such
//     an attempt later fails, RecordAttempt re-points that delivery to the
//     active generation for its next try;
//   - deliveries NOT YET SENT (status 'pending') are re-pointed to the new
//     generation and will be delivered to the new URL.
func (s *Store) MigrateEndpoint(ctx context.Context, oldID, newURL string) (*Endpoint, error) {
	var migrated *Endpoint
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		old, err := scanEndpoint(tx.QueryRow(ctx,
			`SELECT `+endpointCols+` FROM endpoints WHERE id = $1 FOR UPDATE`, oldID))
		if err != nil {
			return err
		}
		if old.Status != "active" {
			return fmt.Errorf("endpoint %s is %s, only active endpoints can be migrated", oldID, old.Status)
		}
		newEp, err := scanEndpoint(tx.QueryRow(ctx,
			`INSERT INTO endpoints (lineage_id, generation, url, description, secret,
			        subscribed_events, max_attempts, backoff_base_ms, backoff_max_ms, http_timeout_ms)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			 RETURNING `+endpointCols,
			old.LineageID, old.Generation+1, newURL, old.Description, old.Secret,
			old.SubscribedEvents, old.MaxAttempts, old.BackoffBaseMs, old.BackoffMaxMs, old.HTTPTimeoutMs))
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE endpoints SET status = 'superseded', updated_at = now() WHERE id = $1`, oldID); err != nil {
			return err
		}
		// Re-point unsent deliveries to the new generation.
		if _, err := tx.Exec(ctx,
			`UPDATE deliveries SET endpoint_id = $2, updated_at = now()
			 WHERE endpoint_id = $1 AND status = 'pending'`, oldID, newEp.ID); err != nil {
			return err
		}
		migrated = newEp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return migrated, nil
}
