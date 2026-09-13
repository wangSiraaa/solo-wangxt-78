package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

var ErrNotFound = errors.New("not found")

const endpointCols = `id, url, description, secret, subscribed_events, status,
	max_attempts, backoff_base_ms, backoff_max_ms, http_timeout_ms, created_at, updated_at`

func scanEndpoint(row pgx.Row) (*Endpoint, error) {
	var e Endpoint
	err := row.Scan(&e.ID, &e.URL, &e.Description, &e.Secret, &e.SubscribedEvents,
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

// CreateEndpoint inserts a new endpoint.
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
		if err := rows.Scan(&e.ID, &e.URL, &e.Description, &e.Secret, &e.SubscribedEvents,
			&e.Status, &e.MaxAttempts, &e.BackoffBaseMs, &e.BackoffMaxMs, &e.HTTPTimeoutMs,
			&e.CreatedAt, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EndpointPatch carries optional updates; nil fields are left unchanged.
type EndpointPatch struct {
	URL              *string
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
	if p.URL != nil {
		add("url = $%d", *p.URL)
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

// RotateSecret replaces the endpoint's signing secret.
func (s *Store) RotateSecret(ctx context.Context, id, newSecret string) (*Endpoint, error) {
	return scanEndpoint(s.pool.QueryRow(ctx,
		`UPDATE endpoints SET secret = $2, updated_at = now()
		 WHERE id = $1 RETURNING `+endpointCols, id, newSecret))
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
