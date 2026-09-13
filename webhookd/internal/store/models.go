package store

import (
	"encoding/json"
	"time"
)

// Endpoint is one GENERATION of a registered webhook receiver. All
// generations of a logical endpoint share LineageID; switching the receiver
// domain creates a new generation and supersedes the old one.
type Endpoint struct {
	ID               string    `json:"id"`
	LineageID        string    `json:"lineage_id"`
	Generation       int       `json:"generation"`
	URL              string    `json:"url"`
	Description      string    `json:"description"`
	Secret           string    `json:"-"`
	PreviousSecret   *string   `json:"-"`
	PreviousSecretExpiresAt *time.Time `json:"previous_secret_expires_at,omitempty"`
	SubscribedEvents []string  `json:"subscribed_events"`
	Status           string    `json:"status"`
	MaxAttempts      int       `json:"max_attempts"`
	BackoffBaseMs    int       `json:"backoff_base_ms"`
	BackoffMaxMs     int       `json:"backoff_max_ms"`
	HTTPTimeoutMs    int       `json:"http_timeout_ms"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// Event is the immutable business fact. Its ID never changes across retries
// and dead-letter replays. KeySeq is its position in the per-business-key
// delivery sequence.
type Event struct {
	ID             string          `json:"id"`
	IdempotencyKey string          `json:"-"`
	EventType      string          `json:"type"`
	BusinessKey    string          `json:"business_key"`
	KeySeq         int64           `json:"seq"`
	Payload        json.RawMessage `json:"data"`
	CreatedAt      time.Time       `json:"created_at"`
}

// Delivery is one event bound for one endpoint generation.
type Delivery struct {
	ID             string     `json:"id"`
	EventID        string     `json:"event_id"`
	EndpointID     string     `json:"endpoint_id"`
	BusinessKey    string     `json:"business_key"`
	KeySeq         int64      `json:"seq"`
	LineageID      string     `json:"lineage_id"`
	Status         string     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	LastStatusCode *int       `json:"last_status_code,omitempty"`
	LastError      *string    `json:"last_error,omitempty"`
	SkipReason     *string    `json:"skip_reason,omitempty"`
	SkippedBy      *string    `json:"skipped_by,omitempty"`
	SkippedAt      *time.Time `json:"skipped_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// Attempt is one HTTP delivery try.
type Attempt struct {
	ID         int64      `json:"id"`
	DeliveryID string     `json:"delivery_id"`
	AttemptNo  int        `json:"attempt_no"`
	StatusCode *int       `json:"status_code,omitempty"`
	Error      string     `json:"error,omitempty"`
	DurationMs int        `json:"duration_ms"`
	CreatedAt  time.Time  `json:"created_at"`
}
