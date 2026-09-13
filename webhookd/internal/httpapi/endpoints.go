package httpapi

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/example/webhookd/internal/signature"
	"github.com/example/webhookd/internal/store"
)

// ---------- endpoints ----------

type retryPolicyReq struct {
	MaxAttempts   *int `json:"max_attempts" binding:"omitempty,min=1,max=25"`
	BackoffBaseMs *int `json:"backoff_base_ms" binding:"omitempty,min=50,max=600000"`
	BackoffMaxMs  *int `json:"backoff_max_ms" binding:"omitempty,min=100,max=3600000"`
	HTTPTimeoutMs *int `json:"http_timeout_ms" binding:"omitempty,min=500,max=60000"`
}

type createEndpointReq struct {
	URL              string          `json:"url" binding:"required"`
	Description      string          `json:"description"`
	Secret           string          `json:"secret"` // optional; generated when empty
	SubscribedEvents []string        `json:"subscribed_events" binding:"required,min=1"`
	RetryPolicy      *retryPolicyReq `json:"retry_policy"`
}

// endpointJSON renders an endpoint; the secret is only ever returned at
// creation/rotation time.
func endpointJSON(e *store.Endpoint, includeSecret bool) gin.H {
	out := gin.H{
		"id":                e.ID,
		"lineage_id":        e.LineageID,
		"generation":        e.Generation,
		"url":               e.URL,
		"description":       e.Description,
		"subscribed_events": e.SubscribedEvents,
		"status":            e.Status,
		"retry_policy": gin.H{
			"max_attempts":    e.MaxAttempts,
			"backoff_base_ms": e.BackoffBaseMs,
			"backoff_max_ms":  e.BackoffMaxMs,
			"http_timeout_ms": e.HTTPTimeoutMs,
		},
		"created_at": e.CreatedAt,
		"updated_at": e.UpdatedAt,
	}
	if e.PreviousSecretExpiresAt != nil {
		out["previous_secret_expires_at"] = e.PreviousSecretExpiresAt
	}
	if includeSecret {
		out["secret"] = e.Secret
	}
	return out
}

func applyPolicyDefaults(e *store.Endpoint, p *retryPolicyReq) {
	e.MaxAttempts, e.BackoffBaseMs, e.BackoffMaxMs, e.HTTPTimeoutMs = 8, 1000, 300000, 10000
	if p == nil {
		return
	}
	if p.MaxAttempts != nil {
		e.MaxAttempts = *p.MaxAttempts
	}
	if p.BackoffBaseMs != nil {
		e.BackoffBaseMs = *p.BackoffBaseMs
	}
	if p.BackoffMaxMs != nil {
		e.BackoffMaxMs = *p.BackoffMaxMs
	}
	if p.HTTPTimeoutMs != nil {
		e.HTTPTimeoutMs = *p.HTTPTimeoutMs
	}
}

func (s *Server) createEndpoint(c *gin.Context) {
	var req createEndpointReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	// SSRF guard: reject internal/reserved addresses at registration time.
	if err := s.guard.ValidateURL(req.URL); err != nil {
		abort(c, http.StatusBadRequest, errors.New("url_not_allowed: "+err.Error()))
		return
	}
	secret := req.Secret
	if secret == "" {
		var err error
		secret, err = signature.GenerateSecret()
		if err != nil {
			abort(c, http.StatusInternalServerError, err)
			return
		}
	}
	ep := &store.Endpoint{
		URL:              req.URL,
		Description:      req.Description,
		Secret:           secret,
		SubscribedEvents: req.SubscribedEvents,
	}
	applyPolicyDefaults(ep, req.RetryPolicy)
	created, err := s.store.CreateEndpoint(c.Request.Context(), ep)
	if err != nil {
		abort(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusCreated, endpointJSON(created, true))
}

func (s *Server) listEndpoints(c *gin.Context) {
	eps, err := s.store.ListEndpoints(c.Request.Context())
	if err != nil {
		abort(c, http.StatusInternalServerError, err)
		return
	}
	out := make([]gin.H, 0, len(eps))
	for i := range eps {
		out = append(out, endpointJSON(&eps[i], false))
	}
	c.JSON(http.StatusOK, gin.H{"endpoints": out})
}

func (s *Server) getEndpoint(c *gin.Context) {
	ep, err := s.store.GetEndpoint(c.Request.Context(), c.Param("id"))
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err)
		return
	}
	c.JSON(http.StatusOK, endpointJSON(ep, false))
}

type updateEndpointReq struct {
	Description      *string         `json:"description"`
	SubscribedEvents *[]string       `json:"subscribed_events"`
	Status           *string         `json:"status" binding:"omitempty,oneof=active disabled"`
	RetryPolicy      *retryPolicyReq `json:"retry_policy"`
}

// updateEndpoint patches mutable fields. The URL is deliberately NOT
// patchable: changing the receiver domain is a migration (new generation).
func (s *Server) updateEndpoint(c *gin.Context) {
	var req updateEndpointReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	patch := store.EndpointPatch{
		Description:      req.Description,
		SubscribedEvents: req.SubscribedEvents,
		Status:           req.Status,
	}
	if p := req.RetryPolicy; p != nil {
		patch.MaxAttempts = p.MaxAttempts
		patch.BackoffBaseMs = p.BackoffBaseMs
		patch.BackoffMaxMs = p.BackoffMaxMs
		patch.HTTPTimeoutMs = p.HTTPTimeoutMs
	}
	ep, err := s.store.UpdateEndpoint(c.Request.Context(), c.Param("id"), patch)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err)
		return
	}
	c.JSON(http.StatusOK, endpointJSON(ep, false))
}

type migrateEndpointReq struct {
	URL string `json:"url" binding:"required"`
}

// migrateEndpoint switches the lineage to a new receiver domain: the old
// generation is superseded and unsent deliveries move to the new generation,
// all in one transaction. In-flight attempts complete against their own
// (old-generation) delivery rows.
func (s *Server) migrateEndpoint(c *gin.Context) {
	var req migrateEndpointReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	if err := s.guard.ValidateURL(req.URL); err != nil {
		abort(c, http.StatusBadRequest, errors.New("url_not_allowed: "+err.Error()))
		return
	}
	ep, err := s.store.MigrateEndpoint(c.Request.Context(), c.Param("id"), req.URL)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err)
		return
	}
	c.JSON(http.StatusCreated, endpointJSON(ep, false))
}

func (s *Server) deleteEndpoint(c *gin.Context) {
	if err := s.store.DisableEndpoint(c.Request.Context(), c.Param("id")); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "disabled"})
}

type rotateSecretReq struct {
	// WindowSeconds bounds how long the PREVIOUS secret stays acceptable
	// after rotation. Default 1h, max 24h — old signatures are never kept
	// valid indefinitely.
	WindowSeconds *int `json:"window_seconds" binding:"omitempty,min=0,max=86400"`
}

func (s *Server) rotateSecret(c *gin.Context) {
	var req rotateSecretReq
	if c.Request.Body != nil && c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			abort(c, http.StatusBadRequest, err)
			return
		}
	}
	window := 3600
	if req.WindowSeconds != nil {
		window = *req.WindowSeconds
	}
	secret, err := signature.GenerateSecret()
	if err != nil {
		abort(c, http.StatusInternalServerError, err)
		return
	}
	ep, err := s.store.RotateSecret(c.Request.Context(), c.Param("id"), secret, window)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err)
		return
	}
	c.JSON(http.StatusOK, endpointJSON(ep, true))
}
