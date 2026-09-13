package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/example/webhookd/internal/store"
)

// ---------- events ----------

type publishEventReq struct {
	Type string `json:"type" binding:"required"`
	// BusinessKey scopes ordering: events with the same key are delivered
	// in sequence, different keys in parallel. Falls back to
	// data.business_key, then to the event id.
	BusinessKey string          `json:"business_key"`
	Data        json.RawMessage `json:"data" binding:"required"`
}

// publishEvent writes the event, assigns its per-key sequence number,
// matches subscriptions and creates pending deliveries in ONE transaction
// (transactional outbox). The Idempotency-Key header makes retries by the
// producer safe: the same key always returns the same event identity.
func (s *Server) publishEvent(c *gin.Context) {
	idemKey := c.GetHeader("Idempotency-Key")
	if idemKey == "" {
		abort(c, http.StatusBadRequest, errors.New("Idempotency-Key header is required"))
		return
	}
	var req publishEventReq
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, err)
		return
	}
	bizKey := req.BusinessKey
	if bizKey == "" {
		var p struct {
			BusinessKey string `json:"business_key"`
		}
		if err := json.Unmarshal(req.Data, &p); err == nil {
			bizKey = p.BusinessKey
		}
	}
	eventID := uuid.NewString()
	if bizKey == "" {
		bizKey = eventID // no grouping requested: every event is its own key
	}
	res, err := s.store.CreateEvent(c.Request.Context(), store.CreateEventParams{
		ID:             eventID,
		IdempotencyKey: idemKey,
		EventType:      req.Type,
		BusinessKey:    bizKey,
		Payload:        req.Data,
	})
	if err != nil {
		abort(c, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK
	}
	c.JSON(status, gin.H{
		"event":              res.Event,
		"deliveries_created": len(res.Deliveries),
		"deduplicated":       res.Duplicate,
	})
}

func (s *Server) getEvent(c *gin.Context) {
	ev, err := s.store.GetEvent(c.Request.Context(), c.Param("id"))
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err)
		return
	}
	dels, err := s.store.ListDeliveries(c.Request.Context(), store.DeliveryFilter{EventID: ev.ID})
	if err != nil {
		abort(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"event": ev, "deliveries": dels})
}

// ---------- deliveries ----------

func (s *Server) listDeliveries(c *gin.Context) {
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "100"))
	dels, err := s.store.ListDeliveries(c.Request.Context(), store.DeliveryFilter{
		EventID:     c.Query("event_id"),
		EndpointID:  c.Query("endpoint_id"),
		BusinessKey: c.Query("business_key"),
		Status:      c.Query("status"),
		Limit:       limit,
	})
	if err != nil {
		abort(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"deliveries": dels})
}

func (s *Server) getDelivery(c *gin.Context) {
	d, err := s.store.GetDelivery(c.Request.Context(), c.Param("id"))
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, store.ErrNotFound) {
			status = http.StatusNotFound
		}
		abort(c, status, err)
		return
	}
	attempts, err := s.store.ListAttempts(c.Request.Context(), d.ID)
	if err != nil {
		abort(c, http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"delivery": d, "attempts": attempts})
}

// replayDelivery re-queues a DEAD delivery. The original event row is
// reused as-is: replay never manufactures a new business fact.
func (s *Server) replayDelivery(c *gin.Context) {
	d, err := s.store.ReplayDeadDelivery(c.Request.Context(), c.Param("id"))
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			abort(c, http.StatusConflict, errors.New("delivery not found or not in dead state"))
		default:
			abort(c, http.StatusInternalServerError, err)
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{"delivery": d, "note": "re-queued with original event id " + d.EventID})
}

// skipDelivery records the HUMAN decision to skip a poison message. The
// reason is mandatory; the skipped sequence number remains a visible gap
// downstream (the event is never delivered).
func (s *Server) skipDelivery(c *gin.Context) {
	var req struct {
		Reason     string `json:"reason" binding:"required"`
		ResolvedBy string `json:"resolved_by"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		abort(c, http.StatusBadRequest, errors.New("a human skip decision requires a reason: "+err.Error()))
		return
	}
	d, err := s.store.SkipDeadDelivery(c.Request.Context(), c.Param("id"), req.Reason, req.ResolvedBy)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			abort(c, http.StatusConflict, errors.New("delivery not found or not in dead state"))
		default:
			abort(c, http.StatusInternalServerError, err)
		}
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"delivery": d,
		"note":     "skipped seq " + jsonNumber(d.KeySeq) + " of key " + d.BusinessKey + "; the gap stays visible downstream",
	})
}

func jsonNumber(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
