// Package testkit is the explicitly-allowed LOCAL receiver used by tests and
// demos. It verifies signatures like a real customer receiver, simulates
// failure modes (500, 429, timeout, dropped connection, redirect), and keeps
// counters that demonstrate the difference between a duplicate DELIVERY and
// a duplicate BUSINESS effect.
//
// Mount it only in dev/test (config.EnableTestkit).
package testkit

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/example/webhookd/internal/signature"
)

// Receiver is a reference webhook receiver with failure simulation.
type Receiver struct {
	mu sync.Mutex
	// deliveries: event id -> number of times the event was delivered here.
	deliveries map[string]int
	// business: business key -> number of times the business effect ran.
	// A correct receiver applies the effect at most once per event id, so
	// duplicate deliveries never inflate this counter.
	business map[string]int
	// modes: simulation key -> mode (ok, fail500, fail429, flaky:N,
	// flaky429:N, timeout, drop, redirect).
	modes map[string]string
	// flakyLeft: key -> failures remaining before succeeding.
	flakyLeft map[string]int
	// SleepDuration for mode=timeout (must exceed the endpoint's HTTP timeout).
	SleepDuration time.Duration
}

func NewReceiver() *Receiver {
	return &Receiver{
		deliveries:    map[string]int{},
		business:      map[string]int{},
		modes:         map[string]string{},
		flakyLeft:     map[string]int{},
		SleepDuration: 3 * time.Second,
	}
}

// Mount registers the testkit routes on r.
func (rc *Receiver) Mount(r gin.IRouter) {
	r.POST("/testkit/receive", rc.handleReceive)
	r.GET("/testkit/state", rc.handleState)
	r.POST("/testkit/mode", rc.handleSetMode)
	r.POST("/testkit/reset", rc.handleReset)
}

// handleReceive is the webhook endpoint.
// Query params: secret (endpoint secret to verify against), mode, key
// (simulation key shared with /testkit/mode and /testkit/state).
func (rc *Receiver) handleReceive(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body: " + err.Error()})
		return
	}
	secret := c.Query("secret")
	if secret == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing ?secret="})
		return
	}
	// 1. Signature + timestamp window verification (what a real receiver does).
	if err := signature.Verify(secret, c.GetHeader(signature.HeaderSignature), body,
		time.Now(), signature.DefaultTolerance); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "signature verification failed: " + err.Error()})
		return
	}
	eventID := c.GetHeader(signature.HeaderID)
	if eventID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing Webhook-Id header"})
		return
	}
	var env struct {
		ID   string          `json:"id"`
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid envelope"})
		return
	}
	if env.ID != eventID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Webhook-Id header does not match body id"})
		return
	}

	mode := rc.modeFor(c.Query("key"), c.Query("mode"))

	// 2. Failure simulation hooks run BEFORE any business processing.
	switch {
	case mode == "fail500":
		c.JSON(http.StatusInternalServerError, gin.H{"error": "simulated 500"})
		return
	case mode == "fail429":
		c.Header("Retry-After", "0")
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "simulated 429"})
		return
	case mode == "redirect":
		c.Redirect(http.StatusFound, "/testkit/receive?secret="+secret+"&mode=ok")
		return
	case strings.HasPrefix(mode, "flaky:") || strings.HasPrefix(mode, "flaky429:"):
		if rc.consumeFlaky(c.Query("key")) {
			if strings.HasPrefix(mode, "flaky429:") {
				c.Header("Retry-After", "0")
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "simulated flaky 429"})
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "simulated flaky 500"})
			}
			return
		}
	}

	// 3. Idempotent business processing: the SAME event id never applies the
	//    business effect twice, no matter how many times it is delivered.
	duplicate := rc.applyOnce(eventID, businessKey(env.Data, eventID))

	// 4. Post-processing failure simulation. "drop"/"timeout" always trigger;
	// "drop:N"/"timeout:N" trigger only for the next N deliveries of this key.
	switch {
	case mode == "drop" || (strings.HasPrefix(mode, "drop:") && rc.consumeFlaky(c.Query("key"))):
		// Process the event, then kill the connection without any response:
		// the dispatcher sees a transport error and retries — a classic
		// "response lost" scenario.
		rc.hijackClose(c)
		return
	case mode == "timeout" || (strings.HasPrefix(mode, "timeout:") && rc.consumeFlaky(c.Query("key"))):
		// Process the event, then answer too slowly: the dispatcher times out
		// and retries with the same event id.
		time.Sleep(rc.SleepDuration)
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "duplicate": duplicate})
}

// applyOnce records the delivery and applies the business effect at most
// once per event id. Returns true when this delivery was a duplicate.
func (rc *Receiver) applyOnce(eventID, bizKey string) (duplicate bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.deliveries[eventID]++
	if rc.deliveries[eventID] > 1 {
		return true // duplicate DELIVERY detected -> no duplicate BUSINESS effect
	}
	rc.business[bizKey]++
	return false
}

func (rc *Receiver) modeFor(key, inline string) string {
	if inline != "" {
		return inline
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.modes[key]
}

func (rc *Receiver) consumeFlaky(key string) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.flakyLeft[key] > 0 {
		rc.flakyLeft[key]--
		return true
	}
	return false
}

// hijackClose closes the underlying connection without writing a response.
func (rc *Receiver) hijackClose(c *gin.Context) {
	conn, _, err := c.Writer.(http.Hijacker).Hijack()
	if err == nil {
		conn.Close() //nolint:errcheck
	}
}

func (rc *Receiver) handleState(c *gin.Context) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	dups := 0
	for _, n := range rc.deliveries {
		if n > 1 {
			dups += n - 1
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"deliveries":          rc.deliveries,
		"duplicate_deliveries": dups,
		"business_effects":    rc.business,
	})
}

func (rc *Receiver) handleSetMode(c *gin.Context) {
	var req struct {
		Key   string `json:"key"`
		Mode  string `json:"mode"`
		Flaky int    `json:"flaky"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.modes[req.Key] = req.Mode
	// Any "<mode>:N" suffix arms the one-shot counter for that key.
	if parts := strings.Split(req.Mode, ":"); len(parts) == 2 {
		if n, err := strconv.Atoi(parts[1]); err == nil && n > 0 {
			rc.flakyLeft[req.Key] = n
		}
	}
	if req.Flaky > 0 {
		rc.flakyLeft[req.Key] = req.Flaky
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (rc *Receiver) handleReset(c *gin.Context) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.deliveries = map[string]int{}
	rc.business = map[string]int{}
	rc.modes = map[string]string{}
	rc.flakyLeft = map[string]int{}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// businessKey extracts a stable business idempotency key from the payload,
// falling back to the event id.
func businessKey(data json.RawMessage, eventID string) string {
	var p struct {
		BusinessKey string `json:"business_key"`
	}
	if err := json.Unmarshal(data, &p); err == nil && p.BusinessKey != "" {
		return p.BusinessKey
	}
	return eventID
}
