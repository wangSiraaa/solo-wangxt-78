// Package testkit is the explicitly-allowed LOCAL receiver used by tests and
// demos. It verifies signatures like a real customer receiver (including the
// bounded dual-secret rotation window), simulates failure modes, and tracks
// per-business-key sequences, gaps and concurrency so tests can assert
// ordering and parallelism.
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

type secretSet struct {
	current      string
	previous     string
	prevExpires  time.Time
}

// Receiver is a reference webhook receiver with failure simulation.
type Receiver struct {
	mu sync.Mutex
	// deliveries: event id -> number of times the event was delivered here.
	deliveries map[string]int
	// business: business key -> number of times the business effect ran.
	business map[string]int
	// attemptSeqs: business key -> seqs of every signature-valid request.
	attemptSeqs map[string][]int64
	// processedSeqs: business key -> seqs whose business effect ran (once
	// per event id). Gaps in this sequence are visible downstream.
	processedSeqs map[string][]int64
	// concurrency tracking (per key and total).
	inflight       map[string]int
	inflightTotal  int
	maxInflightKey int
	maxInflightAll int
	// secrets registered via /testkit/secret (rotation-aware).
	secrets map[string]secretSet
	// modes: simulation key -> mode.
	modes map[string]string
	// flakyLeft: key -> failures remaining before succeeding.
	flakyLeft map[string]int
	// SleepDuration for mode=timeout and mode=slow.
	SleepDuration time.Duration
	SlowDuration  time.Duration
}

func NewReceiver() *Receiver {
	return &Receiver{
		deliveries:    map[string]int{},
		business:      map[string]int{},
		attemptSeqs:   map[string][]int64{},
		processedSeqs: map[string][]int64{},
		inflight:      map[string]int{},
		secrets:       map[string]secretSet{},
		modes:         map[string]string{},
		flakyLeft:     map[string]int{},
		SleepDuration: 3 * time.Second,
		SlowDuration:  100 * time.Millisecond,
	}
}

// Mount registers the testkit routes on r.
func (rc *Receiver) Mount(r gin.IRouter) {
	r.POST("/testkit/receive", rc.handleReceive)
	r.GET("/testkit/state", rc.handleState)
	r.POST("/testkit/mode", rc.handleSetMode)
	r.POST("/testkit/secret", rc.handleSetSecret)
	r.POST("/testkit/reset", rc.handleReset)
}

type envelope struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	BusinessKey string          `json:"business_key"`
	Seq         int64           `json:"seq"`
	Data        json.RawMessage `json:"data"`
}

// handleReceive is the webhook endpoint.
// Query params: key (simulation/secret-store key), mode, or inline secret
// (+ prev_secret, prev_expires unix) for simple cases.
func (rc *Receiver) handleReceive(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body: " + err.Error()})
		return
	}
	key := c.Query("key")
	current, previous, prevExp := rc.secretsFor(key)
	if inline := c.Query("secret"); inline != "" {
		current = inline
		previous = c.Query("prev_secret")
		if exp, err := strconv.ParseInt(c.Query("prev_expires"), 10, 64); err == nil {
			prevExp = time.Unix(exp, 0)
		}
	}
	if current == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no secret: pass ?secret= or register one via /testkit/secret"})
		return
	}
	// 1. Signature + timestamp window + bounded rotation window.
	if err := signature.VerifyWithRotation(current, previous, prevExp,
		c.GetHeader(signature.HeaderSignature), body, time.Now(), signature.DefaultTolerance); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "signature verification failed: " + err.Error()})
		return
	}
	eventID := c.GetHeader(signature.HeaderID)
	if eventID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing Webhook-Id header"})
		return
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil || env.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid envelope"})
		return
	}
	if env.ID != eventID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Webhook-Id header does not match body id"})
		return
	}
	bizKey := env.BusinessKey
	if bizKey == "" {
		bizKey = eventID
	}
	rc.recordAttemptSeq(bizKey, env.Seq)

	mode := rc.modeFor(key, c.Query("mode"))

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
		c.Redirect(http.StatusFound, "/testkit/receive?key="+key)
		return
	case strings.HasPrefix(mode, "flaky:") || strings.HasPrefix(mode, "flaky429:"):
		if rc.consumeFlaky(key) {
			if strings.HasPrefix(mode, "flaky429:") {
				c.Header("Retry-After", "0")
				c.JSON(http.StatusTooManyRequests, gin.H{"error": "simulated flaky 429"})
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "simulated flaky 500"})
			}
			return
		}
	}

	// 3. Idempotent business processing under concurrency tracking.
	rc.enter(bizKey)
	duplicate := rc.applyOnce(eventID, bizKey, env.Seq)
	if mode == "slow" {
		time.Sleep(rc.SlowDuration)
	}

	// 4. Post-processing failure simulation. "drop"/"timeout" always trigger;
	// "drop:N"/"timeout:N" trigger only for the next N deliveries of this key.
	switch {
	case mode == "drop" || (strings.HasPrefix(mode, "drop:") && rc.consumeFlaky(key)):
		// Process the event, then kill the connection without any response:
		// the dispatcher sees a transport error and retries — a classic
		// "response lost" scenario.
		rc.exit(bizKey)
		rc.hijackClose(c)
		return
	case mode == "timeout" || (strings.HasPrefix(mode, "timeout:") && rc.consumeFlaky(key)):
		// Process the event, then answer too slowly: the dispatcher times out
		// and retries with the same event id.
		time.Sleep(rc.SleepDuration)
	}
	rc.exit(bizKey)
	c.JSON(http.StatusOK, gin.H{"status": "ok", "duplicate": duplicate})
}

func (rc *Receiver) recordAttemptSeq(key string, seq int64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.attemptSeqs[key] = append(rc.attemptSeqs[key], seq)
}

func (rc *Receiver) enter(key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.inflight[key]++
	rc.inflightTotal++
	if rc.inflight[key] > rc.maxInflightKey {
		rc.maxInflightKey = rc.inflight[key]
	}
	if rc.inflightTotal > rc.maxInflightAll {
		rc.maxInflightAll = rc.inflightTotal
	}
}

func (rc *Receiver) exit(key string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.inflight[key]--
	rc.inflightTotal--
}

// applyOnce records the delivery and applies the business effect at most
// once per event id. Returns true when this delivery was a duplicate.
func (rc *Receiver) applyOnce(eventID, bizKey string, seq int64) (duplicate bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.deliveries[eventID]++
	if rc.deliveries[eventID] > 1 {
		return true // duplicate DELIVERY detected -> no duplicate BUSINESS effect
	}
	rc.business[bizKey]++
	rc.processedSeqs[bizKey] = append(rc.processedSeqs[bizKey], seq)
	return false
}

func (rc *Receiver) secretsFor(key string) (current, previous string, prevExp time.Time) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if s, ok := rc.secrets[key]; ok {
		return s.current, s.previous, s.prevExpires
	}
	return "", "", time.Time{}
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
	gaps := map[string][]int64{}
	for key, seqs := range rc.processedSeqs {
		gaps[key] = findGaps(seqs)
	}
	c.JSON(http.StatusOK, gin.H{
		"deliveries":           rc.deliveries,
		"duplicate_deliveries": dups,
		"business_effects":     rc.business,
		"attempt_sequences":    rc.attemptSeqs,
		"processed_sequences":  rc.processedSeqs,
		"sequence_gaps":        gaps,
		"max_concurrent_per_key": rc.maxInflightKey,
		"max_concurrent_total":   rc.maxInflightAll,
	})
}

// findGaps returns the sequence numbers missing between 1 and max: per-key
// sequences start at 1, so any missing number below the highest processed
// seq is a visible gap (e.g. a skipped poison message).
func findGaps(seqs []int64) []int64 {
	if len(seqs) == 0 {
		return nil
	}
	seen := map[int64]bool{}
	var hi int64
	for _, s := range seqs {
		seen[s] = true
		if s > hi {
			hi = s
		}
	}
	var gaps []int64
	for s := int64(1); s <= hi; s++ {
		if !seen[s] {
			gaps = append(gaps, s)
		}
	}
	return gaps
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

// handleSetSecret registers the secret(s) a simulation key verifies against,
// including the previous secret and its expiry for rotation-window tests.
func (rc *Receiver) handleSetSecret(c *gin.Context) {
	var req struct {
		Key                 string `json:"key" binding:"required"`
		Secret              string `json:"secret" binding:"required"`
		PreviousSecret      string `json:"previous_secret"`
		PreviousExpiresUnix int64  `json:"previous_expires_unix"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.secrets[req.Key] = secretSet{
		current:     req.Secret,
		previous:    req.PreviousSecret,
		prevExpires: time.Unix(req.PreviousExpiresUnix, 0),
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (rc *Receiver) handleReset(c *gin.Context) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.deliveries = map[string]int{}
	rc.business = map[string]int{}
	rc.attemptSeqs = map[string][]int64{}
	rc.processedSeqs = map[string][]int64{}
	rc.inflight = map[string]int{}
	rc.inflightTotal = 0
	rc.maxInflightKey = 0
	rc.maxInflightAll = 0
	rc.secrets = map[string]secretSet{}
	rc.modes = map[string]string{}
	rc.flakyLeft = map[string]int{}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
