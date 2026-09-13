// Package signature implements the webhook signing scheme.
//
// Wire format (Stripe-style):
//
//	Webhook-Signature: t=<unix-seconds>,v1=<hex-hmac-sha256>
//
// The signed payload is "<t>.<raw-body>" and the key is the endpoint secret
// (whsec_...). Receivers MUST additionally enforce a timestamp tolerance
// window so captured signatures cannot be replayed indefinitely.
package signature

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	HeaderID        = "Webhook-Id"
	HeaderTimestamp = "Webhook-Timestamp"
	HeaderSignature = "Webhook-Signature"

	// DefaultTolerance is the maximum accepted clock skew between sender and
	// receiver. Signatures older (or newer) than this are rejected.
	DefaultTolerance = 5 * time.Minute
)

var (
	ErrMissingSignature       = errors.New("missing signature header")
	ErrInvalidSignatureHeader = errors.New("invalid signature header")
	ErrTimestampOutsideWindow = errors.New("timestamp outside tolerance window")
	ErrSignatureMismatch      = errors.New("signature mismatch")
)

// GenerateSecret returns a new endpoint secret: "whsec_" + 32 random bytes hex.
func GenerateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate secret: %w", err)
	}
	return "whsec_" + hex.EncodeToString(b), nil
}

// Sign computes hex(HMAC-SHA256(secret, "<ts>.<body>")).
func Sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// SignHeader builds the full Webhook-Signature header value.
func SignHeader(secret string, ts int64, body []byte) string {
	return fmt.Sprintf("t=%d,v1=%s", ts, Sign(secret, ts, body))
}

// Verify checks the header against the body and the timestamp window.
// A zero tolerance skips the time-window check (not recommended).
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	if header == "" {
		return ErrMissingSignature
	}
	ts, sigs, err := parseHeader(header)
	if err != nil {
		return err
	}
	if tolerance > 0 {
		age := now.Sub(time.Unix(ts, 0))
		if age < -tolerance || age > tolerance {
			return ErrTimestampOutsideWindow
		}
	}
	expected := Sign(secret, ts, body)
	for _, s := range sigs {
		if hmac.Equal([]byte(s), []byte(expected)) {
			return nil
		}
	}
	return ErrSignatureMismatch
}

// VerifyWithRotation verifies against the current secret, falling back to
// the previous secret ONLY while the rotation window is still open
// (now.Before(previousExpiresAt)). Once the window closes, old signatures
// are rejected — rotated secrets are never kept valid indefinitely.
func VerifyWithRotation(current, previous string, previousExpiresAt time.Time,
	header string, body []byte, now time.Time, tolerance time.Duration) error {
	err := Verify(current, header, body, now, tolerance)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrSignatureMismatch) && previous != "" && now.Before(previousExpiresAt) {
		return Verify(previous, header, body, now, tolerance)
	}
	return err
}

// parseHeader parses "t=...,v1=...,v1=..." (extra keys are ignored so the
// scheme can be extended with new versions).
func parseHeader(h string) (int64, []string, error) {
	var (
		ts   int64
		sigs []string
	)
	for _, part := range strings.Split(h, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return 0, nil, ErrInvalidSignatureHeader
		}
		switch kv[0] {
		case "t":
			v, err := strconv.ParseInt(kv[1], 10, 64)
			if err != nil {
				return 0, nil, ErrInvalidSignatureHeader
			}
			ts = v
		case "v1":
			if _, err := hex.DecodeString(kv[1]); err != nil || len(kv[1]) != 64 {
				return 0, nil, ErrInvalidSignatureHeader
			}
			sigs = append(sigs, kv[1])
		}
	}
	if ts == 0 || len(sigs) == 0 {
		return 0, nil, ErrInvalidSignatureHeader
	}
	return ts, sigs, nil
}
