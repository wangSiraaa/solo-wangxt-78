package signature

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"
)

type vectorFile struct {
	Vectors []struct {
		Name      string `json:"name"`
		Secret    string `json:"secret"`
		Timestamp int64  `json:"timestamp"`
		Body      string `json:"body"`
		Signature string `json:"signature"`
		Header    string `json:"header"`
	} `json:"vectors"`
}

// TestVectors pins the signing scheme to fixed, language-neutral vectors
// (regenerate with: go run ./scripts/genvectors).
func TestVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vf vectorFile
	if err := json.Unmarshal(raw, &vf); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if len(vf.Vectors) == 0 {
		t.Fatal("no vectors")
	}
	for _, v := range vf.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			if got := Sign(v.Secret, v.Timestamp, []byte(v.Body)); got != v.Signature {
				t.Errorf("Sign mismatch:\n got %s\nwant %s", got, v.Signature)
			}
			if got := SignHeader(v.Secret, v.Timestamp, []byte(v.Body)); got != v.Header {
				t.Errorf("SignHeader mismatch:\n got %s\nwant %s", got, v.Header)
			}
			// A receiver at signing time must accept the vector.
			at := time.Unix(v.Timestamp, 0)
			if err := Verify(v.Secret, v.Header, []byte(v.Body), at, DefaultTolerance); err != nil {
				t.Errorf("Verify rejected valid vector: %v", err)
			}
		})
	}
}

func TestVerifyRejects(t *testing.T) {
	secret := "TEST_ONLY_WEBHOOK_VERIFY_SECRET"
	body := []byte(`{"id":"evt_1"}`)
	ts := time.Now().Unix()
	header := SignHeader(secret, ts, body)

	if err := Verify(secret, header, body, time.Now(), DefaultTolerance); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := Verify("TEST_ONLY_WRONG_WEBHOOK_SECRET", header, body, time.Now(), DefaultTolerance); !errors.Is(err, ErrSignatureMismatch) {
		t.Errorf("wrong secret: got %v, want ErrSignatureMismatch", err)
	}
	if err := Verify(secret, header, []byte(`{"id":"evt_2"}`), time.Now(), DefaultTolerance); !errors.Is(err, ErrSignatureMismatch) {
		t.Errorf("tampered body: got %v, want ErrSignatureMismatch", err)
	}
	if err := Verify(secret, "", body, time.Now(), DefaultTolerance); !errors.Is(err, ErrMissingSignature) {
		t.Errorf("missing header: got %v, want ErrMissingSignature", err)
	}
	if err := Verify(secret, "garbage", body, time.Now(), DefaultTolerance); !errors.Is(err, ErrInvalidSignatureHeader) {
		t.Errorf("garbage header: got %v, want ErrInvalidSignatureHeader", err)
	}
	// Timestamp outside the tolerance window (replay of an old capture).
	stale := SignHeader(secret, ts-3600, body)
	if err := Verify(secret, stale, body, time.Now(), DefaultTolerance); !errors.Is(err, ErrTimestampOutsideWindow) {
		t.Errorf("stale timestamp: got %v, want ErrTimestampOutsideWindow", err)
	}
	future := SignHeader(secret, ts+3600, body)
	if err := Verify(secret, future, body, time.Now(), DefaultTolerance); !errors.Is(err, ErrTimestampOutsideWindow) {
		t.Errorf("future timestamp: got %v, want ErrTimestampOutsideWindow", err)
	}
}
