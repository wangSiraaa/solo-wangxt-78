// Command receiver is a minimal standalone example of a CUSTOMER webhook
// receiver: it verifies the signature, enforces the timestamp window and
// deduplicates on the event id, keeping duplicate deliveries distinct from
// duplicate business processing.
//
//	WEBHOOK_SECRET=whsec_... PORT=9090 go run ./cmd/receiver
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/example/webhookd/internal/signature"
)

type state struct {
	mu         sync.Mutex
	deliveries map[string]int // event id -> delivery count
	business   map[string]int // business key -> applied count
}

func main() {
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		log.Fatal("WEBHOOK_SECRET is required")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}
	st := &state{deliveries: map[string]int{}, business: map[string]int{}}

	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		// 1. Verify signature AND timestamp window.
		if err := signature.Verify(secret, r.Header.Get(signature.HeaderSignature), body,
			time.Now(), signature.DefaultTolerance); err != nil {
			http.Error(w, "signature verification failed: "+err.Error(), http.StatusUnauthorized)
			return
		}
		// 2. Event id is the dedup key.
		eventID := r.Header.Get(signature.HeaderID)
		var env struct {
			ID   string          `json:"id"`
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &env); err != nil || env.ID == "" || env.ID != eventID {
			http.Error(w, "invalid envelope", http.StatusBadRequest)
			return
		}
		var payload struct {
			BusinessKey string `json:"business_key"`
		}
		json.Unmarshal(env.Data, &payload) //nolint:errcheck
		bizKey := payload.BusinessKey
		if bizKey == "" {
			bizKey = eventID
		}

		// 3. Duplicate DELIVERY (same event id again) must not cause a
		//    duplicate BUSINESS effect.
		st.mu.Lock()
		st.deliveries[eventID]++
		duplicate := st.deliveries[eventID] > 1
		if !duplicate {
			st.business[bizKey]++ // the actual business effect, applied once
		}
		st.mu.Unlock()

		log.Printf("event=%s type=%s duplicate=%v -> business[%q] applied once", eventID, env.Type, duplicate, bizKey)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": "ok", "duplicate": duplicate}) //nolint:errcheck
	})

	http.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		defer st.mu.Unlock()
		dups := 0
		for _, n := range st.deliveries {
			if n > 1 {
				dups += n - 1
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"deliveries":           st.deliveries,
			"duplicate_deliveries": dups,
			"business_effects":     st.business,
		})
	})

	log.Printf("receiver listening on :%s (POST /webhook, GET /state)", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
