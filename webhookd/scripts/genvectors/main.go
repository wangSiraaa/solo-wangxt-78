// Command genvectors regenerates internal/signature/testdata/vectors.json.
// The vectors are fixed constants so any implementation (Go or otherwise)
// can verify interoperability of the signing scheme.
//
//	go run ./scripts/genvectors > internal/signature/testdata/vectors.json
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/example/webhookd/internal/signature"
)

type vector struct {
	Name      string `json:"name"`
	Secret    string `json:"secret"`
	Timestamp int64  `json:"timestamp"`
	Body      string `json:"body"`
	Signature string `json:"signature"`
	Header    string `json:"header"`
}

func main() {
	bodies := []struct {
		name   string
		secret string
		ts     int64
		body   string
	}{
		{
			name:   "basic-order-event",
			secret: "whsec_0000000000000000000000000000000000000000000000000000000000000001",
			ts:     1694515200,
			body:   `{"id":"evt_0190a8b0-7a2b-7c3d-8e4f-000000000001","type":"order.created","created_at":"2023-09-12T10:00:00Z","data":{"business_key":"order-42","amount":100}}`,
		},
		{
			name:   "empty-json-object",
			secret: "whsec_abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
			ts:     1700000000,
			body:   `{}`,
		},
		{
			name:   "unicode-payload",
			secret: "whsec_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			ts:     1757664000,
			body:   `{"id":"evt_unicode","type":"user.updated","data":{"name":"张三","emoji":"🚀"}}`,
		},
	}
	vectors := make([]vector, 0, len(bodies))
	for _, b := range bodies {
		vectors = append(vectors, vector{
			Name:      b.name,
			Secret:    b.secret,
			Timestamp: b.ts,
			Body:      b.body,
			Signature: signature.Sign(b.secret, b.ts, []byte(b.body)),
			Header:    signature.SignHeader(b.secret, b.ts, []byte(b.body)),
		})
	}
	out, err := json.MarshalIndent(map[string]any{
		"scheme":  "Webhook-Signature: t=<unix>,v1=<hex-hmac-sha256(secret, '<t>.<body>')>",
		"vectors": vectors,
	}, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(os.Stdout, string(out))
}
