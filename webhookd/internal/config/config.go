// Package config loads service configuration from the environment.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL string
	HTTPAddr    string

	DeliveryWorkers int
	PollInterval    time.Duration

	// AllowPrivateHosts explicitly permits endpoint URLs pointing at these
	// hosts/CIDRs (e.g. "127.0.0.1/32,::1/128"). For local tests/dev ONLY —
	// keep empty in production so loopback admin ports and internal reserved
	// ranges stay unreachable.
	AllowPrivateHosts []string

	// EnableTestkit mounts the local failure-simulation receiver at /testkit.
	EnableTestkit bool
}

func Load() Config {
	return Config{
		DatabaseURL:       env("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/webhookd?sslmode=disable"),
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		DeliveryWorkers:   envInt("DELIVERY_WORKERS", 4),
		PollInterval:      time.Duration(envInt("POLL_INTERVAL_MS", 500)) * time.Millisecond,
		AllowPrivateHosts: envCSV("ALLOW_PRIVATE_HOSTS"),
		EnableTestkit:     env("ENABLE_TESTKIT", "true") == "true",
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envCSV(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
