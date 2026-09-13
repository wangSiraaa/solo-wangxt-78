// Command server runs the webhook delivery service: management API +
// delivery workers, both backed by PostgreSQL.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/webhookd/internal/config"
	"github.com/example/webhookd/internal/dispatcher"
	"github.com/example/webhookd/internal/httpapi"
	"github.com/example/webhookd/internal/ssrf"
	"github.com/example/webhookd/internal/store"
	"github.com/example/webhookd/internal/testkit"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	guard, err := ssrf.New(cfg.AllowPrivateHosts)
	if err != nil {
		log.Fatalf("ssrf guard: %v", err)
	}
	if len(cfg.AllowPrivateHosts) > 0 {
		log.Printf("WARNING: private-host allowlist active (dev/test only): %v", cfg.AllowPrivateHosts)
	}

	disp := dispatcher.New(st, guard, cfg.DeliveryWorkers, cfg.PollInterval)
	go disp.Run(ctx)

	var tk *testkit.Receiver
	if cfg.EnableTestkit {
		tk = testkit.NewReceiver()
		log.Printf("testkit receiver mounted at /testkit (disable with ENABLE_TESTKIT=false)")
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewServer(st, guard, tk).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("webhookd listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx) //nolint:errcheck
}
