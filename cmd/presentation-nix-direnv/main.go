package main

import (
	"context"
	"embed"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "unknown"

//go:embed all:static
var staticFS embed.FS

// ready flips to true once the app has finished its startup work; until then
// readiness probes fail and traffic is held back by the kubelet.
var ready atomic.Bool

// shutdownTimeout bounds how long graceful shutdown waits for in-flight
// requests to drain before forcing the process down.
const shutdownTimeout = 10 * time.Second
const drainDelay = 5 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := net.JoinHostPort("", port)

	// Signal-aware context — cancel on SIGINT/SIGTERM so the shutdown path
	// below runs deterministically instead of relying on os.Exit from a signal
	// handler somewhere.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	staticRoot, err := fs.Sub(staticFS, "static")
	if err != nil {
		slog.Error("failed to init static root", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()

	// /livez — process is alive. Always 200 unless the goroutine is wedged.
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// /readyz — process is ready to serve traffic. Static content has no
	// dependency to warm, so this only reflects listener startup/shutdown.
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	fileServer := http.FileServer(http.FS(staticRoot))

	// Presenter mode (and its /presenter /overview /notes /entry routes) is
	// compiled out of the static build via `presenter: dev` in the slides.md
	// headmatter, so there is nothing presenter-related to guard here — those
	// paths just land on Slidev's client-side 404 page.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Slidev's router runs in HTML5 history mode (routes like /1, /2 for
		// slide numbers), so a direct load or refresh on those paths isn't a
		// real file — fall back to index.html and let the client-side router
		// take over instead of 404ing.
		p := strings.TrimPrefix(r.URL.Path, "/")
		if p == "" {
			p = "."
		}
		if _, err := fs.Stat(staticRoot, p); err != nil {
			r = r.Clone(r.Context())
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() && r.URL.Path != "/livez" && r.URL.Path != "/readyz" {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}
		mux.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("starting presentation-nix-direnv", "version", version, "addr", addr)
		ready.Store(true)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			slog.Error("server failed", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
		stop()
		slog.Info("shutdown signal received, draining then stopping", "drain", drainDelay, "timeout", shutdownTimeout)
		ready.Store(false)
		time.Sleep(drainDelay)

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("graceful shutdown failed", "error", err)
			os.Exit(1)
		}
		slog.Info("server stopped cleanly")
	}
}
