// Command opencode-collector is a lightweight daemon that reads local
// OpenCode SQLite source databases and pushes usage telemetry to the
// OpenCode Gateway.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/opencode-gateway/collectors/internal/collector"
	"github.com/opencode-gateway/collectors/internal/config"
)

// Version is the collector version string. It is injected at build time
// via ldflags, or defaults to "dev" for development builds.
var Version = "dev"

func main() {
	// -version flag prints version and exits before any other setup.
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("opencode-collector v" + Version)
		os.Exit(0)
	}

	// Load configuration from environment variables.
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Create the collector — wires all components together and resolves
	// the client hostname once at startup.
	c, err := collector.NewCollector(cfg, Version)
	if err != nil {
		slog.Error("failed to create collector", "error", err)
		os.Exit(1)
	}

	// Create a context that is cancelled when SIGINT or SIGTERM is received.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Run the collector in a goroutine so we can implement a 30s grace
	// period for shutdown. Run() blocks until ctx is cancelled, then it
	// returns after the current iteration completes and in-flight POSTs
	// finish (thanks to context.WithoutCancel used internally).
	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	// Wait for either Run() to return on its own, or a signal.
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			slog.Error("collector exited with error", "error", err)
			os.Exit(1)
		}
		return

	case <-ctx.Done():
		slog.Info("shutdown signal received, allowing in-flight work to complete",
			"grace_period", "30s",
		)

		// Wait up to 30 seconds for Run() to finish.
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()

		select {
		case err := <-done:
			if err != nil && err != context.Canceled {
				slog.Error("collector exited with error", "error", err)
				os.Exit(1)
			}
			slog.Info("graceful shutdown complete")
			return

		case <-timer.C:
			slog.Error("shutdown timed out after 30s grace period, forcing exit")
			os.Exit(1)
		}
	}
}
