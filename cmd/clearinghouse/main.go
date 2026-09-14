package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/livepeer/clearinghouse/internal/app"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := app.Execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		slog.Error("clearinghouse failed", "error", err)
		os.Exit(1)
	}
}
