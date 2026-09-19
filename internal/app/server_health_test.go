package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/stretchr/testify/require"
)

func TestAuthWebhookHealthRoutes(t *testing.T) {
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "health.db"), true)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	handler := authWebhookHandler(context.Background(), db, "test-token")
	for _, tc := range []struct {
		path string
		want int
	}{{"/livez", http.StatusOK}, {"/readyz", http.StatusOK}, {"/healthz", http.StatusNotFound}} {
		t.Run(tc.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
			require.Equal(t, tc.want, response.Code)
		})
	}
}

func TestAuthWebhookReadinessFailures(t *testing.T) {
	t.Run("shutdown", func(t *testing.T) {
		db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "shutdown.db"), true)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		response := httptest.NewRecorder()
		authWebhookHandler(ctx, db, "test-token").ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.Equal(t, "not ready\n", response.Body.String())
	})

	t.Run("database unavailable", func(t *testing.T) {
		db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "closed.db"), true)
		require.NoError(t, err)
		require.NoError(t, db.Close())

		response := httptest.NewRecorder()
		authWebhookHandler(context.Background(), db, "test-token").ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.Equal(t, "not ready\n", response.Body.String())
	})
}
