package app

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/livepeer/clearinghouse/internal/serviceauth"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/stretchr/testify/require"
)

func TestHTTPHealthRoutes(t *testing.T) {
	for _, listener := range []struct {
		name    string
		handler func(context.Context, *store.Store, *serviceauth.Registry) http.Handler
	}{{"webhook", authWebhookHandler}, {"management", managementHandler}} {
		t.Run(listener.name, func(t *testing.T) {
			db, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "health.db"), true)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			handler := listener.handler(ctx, db, nil)
			check := func(path string, want int) {
				t.Helper()
				response := managementRequest(t, handler, "GET", path, "", "", "")
				require.Equal(t, want, response.Code, path)
				if want == http.StatusServiceUnavailable {
					require.Equal(t, "not ready\n", response.Body.String())
				}
			}
			check("/livez", http.StatusOK)
			check("/readyz", http.StatusOK)
			check("/healthz", http.StatusNotFound)
			cancel()
			check("/readyz", http.StatusServiceUnavailable)
			require.NoError(t, db.Close())
			handler = listener.handler(t.Context(), db, nil)
			check("/readyz", http.StatusServiceUnavailable)
		})
	}
}
