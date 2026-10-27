package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestRegistrySnapshotsAndBoundedRoutes(t *testing.T) {
	for i := 0; i < 2; i++ {
		m := New("test", "commit")
		mux := http.NewServeMux()
		mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(201) })
		h := m.HTTP("management", mux)
		for _, path := range []string{"/items/secret-a", "/items/secret-b", "/unknown-secret"} {
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
		}
		r := httptest.NewRecorder()
		m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
		require.Equal(t, 200, r.Code)
		require.NotContains(t, r.Body.String(), "secret")
		require.Contains(t, r.Body.String(), "GET /items/{id}")
		require.Contains(t, r.Body.String(), `http_requests_in_flight{listener="management"} 0`)
		m.Snapshot("test", []prometheus.Metric{Gauge("test_balance", 2, nil)})
		m.Snapshot("test", nil)
		families, err := m.Registry.Gather()
		require.NoError(t, err)
		for _, f := range families {
			require.NotEqual(t, Prefix+"test_balance", f.GetName())
		}
	}
}
func TestConcurrentGatherAndUpdates(t *testing.T) {
	m := New("test", "commit")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 30; j++ {
				m.Add("test_total", 1, nil)
				m.Snapshot("test", []prometheus.Metric{Gauge("test_value", 1, nil)})
				_, err := m.Registry.Gather()
				require.NoError(t, err)
			}
		})
	}
	wg.Wait()
}
