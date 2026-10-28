package app

import (
	"context"
	"fmt"
	"github.com/livepeer/clearinghouse/internal/chain"
	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/livepeer/clearinghouse/internal/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"net"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMetricsBindValidation(t *testing.T) {
	p := ServeParams{MaxKafkaLag: time.Minute, EnableMetrics: mustHTTPBind(t, "0.0.0.0:9094"), EnableKafka: true, CredsFile: "unused", KafkaBind: "127.0.0.1:9092", KafkaTopic: "events"}
	require.ErrorContains(t, p.Validate(), "metrics bind")
	p.UnsafeHTTPBind = true
	require.NoError(t, p.Validate())
	p.EnableAuthWebhook = p.EnableMetrics
	require.ErrorContains(t, p.Validate(), "separate TCP port")
}

func TestMonitorRefreshFailureRecoveryAndOwnership(t *testing.T) {
	for _, mode := range []string{"accounting", "chain", "http"} {
		t.Run(mode, func(t *testing.T) {
			f := testutil.New(t, "100")
			m := metrics.New("test", "commit")
			p := ServeParams{Common: Common{DBPath: f.Path}, EnableAccounting: mode == "accounting", EnableOnchainListener: mode == "chain"}
			if mode == "http" {
				p.EnableAuthWebhook = mustHTTPBind(t, testutil.Port(t))
			}
			rename := func(from, to string) {
				_, err := f.DB.DB.Exec("ALTER TABLE " + from + " RENAME TO " + to)
				require.NoError(t, err)
			}
			rename("usage_events", "hidden_usage_events")
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan struct{})
			go func() {
				defer close(done)
				monitorMetricsEvery(ctx, p, f.DB, &kafka.Listener{DB: f.DB, Topic: "events"}, &chain.Listener{}, m, 5*time.Millisecond)
			}()
			defer func() { cancel(); <-done }()
			labels := map[string]string{"collector": "database"}
			wait := func(success float64) {
				testutil.Eventually(t, func() bool {
					return testutil.HasMetric(t, m, "collector_success", labels) && testutil.MetricValue(t, m, "collector_success", labels) == success
				})
				if mode == "http" {
					testutil.Eventually(t, func() bool {
						return testutil.HasMetric(t, m, "component_ready") && testutil.MetricValue(t, m, "component_ready", map[string]string{"component": "webhook"}) == success
					})
				}
			}
			wait(0)
			require.False(t, testutil.HasMetric(t, m, "collector_last_success_timestamp_seconds", labels))
			rename("hidden_usage_events", "usage_events")
			wait(1)
			require.Equal(t, mode == "accounting", testutil.HasMetric(t, m, "allocation_available_eth"))
			require.Equal(t, mode == "chain", testutil.HasMetric(t, m, "settlements"))
			last := testutil.MetricValue(t, m, "collector_last_success_timestamp_seconds", labels)
			require.Greater(t, last, 0.)
			rename("usage_events", "hidden_usage_events")
			wait(0)
			require.False(t, testutil.HasMetric(t, m, "allocation_available_eth"))
			require.False(t, testutil.HasMetric(t, m, "settlements"))
			require.GreaterOrEqual(t, testutil.MetricValue(t, m, "collector_last_success_timestamp_seconds", labels), last)
			rename("hidden_usage_events", "usage_events")
			wait(1)
			if mode == "accounting" {
				require.InDelta(t, 100e-18, testutil.MetricValue(t, m, "allocation_available_eth", nil), 1e-30)
			}
		})
	}
}

func TestMonitoringDoesNotWaitForPrimaryConnection(t *testing.T) {
	for _, accounting := range []bool{false, true} {
		t.Run(fmt.Sprint(accounting), func(t *testing.T) {
			f := testutil.New(t, "100")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			broker, err := kafka.OpenBroker(ctx, testutil.Port(t), "", "events", t.TempDir(), nil)
			require.NoError(t, err)
			brokerDone := make(chan error, 1)
			go func() { brokerDone <- broker.Serve(ctx) }()
			defer func() { cancel(); broker.Close(); require.NoError(t, <-brokerDone) }()
			held, err := f.DB.DB.Conn(ctx)
			require.NoError(t, err)
			defer held.Close()
			m := metrics.New("test", "commit")
			p := ServeParams{Common: Common{DBPath: f.Path}, EnableAccounting: accounting}
			if !accounting {
				p.EnableAuthWebhook = mustHTTPBind(t, testutil.Port(t))
			}
			k := &kafka.Listener{DB: f.DB, Broker: broker.Addr(), Topic: "events"}
			done := make(chan struct{})
			go func() {
				defer close(done)
				monitorMetricsEvery(ctx, p, f.DB, k, &chain.Listener{}, m, time.Millisecond)
			}()
			defer func() { cancel(); <-done }()
			require.Eventually(t, func() bool {
				return testutil.HasMetric(t, m, "collector_success", map[string]string{"collector": "database"}) && testutil.MetricValue(t, m, "collector_success", map[string]string{"collector": "database"}) == 1
			}, time.Second, time.Millisecond)
			if accounting {
				require.Eventually(t, func() bool {
					return testutil.HasMetric(t, m, "collector_success", map[string]string{"collector": "kafka"}) && testutil.MetricValue(t, m, "collector_success", map[string]string{"collector": "kafka"}) == 1
				}, time.Second, time.Millisecond)
				require.Equal(t, 0., testutil.MetricValue(t, m, "accounting_lag_records", nil))
			}
		})
	}
}

func TestMonitorTimeoutVersusShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint(shutdown), func(t *testing.T) {
			f := testutil.New(t, "100")
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer ln.Close()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, e := ln.Accept()
				if e == nil {
					accepted <- conn
				}
			}()
			m := metrics.New("test", "commit")
			m.Publish(t.Context(), "kafka", time.Now(), []prometheus.Metric{metrics.Gauge("accounting_lag_records", 7, nil)}, nil)
			last := testutil.MetricValue(t, m, "collector_last_success_timestamp_seconds", map[string]string{"collector": "kafka"})
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				monitorMetricsEvery(ctx, ServeParams{Common: Common{DBPath: f.Path}, EnableAccounting: true}, f.DB, &kafka.Listener{DB: f.DB, Broker: ln.Addr().String(), Topic: "events"}, &chain.Listener{}, m, time.Millisecond)
			}()
			var conn net.Conn
			select {
			case conn = <-accepted:
			case <-time.After(time.Second):
				t.Fatal("probe did not start")
			}
			for range 10 {
				requestCtx, stop := context.WithTimeout(t.Context(), time.Second)
				decision, e := f.DB.Authorize(requestCtx, f.Request)
				stop()
				require.NoError(t, e)
				require.Equal(t, 200, decision.Status)
				r := httptest.NewRecorder()
				m.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
				require.Equal(t, 200, r.Code)
			}
			if shutdown {
				cancel()
			} else {
				<-ctx.Done()
			}
			conn.Close()
			<-done
			labels := map[string]string{"collector": "kafka"}
			require.Equal(t, last, testutil.MetricValue(t, m, "collector_last_success_timestamp_seconds", labels))
			if shutdown {
				require.False(t, testutil.HasMetric(t, m, "collector_errors_total"))
				require.Equal(t, 1., testutil.MetricValue(t, m, "collector_success", labels))
				require.Equal(t, 7., testutil.MetricValue(t, m, "accounting_lag_records", nil))
			} else {
				require.Equal(t, 1., testutil.MetricValue(t, m, "collector_errors_total", labels))
				require.Equal(t, 0., testutil.MetricValue(t, m, "collector_success", labels))
				require.False(t, testutil.HasMetric(t, m, "accounting_lag_records"))
			}
		})
	}
}
