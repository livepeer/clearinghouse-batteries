package app

import (
	"context"
	"database/sql"
	"errors"
	"github.com/livepeer/clearinghouse/internal/chain"
	"github.com/livepeer/clearinghouse/internal/kafka"
	"github.com/livepeer/clearinghouse/internal/metrics"
	"github.com/livepeer/clearinghouse/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func monitorMetrics(ctx context.Context, p ServeParams, db *store.Store, k *kafka.Listener, c *chain.Listener, m *metrics.Metrics) {
	monitorMetricsEvery(ctx, p, db, k, c, m, 15*time.Second)
}

func monitorMetricsEvery(ctx context.Context, p ServeParams, db *store.Store, k *kafka.Listener, c *chain.Listener, m *metrics.Metrics, interval time.Duration) {
	var reader *sql.DB
	if db != nil {
		path, _ := filepath.Abs(p.DBPath)
		u := url.URL{Scheme: "file", Path: path}
		u.RawQuery = "mode=ro&_query_only=1&_txlock=deferred&_busy_timeout=1000"
		reader, _ = sql.Open("sqlite3", u.String())
		if reader != nil {
			reader.SetMaxOpenConns(1)
			defer reader.Close()
		}
	}
	collect := func(name string, fn func(context.Context) ([]prometheus.Metric, error)) bool {
		start := time.Now()
		probe, cancel := context.WithTimeout(ctx, 5*time.Second)
		items, err := fn(probe)
		cancel()
		return m.Publish(ctx, name, start, items, err)
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		dbReady := true
		if db != nil {
			dbReady = collect("database", func(ctx context.Context) ([]prometheus.Metric, error) {
				if reader == nil {
					return nil, errors.New("metrics database unavailable")
				}
				return store.MetricSnapshot(ctx, reader, p.EnableAccounting, p.EnableOnchainListener)
			})
		}
		if ctx.Err() != nil {
			return
		}
		var states []prometheus.Metric
		for _, v := range []struct {
			name           string
			enabled, ready bool
		}{{"webhook", p.EnableAuthWebhook.IsValid(), dbReady}, {"management", p.EnableManagementAPI.IsValid(), dbReady}, {"kafka", p.EnableKafka, true}, {"accounting", p.EnableAccounting, k.Ready.Load()}, {"chain", p.EnableOnchainListener, c.Ready.Load()}} {
			enabled, ready := 0., 0.
			if v.enabled {
				enabled = 1
				if v.ready {
					ready = 1
				}
			}
			states = append(states, metrics.Gauge("component_enabled", enabled, []string{"component"}, v.name), metrics.Gauge("component_ready", ready, []string{"component"}, v.name))
		}
		m.Snapshot("components", states)
		if p.EnableAccounting {
			collect("kafka", func(ctx context.Context) ([]prometheus.Metric, error) {
				if reader == nil {
					return nil, errors.New("metrics database unavailable")
				}
				return k.MetricSnapshot(ctx, &store.Store{DB: reader})
			})
		}
		if ctx.Err() != nil {
			return
		}
		if db != nil {
			s := db.DB.Stats()
			m.Snapshot("pool", []prometheus.Metric{metrics.Gauge("db_pool_connections", float64(s.InUse), []string{"state"}, "in_use"), metrics.Gauge("db_pool_connections", float64(s.Idle), []string{"state"}, "idle"), metrics.Counter("db_pool_waits_total", float64(s.WaitCount)), metrics.Counter("db_pool_wait_duration_seconds_total", s.WaitDuration.Seconds())})
		}
		collect("storage", func(context.Context) ([]prometheus.Metric, error) {
			var out []prometheus.Metric
			paths := map[string]string{}
			if db != nil {
				paths["accounting"] = p.DBPath
			}
			if p.EnableKafka {
				files, e := filepath.Glob(filepath.Join(filepath.Dir(p.DBPath), "minikafka_*.db"))
				if e != nil {
					return nil, e
				}
				for _, f := range files {
					role := strings.TrimSuffix(filepath.Base(f), ".db")
					if role != "minikafka_+meta" {
						part, ok := strings.CutPrefix(role, "minikafka_"+p.KafkaTopic+"_")
						if !ok {
							continue
						}
						if _, e := strconv.ParseUint(part, 10, 31); e != nil {
							continue
						}
					}
					paths[role] = f
				}
			}
			for role, path := range paths {
				for _, suffix := range []string{"", "-wal"} {
					info, e := os.Stat(path + suffix)
					if os.IsNotExist(e) {
						continue
					}
					if e != nil {
						return nil, e
					}
					kind := "database"
					if suffix != "" {
						kind = "wal"
					}
					out = append(out, metrics.Gauge("storage_file_bytes", float64(info.Size()), []string{"role", "kind"}, role, kind))
				}
			}
			return out, nil
		})
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
