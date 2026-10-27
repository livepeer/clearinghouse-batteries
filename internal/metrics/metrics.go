// Package metrics owns process-local instrumentation. No collector performs I/O during a scrape.
package metrics

import (
	"context"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"math/big"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const Prefix = "livepeer_clearinghouse_"

type Metrics struct {
	Registry    *prometheus.Registry
	mu          sync.Mutex
	counters    map[string]*prometheus.CounterVec
	histograms  map[string]*prometheus.HistogramVec
	snapshots   map[string][]prometheus.Metric
	amounts     map[string]*big.Int
	lastSuccess map[string]float64
}

func New(version, commit string) *Metrics {
	m := &Metrics{lastSuccess: map[string]float64{}, amounts: map[string]*big.Int{}, Registry: prometheus.NewRegistry(), counters: map[string]*prometheus.CounterVec{}, histograms: map[string]*prometheus.HistogramVec{}, snapshots: map[string][]prometheus.Metric{}}
	m.Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), m)
	m.Snapshot("build", []prometheus.Metric{Gauge("build_info", 1, []string{"version", "commit"}, version, commit)})
	return m
}
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {} // Dynamic snapshots are an unchecked collector.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, items := range m.snapshots {
		for _, v := range items {
			ch <- v
		}
	}
}
func Gauge(name string, value float64, labels []string, values ...string) prometheus.Metric {
	return prometheus.MustNewConstMetric(prometheus.NewDesc(Prefix+name, name, labels, nil), prometheus.GaugeValue, value, values...)
}
func Counter(name string, value float64) prometheus.Metric {
	return prometheus.MustNewConstMetric(prometheus.NewDesc(Prefix+name, name, nil, nil), prometheus.CounterValue, value)
}
func (m *Metrics) Snapshot(name string, items []prometheus.Metric) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshots[name] = items
}

// AddAmount accumulates exact atomic units; only the exposed aggregate is rounded.
func (m *Metrics) AddAmount(name string, units *big.Int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.amounts[name] == nil {
		m.amounts[name] = new(big.Int)
	}
	m.amounts[name].Add(m.amounts[name], units)
	value, _ := new(big.Rat).SetFrac(m.amounts[name], new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)).Float64()
	m.snapshots[name] = []prometheus.Metric{Counter(name, value)}
}

// Publish atomically replaces collector data and health, retaining the last success on failure.
func (m *Metrics) Publish(ctx context.Context, name string, start time.Time, items []prometheus.Metric, err error) bool {
	if m == nil {
		return err == nil
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return false
	}
	ok := 1.
	if err != nil {
		ok = 0
		items = nil
		m.Add("collector_errors_total", 1, []string{"collector"}, name)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		m.lastSuccess[name] = float64(time.Now().UnixMilli()) / 1000
	}
	items = append(items, Gauge("collector_success", ok, []string{"collector"}, name), Gauge("collector_duration_seconds", time.Since(start).Seconds(), []string{"collector"}, name))
	if last := m.lastSuccess[name]; last > 0 {
		items = append(items, Gauge("collector_last_success_timestamp_seconds", last, []string{"collector"}, name))
		if name == "chain" {
			items = append(items, Gauge("chain_last_successful_poll_timestamp_seconds", last, nil))
		}
	}
	m.snapshots["collector_"+name] = items
	return err == nil
}
func (m *Metrics) Add(name string, n float64, labels []string, values ...string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	v := m.counters[name]
	if v == nil {
		v = prometheus.NewCounterVec(prometheus.CounterOpts{Name: Prefix + name, Help: name}, labels)
		m.Registry.MustRegister(v)
		m.counters[name] = v
	}
	m.mu.Unlock()
	v.WithLabelValues(values...).Add(n)
}
func (m *Metrics) Observe(name string, n float64, labels []string, values ...string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	v := m.histograms[name]
	if v == nil {
		v = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: Prefix + name, Help: name, Buckets: []float64{.001, .005, .01, .05, .1, .5, 1, 5, 15, 30, 60, 120, 300}}, labels)
		m.Registry.MustRegister(v)
		m.histograms[name] = v
	}
	m.mu.Unlock()
	v.WithLabelValues(values...).Observe(n)
}
func (m *Metrics) Error(component, operation, reason string) {
	m.Add("service_errors_total", 1, []string{"component", "operation", "reason"}, component, operation, reason)
}
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

type response struct {
	http.ResponseWriter
	code int
}

func (w *response) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
		w.ResponseWriter.WriteHeader(code)
	}
}
func (w *response) Write(b []byte) (int, error) {
	if w.code == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (m *Metrics) HTTP(listener string, next http.Handler) http.Handler {
	if m == nil {
		return next
	}
	active := prometheus.NewGauge(prometheus.GaugeOpts{Name: Prefix + "http_requests_in_flight", Help: "HTTP requests currently being served", ConstLabels: prometheus.Labels{"listener": listener}})
	m.Registry.MustRegister(active)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		active.Inc()
		defer active.Dec()
		rw := &response{ResponseWriter: w}
		defer func() {
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			method := r.Method
			switch method {
			case "GET", "HEAD", "POST", "PATCH", "DELETE", "PUT", "OPTIONS":
			default:
				method = "other"
			}
			code := rw.code
			if code == 0 {
				code = 200
			}
			m.Add("http_requests_total", 1, []string{"listener", "route", "method", "code"}, listener, route, method, strconv.Itoa(code))
			m.Observe("http_request_duration_seconds", time.Since(start).Seconds(), []string{"listener", "route", "method"}, listener, route, method)
		}()
		next.ServeHTTP(rw, r)
	})
}
