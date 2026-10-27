package testutil

import (
	"github.com/livepeer/clearinghouse/internal/metrics"
	"testing"
)

// MetricValue requires a matching sample; unrelated runtime metrics cannot satisfy assertions.
func MetricValue(t testing.TB, m *metrics.Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	value, found := findMetric(t, m, name, labels)
	if !found {
		t.Fatalf("missing metric %s with labels %v", name, labels)
	}
	return value
}
func HasMetric(t testing.TB, m *metrics.Metrics, name string, selectors ...map[string]string) bool {
	t.Helper()
	_, found := findMetric(t, m, name, selectors...)
	return found
}
func findMetric(t testing.TB, m *metrics.Metrics, name string, selectors ...map[string]string) (float64, bool) {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != metrics.Prefix+name {
			continue
		}
	samples:
		for _, sample := range family.Metric {
			for _, selector := range selectors {
				for key, value := range selector {
					found := false
					for _, label := range sample.Label {
						if label.GetName() == key && label.GetValue() == value {
							found = true
							break
						}
					}
					if !found {
						continue samples
					}
				}
			}
			if sample.Counter != nil {
				return sample.Counter.GetValue(), true
			}
			return sample.GetGauge().GetValue(), true
		}
	}
	return 0, false
}
