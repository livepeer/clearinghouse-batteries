package app

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDisplayETHRecursively(t *testing.T) {
	input := map[string]any{
		"total_wei": "1000000000000000000",
		"empty_wei": "",
		"zero_wei":  "0",
		"nested": []any{map[string]any{
			"currency":       "usd",
			"balance_units":  "-2750000000000000000",
			"optional_units": nil,
			"invalid_units":  "invalid",
			"balance_wei":    "-1500000000000000000",
			"optional_wei":   nil,
		}, map[string]string{"currency": "usd", "balance_units": "", "balance_wei": "invalid"}},
	}
	converted, err := displayETH(input)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"total_eth": "1",
		"empty_eth": "",
		"zero_eth":  "0",
		"nested": []any{map[string]any{
			"currency":     "usd",
			"balance_usd":  "-2.75",
			"optional_usd": nil,
			"invalid_usd":  "[error]",
			"balance_eth":  "-1.5",
			"optional_eth": nil,
		}, map[string]string{"currency": "usd", "balance_usd": "", "balance_eth": "[error]"}},
	}, converted)
}
