package app

import (
	"fmt"
	"strings"

	"github.com/livepeer/clearinghouse/internal/units"
)

// displayETH rewrites store-native amounts as ETH or USD decimals for CLI/API JSON.
func displayETH(value any) (any, error) {
	switch value := value.(type) {
	case []map[string]any:
		out := make([]map[string]any, len(value))
		for i, row := range value {
			converted, err := displayAnyMap(row)
			if err != nil {
				return nil, err
			}
			out[i] = converted
		}
		return out, nil
	case []map[string]string:
		out := make([]map[string]string, len(value))
		for i, row := range value {
			converted, err := displayStringMap(row)
			if err != nil {
				return nil, err
			}
			out[i] = converted
		}
		return out, nil
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			converted, err := displayETH(item)
			if err != nil {
				return nil, err
			}
			out[i] = converted
		}
		return out, nil
	case map[string]any:
		return displayAnyMap(value)
	case map[string]string:
		return displayStringMap(value)
	default:
		return value, nil
	}
}

func displayAnyMap(row map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(row))
	for key, value := range row {
		suffix, currency := "_wei", "eth"
		if strings.HasSuffix(key, "_units") {
			suffix = "_units"
			currency, _ = row["currency"].(string)
			if currency != "usd" && currency != "eth" {
				return nil, fmt.Errorf("%s has no valid currency", key)
			}
		}
		if !strings.HasSuffix(key, suffix) {
			converted, err := displayETH(value)
			if err != nil {
				return nil, err
			}
			out[key] = converted
			continue
		}
		key = strings.TrimSuffix(key, suffix) + "_" + currency
		if value == nil {
			out[key] = nil
			continue
		}
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("%s is not a string amount", key)
		}
		converted, err := units.UnitsToDecimal(s)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		out[key] = converted
	}
	return out, nil
}

func displayStringMap(row map[string]string) (map[string]string, error) {
	out := make(map[string]string, len(row))
	for key, value := range row {
		suffix, currency := "_wei", "eth"
		if strings.HasSuffix(key, "_units") {
			suffix, currency = "_units", row["currency"]
			if currency != "usd" && currency != "eth" {
				return nil, fmt.Errorf("%s has no valid currency", key)
			}
		}
		if !strings.HasSuffix(key, suffix) {
			out[key] = value
			continue
		}
		key = strings.TrimSuffix(key, suffix) + "_" + currency
		converted, err := units.UnitsToDecimal(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		out[key] = converted
	}
	return out, nil
}
