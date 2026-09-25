package app

import "testing"

func TestDisplayETHRecursively(t *testing.T) {
	input := map[string]any{
		"total_wei": "1000000000000000000",
		"nested": []any{map[string]any{
			"currency":       "usd",
			"balance_units":  "-2750000000000000000",
			"optional_units": nil,
			"balance_wei":    "-1500000000000000000",
			"optional_wei":   nil,
		}},
	}
	converted, err := displayETH(input)
	if err != nil {
		t.Fatal(err)
	}
	result := converted.(map[string]any)
	if result["total_eth"] != "1" {
		t.Fatal(result)
	}
	nested := result["nested"].([]any)[0].(map[string]any)
	if nested["balance_eth"] != "-1.5" || nested["optional_eth"] != nil {
		t.Fatal(nested)
	}
	if nested["balance_usd"] != "-2.75" || nested["optional_usd"] != nil {
		t.Fatal(nested)
	}
}
