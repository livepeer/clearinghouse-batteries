package app

import "testing"

func TestDisplayETHRecursively(t *testing.T) {
	input := map[string]any{
		"total_wei": "1000000000000000000",
		"nested": []any{map[string]any{
			"balance_wei":  "-1500000000000000000",
			"optional_wei": nil,
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
}
