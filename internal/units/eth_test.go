package units

import (
	"strings"
	"testing"
)

func TestETHToWei(t *testing.T) {
	tests := map[string]string{
		"0":                    "0",
		"0.0":                  "0",
		"0.000000000000000001": "1",
		"0.1":                  "100000000000000000",
		"0001.2300":            "1230000000000000000",
		"1":                    "1000000000000000000",
		"1.2300":               "1230000000000000000",
		"123456789012345678901234567890.123456789012345678": "123456789012345678901234567890123456789012345678",
	}
	for input, want := range tests {
		got, err := ETHToWei(input)
		if err != nil || got != want {
			t.Fatalf("ETHToWei(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	for _, input := range []string{"", "all", "-1", "+1", ".1", "1.", "1e2", "1.0000000000000000001", "1 2", "0x1", "NaN"} {
		if _, err := ETHToWei(input); err == nil {
			t.Fatalf("ETHToWei(%q) succeeded", input)
		}
	}
	large := strings.Repeat("9", 238)
	if got, err := ETHToWei(large); err != nil || got != large+strings.Repeat("0", 18) {
		t.Fatalf("large ETH conversion = %q, %v", got, err)
	}
	if _, err := ETHToWei(strings.Repeat("9", 239)); err == nil {
		t.Fatal("accepted more than 256 wei digits")
	}
}

func TestWeiToETH(t *testing.T) {
	tests := map[string]string{
		"0":                    "0",
		"1":                    "0.000000000000000001",
		"100":                  "0.0000000000000001",
		"100000000000000000":   "0.1",
		"1000000000000000000":  "1",
		"1230000000000000000":  "1.23",
		"-1500000000000000000": "-1.5",
	}
	for input, want := range tests {
		got, err := WeiToETH(input)
		if err != nil || got != want {
			t.Fatalf("WeiToETH(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	large := strings.Repeat("9", 238)
	if got, err := WeiToETH(large + strings.Repeat("0", 18)); err != nil || got != large {
		t.Fatalf("large wei conversion = %q, %v", got, err)
	}
	for _, input := range []string{"", "01", "+1", "1.0", "--1", "wei"} {
		if _, err := WeiToETH(input); err == nil {
			t.Fatalf("WeiToETH(%q) succeeded", input)
		}
	}
}
