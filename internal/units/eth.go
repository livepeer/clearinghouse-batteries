// Package units converts between user-facing ETH or USD decimals and exact 10^18-scale integers.
package units

import (
	"errors"
	"math/big"
	"strings"
)

const decimals = 18

var unitsPerCurrency = new(big.Int).Exp(big.NewInt(10), big.NewInt(decimals), nil)

// ETHToWei converts an unsigned, non-exponential ETH decimal to canonical wei.
func ETHToWei(value string) (string, error) {
	return DecimalToUnits(value, "ETH")
}

func DecimalToUnits(value, currency string) (string, error) {
	if value == "" {
		return "", errors.New("amount in " + currency + " is required")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" || (len(parts) == 2 && parts[1] == "") {
		return "", errors.New("amount must be an unsigned decimal " + currency + " value")
	}
	if !digits(parts[0]) || (len(parts) == 2 && !digits(parts[1])) {
		return "", errors.New("amount must be an unsigned decimal " + currency + " value")
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > decimals {
		return "", errors.New("amount has more than 18 decimal places")
	}
	whole := strings.TrimLeft(parts[0], "0")
	if whole == "" {
		whole = "0"
	}
	digitsValue := whole + fraction + strings.Repeat("0", decimals-len(fraction))
	digitsValue = strings.TrimLeft(digitsValue, "0")
	if digitsValue == "" {
		digitsValue = "0"
	}
	if len(digitsValue) > 256 {
		return "", errors.New("amount exceeds the 256-digit unit limit")
	}
	return digitsValue, nil
}

// WeiToETH converts a canonical signed wei integer to a canonical ETH decimal.
// Signed values are supported because credit-minus-debit reports can be negative.
func WeiToETH(value string) (string, error) {
	return UnitsToDecimal(value)
}

func UnitsToDecimal(value string) (string, error) {
	negative := strings.HasPrefix(value, "-")
	if negative {
		value = strings.TrimPrefix(value, "-")
	}
	if value == "" || !digits(value) || (len(value) > 1 && value[0] == '0') {
		return "", errors.New("invalid amount units")
	}
	n, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return "", errors.New("invalid amount units")
	}
	whole, remainder := new(big.Int), new(big.Int)
	whole.QuoRem(n, unitsPerCurrency, remainder)
	out := whole.String()
	if remainder.Sign() != 0 {
		fraction := remainder.String()
		fraction = strings.Repeat("0", decimals-len(fraction)) + fraction
		out += "." + strings.TrimRight(fraction, "0")
	}
	if negative && n.Sign() != 0 {
		out = "-" + out
	}
	return out, nil
}

func digits(value string) bool {
	for _, c := range value {
		if c < '0' || c > '9' {
			return false
		}
	}
	return value != ""
}
