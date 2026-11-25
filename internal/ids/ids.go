// Package ids parses FlowAI identifiers: positive 64-bit integers (rule 43).
package ids

import (
	"encoding/json"
	"strconv"
)

// Parse accepts a decimal token without sign or leading zeros, 1..2^63-1.
func Parse(s string) (int64, bool) {
	if s == "" || len(s) > 19 || s[0] == '0' {
		return 0, false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil && v > 0
}

// FromJSON accepts a JSON integer number 1..2^63-1 (no fraction, exponent or string).
func FromJSON(raw json.RawMessage) (int64, bool) {
	return Parse(string(raw))
}
