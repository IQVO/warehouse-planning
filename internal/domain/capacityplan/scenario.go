package capacityplan

import (
	"errors"
	"unicode/utf8"
)

// MaxScenarioLength is the longest scenario label, in characters
// (docs/adr/0024 decision 2; migration 0016 checks the same bound).
const MaxScenarioLength = 64

// ErrInvalidScenario is returned for a scenario label that is empty or longer
// than MaxScenarioLength characters. A plan without a label carries none; a
// label is never blank.
var ErrInvalidScenario = errors.New("capacityplan: scenario must be 1 to 64 characters")

// ValidateScenario reports ErrInvalidScenario unless label is 1 to
// MaxScenarioLength characters (runes, not bytes) long.
func ValidateScenario(label string) error {
	if n := utf8.RuneCountInString(label); n < 1 || n > MaxScenarioLength {
		return ErrInvalidScenario
	}
	return nil
}

// validateOptionalScenario applies ValidateScenario to a label that is set;
// the empty string is "no label".
func validateOptionalScenario(label string) error {
	if label == "" {
		return nil
	}
	return ValidateScenario(label)
}
