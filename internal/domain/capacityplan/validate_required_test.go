package capacityplan

import (
	"errors"
	"testing"
)

// ValidateRequired is the rule CreateCapacityPlan applies before it resolves
// demand or path capacity: any blank identifying field is ErrRequiredField.
func TestValidateRequired(t *testing.T) {
	cases := []struct {
		name                                   string
		warehouse, site, location, processPath string
		want                                   error
	}{
		{"all present", "WH-1", "SIM1", "PLAN-DOC", "pick-only", nil},
		{"blank warehouse", "", "SIM1", "PLAN-DOC", "pick-only", ErrRequiredField},
		{"blank site", "WH-1", "", "PLAN-DOC", "pick-only", ErrRequiredField},
		{"blank location", "WH-1", "SIM1", "", "pick-only", ErrRequiredField},
		{"blank process path", "WH-1", "SIM1", "PLAN-DOC", "", ErrRequiredField},
		{"everything blank", "", "", "", "", ErrRequiredField},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateRequired(tc.warehouse, tc.site, tc.location, tc.processPath); !errors.Is(err, tc.want) {
				t.Fatalf("ValidateRequired = %v, want %v", err, tc.want)
			}
		})
	}
}
