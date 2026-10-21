package inbound

import (
	"errors"
	"math"
	"time"
)

var (
	// ErrStandardRequiredField is returned when a labour standard is declared
	// with a blank location or process path id.
	ErrStandardRequiredField = errors.New("inbound: labour standard location and process path id are required")

	// ErrNonPositiveAppointmentMinutes is returned when minutes per
	// appointment is zero, negative or NaN: a standard that costs nothing
	// would silently turn every appointment into zero demand.
	ErrNonPositiveAppointmentMinutes = errors.New("inbound: minutes per appointment must be positive")

	// ErrNegativeASNMinutes is returned when minutes per ASN is negative or
	// NaN. Zero is valid: the optional term is simply not used.
	ErrNegativeASNMinutes = errors.New("inbound: minutes per ASN must not be negative")

	// ErrNonFiniteMinutes is returned for an infinite minutes figure.
	ErrNonFiniteMinutes = errors.New("inbound: labour minutes must be finite")
)

// LabourStandard is the operator-declared labour a dock appointment costs at
// a site for a process path: a fixed MinutesPerAppointment plus an optional
// MinutesPerASN for every ASN the appointment covers (docs/adr/0017
// decision 2). It is a planning parameter owned by this bounded context --
// no upstream publishes it -- keyed (location, process path id), and it is
// the ONLY source of inbound labour minutes: nothing is ever defaulted.
// Immutable once constructed.
//
// The booked event carries no units, pallets or lines, so there is no
// per-unit term; the ASN count is the only per-appointment quantity it has.
type LabourStandard struct {
	location              string
	processPathID         string
	minutesPerAppointment float64
	minutesPerASN         float64
}

// NewLabourStandard validates its arguments and builds a LabourStandard.
func NewLabourStandard(location, processPathID string, minutesPerAppointment, minutesPerASN float64) (LabourStandard, error) {
	if location == "" || processPathID == "" {
		return LabourStandard{}, ErrStandardRequiredField
	}
	if math.IsInf(minutesPerAppointment, 0) || math.IsInf(minutesPerASN, 0) {
		return LabourStandard{}, ErrNonFiniteMinutes
	}
	if !(minutesPerAppointment > 0) {
		return LabourStandard{}, ErrNonPositiveAppointmentMinutes
	}
	if !(minutesPerASN >= 0) {
		return LabourStandard{}, ErrNegativeASNMinutes
	}
	return LabourStandard{
		location: location, processPathID: processPathID,
		minutesPerAppointment: minutesPerAppointment, minutesPerASN: minutesPerASN,
	}, nil
}

// Location returns the site the standard applies to.
func (s LabourStandard) Location() string { return s.location }

// ProcessPathID returns the process path the standard applies to.
func (s LabourStandard) ProcessPathID() string { return s.processPathID }

// MinutesPerAppointment returns the fixed labour minutes of one appointment.
func (s LabourStandard) MinutesPerAppointment() float64 { return s.minutesPerAppointment }

// MinutesPerASN returns the labour minutes added per covered ASN.
func (s LabourStandard) MinutesPerASN() float64 { return s.minutesPerASN }

// LabourMinutes returns the labour the standard attributes to d:
// minutesPerAppointment per appointment plus minutesPerASN per covered ASN.
// It equals the sum of each appointment's own labour, by linearity, so the
// repository only has to count appointments and ASNs.
func (s LabourStandard) LabourMinutes(d Demand) float64 {
	return s.minutesPerAppointment*float64(d.Appointments) + s.minutesPerASN*float64(d.ASNs)
}

// AverageHeads converts labour minutes into the average number of people
// the work needs over a window of the given length: minutes / window minutes.
// A non-positive window yields 0.
func AverageHeads(labourMinutes float64, window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return labourMinutes / window.Minutes()
}
