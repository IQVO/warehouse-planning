package report

import (
	"math"
	"sort"
)

// BottleneckFrequency is a BottleneckCount plus Share: that count as a
// fraction of ALL published plans at the same site in the range.
type BottleneckFrequency struct {
	BottleneckCount
	Share float64 `json:"share"`
}

// BottleneckFrequencies derives each row's Share (plans / the site's total
// plans) from raw counts. Order is preserved; a nil input yields an empty,
// non-nil slice.
func BottleneckFrequencies(counts []BottleneckCount) []BottleneckFrequency {
	totals := make(map[Site]int, len(counts))
	for _, c := range counts {
		totals[c.Site] += c.Plans
	}
	out := make([]BottleneckFrequency, 0, len(counts))
	for _, c := range counts {
		out = append(out, BottleneckFrequency{BottleneckCount: c, Share: Rate(c.Plans, totals[c.Site])})
	}
	return out
}

// ShortageTrendDay is a ShortageDay plus ShortageRate: the fraction of that
// site-day's published plans that had a shortage.
type ShortageTrendDay struct {
	ShortageDay
	ShortageRate float64 `json:"shortage_rate"`
}

// ShortageTrend derives each day's ShortageRate. Order is preserved; a nil
// input yields an empty, non-nil slice.
func ShortageTrend(days []ShortageDay) []ShortageTrendDay {
	out := make([]ShortageTrendDay, 0, len(days))
	for _, d := range days {
		out = append(out, ShortageTrendDay{ShortageDay: d, ShortageRate: Rate(d.PlansWithShortage, d.PlansPublished)})
	}
	return out
}

// Rate is num/den, or 0 when den is not positive (no plans, no rate).
func Rate(num, den int) float64 {
	if den <= 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// Percentile is the continuous percentile (linear interpolation between
// closest ranks, p in [0,1]) of values, the same definition as Postgres'
// percentile_cont. values need not be sorted; it is not modified. An empty
// input yields 0.
func Percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := p * float64(len(sorted)-1)
	lo := math.Floor(rank)
	hi := math.Ceil(rank)
	frac := rank - lo
	return sorted[int(lo)] + (sorted[int(hi)]-sorted[int(lo)])*frac
}
