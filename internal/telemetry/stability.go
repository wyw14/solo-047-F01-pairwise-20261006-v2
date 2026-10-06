package telemetry

import (
	"errors"
	"math"
	"time"
)

type StabilityWindow struct {
	MinimumSamples int
	MaximumSpread  float64
	MinimumPeriod  time.Duration
}

type StabilityResult struct {
	Stable  bool          `json:"stable"`
	Spread  float64       `json:"spread"`
	Period  time.Duration `json:"period"`
	Samples int           `json:"samples"`
	LastSeq uint64        `json:"last_sequence"`
}

func (w StabilityWindow) Evaluate(samples []Sample) (StabilityResult, error) {
	if w.MinimumSamples < 2 || w.MaximumSpread < 0 || w.MinimumPeriod < 0 {
		return StabilityResult{}, errors.New("invalid stability window")
	}
	if len(samples) == 0 {
		return StabilityResult{}, nil
	}
	minimum, maximum := samples[0].Value, samples[0].Value
	first, last := samples[0].ObservedAt, samples[0].ObservedAt
	var lastSequence uint64
	for _, sample := range samples {
		minimum = math.Min(minimum, sample.Value)
		maximum = math.Max(maximum, sample.Value)
		if sample.ObservedAt.Before(first) {
			first = sample.ObservedAt
		}
		if sample.ObservedAt.After(last) {
			last = sample.ObservedAt
		}
		if sample.Sequence > lastSequence {
			lastSequence = sample.Sequence
		}
	}
	result := StabilityResult{
		Spread: maximum - minimum, Period: last.Sub(first), Samples: len(samples), LastSeq: lastSequence,
	}
	result.Stable = result.Samples >= w.MinimumSamples && result.Spread <= w.MaximumSpread && result.Period >= w.MinimumPeriod
	return result, nil
}

// CurrentSamples returns only confirmed current samples strictly after the
// requested sequence. Samples still buffered behind a sequence gap (or from a
// superseded generation) are excluded: the stability window must consume only
// acknowledged, gap-free data.
func CurrentSamples(repository *Repository, repeaterID string, after uint64) []Sample {
	generation, ok := repository.CurrentGeneration(repeaterID)
	if !ok {
		return nil
	}
	var result []Sample
	for _, sample := range repository.ConfirmedSamples(repeaterID, generation) {
		if sample.Kind == KindCurrent && sample.Sequence > after {
			result = append(result, sample)
		}
	}
	return result
}
