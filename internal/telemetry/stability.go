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

// CurrentSamples 返回当前代次、序号大于 after 的已确认电流样本。
// 已确认保证去重且连续；乱序缓冲样本与旧代次样本都不会进入稳定窗口。
func CurrentSamples(repository *Repository, repeaterID string, after uint64) []Sample {
	return repository.Confirmed(repeaterID, KindCurrent, after)
}
