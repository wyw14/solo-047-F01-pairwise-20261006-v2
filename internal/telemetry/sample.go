package telemetry

import (
	"errors"
	"sort"
	"sync"
	"time"
)

type Kind string

const (
	KindOpticalPower Kind = "optical_power"
	KindErrorRate    Kind = "error_rate"
	KindCurrent      Kind = "current"
	KindTemperature  Kind = "temperature"
	KindHeartbeat    Kind = "heartbeat"
)

type Sample struct {
	ID         string    `json:"id"`
	AssetID    string    `json:"asset_id"`
	Kind       Kind      `json:"kind"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	Sequence   uint64    `json:"sequence"`
	Generation uint64    `json:"generation"`
	ObservedAt time.Time `json:"observed_at"`
	ReceivedAt time.Time `json:"received_at"`
}

func (s Sample) Validate() error {
	if s.ID == "" || s.AssetID == "" || s.Kind == "" {
		return errors.New("sample identity, asset and kind are required")
	}
	if s.Sequence == 0 || s.ObservedAt.IsZero() {
		return errors.New("sample sequence and observed time are required")
	}
	return nil
}

// Outcome is the explicit result of admitting a single inbound sample.
type Outcome string

const (
	// OutcomeAccepted is a novel, in-order sample that is immediately confirmed.
	OutcomeAccepted Outcome = "accepted"
	// OutcomeOutOfOrder is a novel sample stored ahead of a sequence gap. It is
	// retained (so replay never loses it) but only becomes confirmed once the gap
	// closes; until then consumers must not use it.
	OutcomeOutOfOrder Outcome = "out_of_order"
	// OutcomeDuplicate is a sample whose (asset, generation, sequence) key was
	// already admitted. It is neither stored again nor projected.
	OutcomeDuplicate Outcome = "duplicate"
)

// Admission reports what Repository.Admit did with a sample.
type Admission struct {
	Outcome  Outcome
	Sample   Sample
	Existing Sample
	// NewlyConfirmed are the samples that transitioned to confirmed because of
	// this admission, in ascending sequence order. An in-order admission yields
	// the sample itself; an admission that closes one or more gaps yields the
	// whole contiguous run, including previously buffered out-of-order samples.
	NewlyConfirmed []Sample
}

// stream holds the per-asset, per-generation ordered sample log.
type stream struct {
	// samples is unique by sequence and kept sorted ascending.
	samples []Sample
	// confirmed is the number of leading samples forming an unbroken sequence
	// run starting at sequence 1. Those are the only samples consumers may see.
	confirmed int
}

type Repository struct {
	mu      sync.RWMutex
	streams map[string]map[uint64]*stream
}

func NewRepository() *Repository {
	return &Repository{streams: make(map[string]map[uint64]*stream)}
}

// Admit validates and deduplicates the sample by (asset, generation, sequence)
// and reports whether it was accepted, stored out of order, or duplicated.
func (r *Repository) Admit(sample Sample) (Admission, error) {
	if err := sample.Validate(); err != nil {
		return Admission{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	generations := r.streams[sample.AssetID]
	if generations == nil {
		generations = make(map[uint64]*stream)
		r.streams[sample.AssetID] = generations
	}
	target := generations[sample.Generation]
	if target == nil {
		target = &stream{}
		generations[sample.Generation] = target
	}

	if index, found := target.indexOf(sample.Sequence); found {
		return Admission{Outcome: OutcomeDuplicate, Sample: sample, Existing: target.samples[index]}, nil
	} else {
		target.samples = insertAt(target.samples, sample, index)
	}

	outcome := OutcomeOutOfOrder
	// An in-order sample extends the confirmed frontier directly: its sequence
	// must be exactly one past the currently contiguous prefix.
	if sample.Sequence == uint64(target.confirmed)+1 {
		outcome = OutcomeAccepted
	}

	previouslyConfirmed := target.confirmed
	target.advanceConfirmed()
	var newly []Sample
	if target.confirmed > previouslyConfirmed {
		newly = append(newly, target.samples[previouslyConfirmed:target.confirmed]...)
	}
	return Admission{Outcome: outcome, Sample: sample, NewlyConfirmed: newly}, nil
}

// Add admits a sample and discards the admission details. It fails only on
// validation errors; duplicates and out-of-order arrivals are accepted
// idempotently.
func (r *Repository) Add(sample Sample) error {
	_, err := r.Admit(sample)
	return err
}

func (s *stream) indexOf(sequence uint64) (int, bool) {
	index := sort.Search(len(s.samples), func(i int) bool { return s.samples[i].Sequence >= sequence })
	if index < len(s.samples) && s.samples[index].Sequence == sequence {
		return index, true
	}
	return index, false
}

func insertAt(items []Sample, sample Sample, index int) []Sample {
	items = append(items, Sample{})
	copy(items[index+1:], items[index:])
	items[index] = sample
	return items
}

// advanceConfirmed grows the confirmed prefix while sequences are contiguous
// from 1, so a late gap-filler confirms every buffered sample behind it.
func (s *stream) advanceConfirmed() {
	for s.confirmed < len(s.samples) && s.samples[s.confirmed].Sequence == uint64(s.confirmed)+1 {
		s.confirmed++
	}
}

// History returns every admitted sample for the asset — duplicates excluded —
// ordered by generation then sequence. Out-of-order samples are included
// because they were genuinely admitted; consumers that need confirmed data use
// ConfirmedSamples.
func (r *Repository) History(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []Sample
	for _, target := range r.streams[assetID] {
		result = append(result, target.samples...)
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Generation != result[j].Generation {
			return result[i].Generation < result[j].Generation
		}
		return result[i].Sequence < result[j].Sequence
	})
	return result
}

// CurrentGeneration is the highest generation ever seen for the asset.
func (r *Repository) CurrentGeneration(assetID string) (uint64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var current uint64
	var found bool
	for generation := range r.streams[assetID] {
		if !found || generation > current {
			current, found = generation, true
		}
	}
	return current, found
}

// ConfirmedSamples returns the contiguous, gap-free confirmed prefix for one
// asset and generation. These are the only samples health and stability
// consumers are allowed to read.
func (r *Repository) ConfirmedSamples(assetID string, generation uint64) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	target := r.streams[assetID][generation]
	if target == nil || target.confirmed == 0 {
		return nil
	}
	result := make([]Sample, target.confirmed)
	copy(result, target.samples[:target.confirmed])
	return result
}

// Latest returns the most recent confirmed sample of a kind for the asset's
// current generation.
func (r *Repository) Latest(assetID string, kind Kind) (Sample, bool) {
	generation, ok := r.CurrentGeneration(assetID)
	if !ok {
		return Sample{}, false
	}
	items := r.ConfirmedSamples(assetID, generation)
	for index := len(items) - 1; index >= 0; index-- {
		if items[index].Kind == kind {
			return items[index], true
		}
	}
	return Sample{}, false
}
