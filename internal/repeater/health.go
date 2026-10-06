package repeater

import (
	"context"
	"sync"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type IncidentSink interface {
	OpenRepeaterIncident(repeaterID string, generation uint64) error
	ResolveRepeaterIncident(repeaterID string, generation uint64) error
	Active(repeaterID string) (uint64, bool)
	LastResolution(repeaterID string) uint64
}

type HealthProjector struct {
	mu        sync.RWMutex
	repeaters map[string]*Repeater
	// applied is the highest (generation, sequence) already projected per
	// repeater. Sequences reset each generation, so the watermark is keyed by
	// generation rather than a single counter.
	applied   map[string]appliedMarker
	incidents IncidentSink
	tripAtC   float64
}

type appliedMarker struct {
	generation uint64
	sequence   uint64
}

func NewHealthProjector(incidents IncidentSink, tripAtC float64) *HealthProjector {
	return &HealthProjector{
		repeaters: make(map[string]*Repeater), applied: make(map[string]appliedMarker),
		incidents: incidents, tripAtC: tripAtC,
	}
}

func (p *HealthProjector) Register(repeater *Repeater) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repeaters[repeater.Snapshot().ID] = repeater
}

func (p *HealthProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if sample.Kind != telemetry.KindTemperature {
		return nil
	}
	p.mu.RLock()
	repeater := p.repeaters[sample.AssetID]
	marker := p.applied[sample.AssetID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	// The ingestor already delivers only confirmed, deduplicated samples in
	// order; this guard makes replay idempotent and tolerates generation
	// rollover where sequences restart at 1.
	if sample.Generation < marker.generation ||
		(sample.Generation == marker.generation && sample.Sequence <= marker.sequence) {
		return nil
	}
	before := repeater.Snapshot()
	if sample.Generation < before.TripGeneration {
		return nil
	}
	after := repeater.update(func(current *Snapshot) {
		current.LastSequence = sample.Sequence
		current.TemperatureC = sample.Value
		current.UpdatedAt = sample.ObservedAt.UTC()
		if sample.Value >= p.tripAtC {
			current.Health = HealthTripped
			current.TripGeneration = sample.Generation
		} else if current.Health != HealthTripped {
			current.Health = HealthNormal
		}
	})
	p.mu.Lock()
	p.applied[sample.AssetID] = appliedMarker{generation: sample.Generation, sequence: sample.Sequence}
	p.mu.Unlock()
	if before.Health != HealthTripped && after.Health == HealthTripped && p.incidents != nil {
		return p.incidents.OpenRepeaterIncident(after.ID, after.TripGeneration)
	}
	return nil
}

func (p *HealthProjector) Reset(repeaterID string, generation uint64) error {
	p.mu.RLock()
	repeater := p.repeaters[repeaterID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	if active, ok := p.incidents.Active(repeaterID); ok && generation < active {
		return nil
	}
	repeater.update(func(current *Snapshot) {
		if generation >= current.TripGeneration {
			current.Health = HealthNormal
			current.TripGeneration = generation
		}
	})
	if p.incidents != nil {
		if err := p.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return err
		}
		_ = p.incidents.LastResolution(repeaterID)
	}
	return nil
}
