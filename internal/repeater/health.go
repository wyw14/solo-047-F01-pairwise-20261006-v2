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
	mu         sync.RWMutex
	repeaters  map[string]*Repeater
	incidents  IncidentSink
	tripAtC    float64
	watermarks map[string]sampleWatermark
}

// sampleWatermark 记录已投影到某个中继的最新（代次、序号），用于回放幂等去重。
type sampleWatermark struct {
	generation uint64
	sequence   uint64
}

func NewHealthProjector(incidents IncidentSink, tripAtC float64) *HealthProjector {
	return &HealthProjector{
		repeaters:  make(map[string]*Repeater),
		incidents:  incidents,
		tripAtC:    tripAtC,
		watermarks: make(map[string]sampleWatermark),
	}
}

func (p *HealthProjector) Register(repeater *Repeater) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repeaters[repeater.Snapshot().ID] = repeater
}

// ApplySample 只消费入站流程已确认的样本，并按（代次、序号）幂等：
// 旧代次、或同代次内序号不大于已投影水位线的样本（含游标回放的重复投递）一律跳过，
// 健康状态不会因重复或乱序样本被二次更新或误报。
func (p *HealthProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if sample.Kind != telemetry.KindTemperature {
		return nil
	}
	p.mu.RLock()
	repeater := p.repeaters[sample.AssetID]
	watermark := p.watermarks[sample.AssetID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	before := repeater.Snapshot()
	if sample.Generation < before.TripGeneration {
		return nil
	}
	// 代次内序号必须严格增长；新代次重置序号水位线，允许从小序号重新开始。
	if sample.Generation < watermark.generation ||
		(sample.Generation == watermark.generation && sample.Sequence <= watermark.sequence) {
		return nil
	}
	after := repeater.update(func(current *Snapshot) {
		current.LastSequence = sample.Sequence
		// 跳闸只锁存健康状态，最新温度读数仍持续更新（由 Reset 解除锁存）。
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
	p.watermarks[sample.AssetID] = sampleWatermark{generation: sample.Generation, sequence: sample.Sequence}
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
