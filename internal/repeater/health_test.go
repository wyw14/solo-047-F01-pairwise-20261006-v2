package repeater

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/telemetry"
)

func tempSample(asset string, generation, sequence uint64, value float64) telemetry.Sample {
	return telemetry.Sample{
		ID: "sample", AssetID: asset, Kind: telemetry.KindTemperature, Value: value,
		Sequence: sequence, Generation: generation,
		ObservedAt: time.Date(2026, 10, 5, 2, 0, int(sequence), 0, time.UTC),
	}
}

func newProjectedRepeater(t *testing.T) (*Repeater, *HealthProjector, *incident.RepeaterLifecycle) {
	t.Helper()
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := NewHealthProjector(lifecycle, 85)
	projector.Register(asset)
	return asset, projector, lifecycle
}

func TestHealthProjectorConsumesOnlyConfirmedInOrder(t *testing.T) {
	asset, projector, _ := newProjectedRepeater(t)
	repository := telemetry.NewRepository()
	ingestor := telemetry.NewIngestor(repository, time.Now, projector)
	ctx := context.Background()

	// seq1 正常温度。
	if _, err := ingestor.Apply(ctx, tempSample("R07", 1, 1, 40)); err != nil {
		t.Fatal(err)
	}
	// seq3 超前乱序：健康投影不得消费。
	if r, err := ingestor.Apply(ctx, tempSample("R07", 1, 3, 99)); err != nil {
		t.Fatal(err)
	} else if r.Status != telemetry.StatusOutOfOrder {
		t.Fatalf("seq3 should be out of order, got %s", r.Status)
	}
	if snapshot := asset.Snapshot(); snapshot.LastSequence != 1 || snapshot.Health != HealthNormal {
		t.Fatalf("buffered hot sample must not affect health, got %+v", snapshot)
	}
	// seq2 补齐缺口，连带确认 seq3（99°C），此时才应跳闸且只跳一次。
	if _, err := ingestor.Apply(ctx, tempSample("R07", 1, 2, 50)); err != nil {
		t.Fatal(err)
	}
	snapshot := asset.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.LastSequence != 3 || snapshot.TemperatureC != 99 {
		t.Fatalf("health should trip on confirmed seq3, got %+v", snapshot)
	}
	if active, ok := lifecycleActive(t, projector, "R07"); !ok || active != 1 {
		t.Fatalf("expected one open incident for generation 1")
	}
}

func lifecycleActive(t *testing.T, projector *HealthProjector, id string) (uint64, bool) {
	t.Helper()
	return projector.incidents.Active(id)
}

func TestHealthProjectorDeduplicatesRepeatedSamples(t *testing.T) {
	asset, projector, lifecycle := newProjectedRepeater(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := projector.ApplySample(ctx, tempSample("R07", 1, 1, 90)); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := asset.Snapshot()
	if snapshot.Health != HealthTripped || snapshot.LastSequence != 1 {
		t.Fatalf("repeated sample should update health once, got %+v", snapshot)
	}
	if active, ok := lifecycle.Active("R07"); !ok || active != 1 {
		t.Fatalf("exactly one incident expected after duplicates, active=%d ok=%v", active, ok)
	}
	// 更小或相等序号（含乱序回退）不得改变状态。
	if err := projector.ApplySample(ctx, tempSample("R07", 1, 1, 20)); err != nil {
		t.Fatal(err)
	}
	if asset.Snapshot().Health != HealthTripped {
		t.Fatal("older sequence must not clear a latched trip")
	}
}

func TestHealthProjectorResetsWatermarkOnNewGeneration(t *testing.T) {
	asset, projector, _ := newProjectedRepeater(t)
	ctx := context.Background()

	// 代次 1：seq10 跳闸。
	if err := projector.ApplySample(ctx, tempSample("R07", 1, 10, 90)); err != nil {
		t.Fatal(err)
	}
	// 代次 2：序号从小数重新开始，不应被旧 LastSequence=10 挡住。
	if err := projector.ApplySample(ctx, tempSample("R07", 2, 1, 30)); err != nil {
		t.Fatal(err)
	}
	snapshot := asset.Snapshot()
	if snapshot.LastSequence != 1 || snapshot.TripGeneration != 1 {
		t.Fatalf("new generation must accept restarted sequence, got %+v", snapshot)
	}
	// 代次 2 的低温样本不会解除跳闸（由 Reset 负责），但温度应被更新为非跳闸门限下的读数。
	if snapshot.TemperatureC != 30 {
		t.Fatalf("new generation reading should be recorded, got %+v", snapshot)
	}
	// 旧代次迟到的大序号样本必须被忽略，不能覆盖新代次读数。
	if err := projector.ApplySample(ctx, tempSample("R07", 1, 11, 120)); err != nil {
		t.Fatal(err)
	}
	if asset.Snapshot().TemperatureC != 30 || asset.Snapshot().LastSequence != 1 {
		t.Fatalf("stale old-generation sample must be ignored, got %+v", asset.Snapshot())
	}
}
