package telemetry

import (
	"context"
	"testing"
	"time"
)

func newTestSample(asset string, generation, sequence uint64, kind Kind) Sample {
	base := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	return Sample{
		ID: "sample", AssetID: asset, Kind: kind, Value: float64(sequence), Unit: "A",
		Sequence: sequence, Generation: generation,
		ObservedAt: base.Add(time.Duration(sequence) * time.Second),
	}
}

// recordSink captures every confirmed sample delivered to it.
type recordSink struct {
	samples []Sample
}

func (s *recordSink) ApplySample(_ context.Context, sample Sample) error {
	s.samples = append(s.samples, sample)
	return nil
}

func TestRepositoryOrdersSamplesBySequence(t *testing.T) {
	repository := NewRepository()
	now := time.Now().UTC()
	for _, sequence := range []uint64{3, 1, 2} {
		if err := repository.Add(Sample{
			ID: "sample", AssetID: "R07", Kind: KindCurrent, Value: float64(sequence),
			Sequence: sequence, ObservedAt: now.Add(time.Duration(sequence) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
	history := repository.History("R07")
	if len(history) != 3 || history[0].Sequence != 1 || history[2].Sequence != 3 {
		t.Fatalf("samples not ordered by sequence: %+v", history)
	}
}

func TestAdmitDeduplicatesByAssetGenerationAndSequence(t *testing.T) {
	repository := NewRepository()
	first := newTestSample("R07", 1, 1, KindCurrent)
	second := newTestSample("R07", 1, 1, KindCurrent)
	second.Value = 999

	admission, err := repository.Admit(first)
	if err != nil {
		t.Fatal(err)
	}
	if admission.Outcome != OutcomeAccepted {
		t.Fatalf("first admission outcome = %q, want accepted", admission.Outcome)
	}

	again, err := repository.Admit(second)
	if err != nil {
		t.Fatal(err)
	}
	if again.Outcome != OutcomeDuplicate {
		t.Fatalf("repeat admission outcome = %q, want duplicate", again.Outcome)
	}
	if again.Existing.Sequence != 1 || again.Existing.Value != 1 {
		t.Fatalf("duplicate should expose the original sample, got %+v", again.Existing)
	}
	if history := repository.History("R07"); len(history) != 1 || history[0].Value != 1 {
		t.Fatalf("duplicate rewrote history: %+v", history)
	}

	// Same sequence in a new generation is a distinct stream, not a duplicate.
	rolled, err := repository.Admit(newTestSample("R07", 2, 1, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Outcome == OutcomeDuplicate {
		t.Fatalf("same sequence in new generation must not be a duplicate: %q", rolled.Outcome)
	}
	// Same sequence for a different asset is also distinct.
	other, err := repository.Admit(newTestSample("R08", 1, 1, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if other.Outcome == OutcomeDuplicate {
		t.Fatalf("same sequence for another asset must not be a duplicate: %q", other.Outcome)
	}
}

func TestOutOfOrderSamplesBufferUntilGapCloses(t *testing.T) {
	repository := NewRepository()

	// Sequence 3 arrives first: retained but not confirmed.
	gapped, err := repository.Admit(newTestSample("R07", 1, 3, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if gapped.Outcome != OutcomeOutOfOrder || len(gapped.NewlyConfirmed) != 0 {
		t.Fatalf("seq 3 should be out of order with no confirmations, got %+v", gapped)
	}
	if confirmed := repository.ConfirmedSamples("R07", 1); len(confirmed) != 0 {
		t.Fatalf("buffered sample leaked into confirmed set: %+v", confirmed)
	}

	// Sequence 1 confirms only itself (gap at 2 remains).
	one, err := repository.Admit(newTestSample("R07", 1, 1, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if one.Outcome != OutcomeAccepted || len(one.NewlyConfirmed) != 1 {
		t.Fatalf("seq 1 admission = %+v", one)
	}

	// Sequence 2 closes the gap and confirms both 2 and the buffered 3, in order.
	two, err := repository.Admit(newTestSample("R07", 1, 2, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if two.Outcome != OutcomeAccepted {
		t.Fatalf("gap-filler outcome = %q, want accepted", two.Outcome)
	}
	if len(two.NewlyConfirmed) != 2 ||
		two.NewlyConfirmed[0].Sequence != 2 || two.NewlyConfirmed[1].Sequence != 3 {
		t.Fatalf("closing the gap should confirm sequences 2,3 in order, got %+v", two.NewlyConfirmed)
	}

	confirmed := repository.ConfirmedSamples("R07", 1)
	if len(confirmed) != 3 {
		t.Fatalf("confirmed set = %+v", confirmed)
	}
}

func TestIngestorProjectsOnlyConfirmedSamples(t *testing.T) {
	repository := NewRepository()
	sink := &recordSink{}
	ingestor := NewIngestor(repository, time.Now, sink)
	ctx := context.Background()

	if result, err := ingestor.Apply(ctx, newTestSample("R07", 1, 2, KindCurrent)); err != nil {
		t.Fatal(err)
	} else if result.Outcome != OutcomeOutOfOrder || len(result.Confirmed) != 0 {
		t.Fatalf("buffered sample = %+v", result)
	}
	if len(sink.samples) != 0 {
		t.Fatalf("sink consumed an unconfirmed sample: %+v", sink.samples)
	}

	// Seq 1 closes nothing by itself except that seq 2 was already buffered, so
	// both become confirmed and are delivered to the sink in sequence order.
	if result, err := ingestor.Apply(ctx, newTestSample("R07", 1, 1, KindCurrent)); err != nil {
		t.Fatal(err)
	} else if len(result.Confirmed) != 2 ||
		result.Confirmed[0].Sequence != 1 || result.Confirmed[1].Sequence != 2 {
		t.Fatalf("seq 1 result = %+v", result)
	}

	// Re-delivering sequence 1 is a duplicate and never reaches the sink.
	if result, err := ingestor.Apply(ctx, newTestSample("R07", 1, 1, KindCurrent)); err != nil {
		t.Fatal(err)
	} else if result.Outcome != OutcomeDuplicate {
		t.Fatalf("repeat result = %+v", result)
	}
	if len(sink.samples) != 2 || sink.samples[0].Sequence != 1 || sink.samples[1].Sequence != 2 {
		t.Fatalf("sink saw a duplicate or out-of-order projection: %+v", sink.samples)
	}
}

func TestReplayFromCursorIsGapFreeAndIdempotent(t *testing.T) {
	repository := NewRepository()
	sink := &recordSink{}
	ingestor := NewIngestor(repository, time.Now, sink)
	ctx := context.Background()

	// Prime history straight through the repository, simulating recovered data.
	for _, sample := range []Sample{
		newTestSample("R07", 1, 1, KindCurrent),
		newTestSample("R07", 1, 2, KindCurrent),
		newTestSample("R07", 1, 3, KindCurrent),
		newTestSample("R08", 1, 1, KindCurrent),
	} {
		if _, err := repository.Admit(sample); err != nil {
			t.Fatal(err)
		}
	}

	// First replay from a fresh cursor delivers all four, per-stream in order.
	report, err := ingestor.Replay(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Delivered != 4 || len(report.Samples) != 4 {
		t.Fatalf("first replay delivered %d samples: %+v", report.Delivered, report.Samples)
	}
	if got := report.Cursor.Position("R07", 1); got != 3 {
		t.Fatalf("cursor R07 gen1 = %d, want 3", got)
	}
	if got := report.Cursor.Position("R08", 1); got != 1 {
		t.Fatalf("cursor R08 gen1 = %d, want 1", got)
	}

	// Replaying from the advanced cursor delivers nothing: no loss, no repeats.
	sink.samples = nil
	again, err := ingestor.Replay(ctx, report.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if again.Delivered != 0 || len(sink.samples) != 0 {
		t.Fatalf("cursor replay re-delivered samples: %+v", again)
	}

	// A new confirmed sample past the cursor is the only thing delivered next.
	if _, err := repository.Admit(newTestSample("R07", 1, 4, KindCurrent)); err != nil {
		t.Fatal(err)
	}
	continued, err := ingestor.Replay(ctx, report.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if continued.Delivered != 1 || continued.Samples[0].Sequence != 4 {
		t.Fatalf("resumed replay should deliver only seq 4, got %+v", continued.Samples)
	}
}

func TestReplayDoesNotDeliverSamplesBehindAGap(t *testing.T) {
	repository := NewRepository()
	sink := &recordSink{}
	ingestor := NewIngestor(repository, time.Now, sink)

	// Sequences 1 and 3 are present; 2 is missing, so 3 must not replay.
	for _, sequence := range []uint64{1, 3} {
		if _, err := repository.Admit(newTestSample("R07", 1, sequence, KindCurrent)); err != nil {
			t.Fatal(err)
		}
	}
	report, err := ingestor.Replay(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Delivered != 1 || report.Samples[0].Sequence != 1 {
		t.Fatalf("replay leaked an unconfirmed sample: %+v", report.Samples)
	}
	if got := report.Cursor.Position("R07", 1); got != 1 {
		t.Fatalf("cursor advanced past an unconfirmed sample: %d", got)
	}

	// Once the gap fills, resuming delivers exactly the remaining two.
	if _, err := repository.Admit(newTestSample("R07", 1, 2, KindCurrent)); err != nil {
		t.Fatal(err)
	}
	resumed, err := ingestor.Replay(context.Background(), report.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Delivered != 2 ||
		resumed.Samples[0].Sequence != 2 || resumed.Samples[1].Sequence != 3 {
		t.Fatalf("resumed replay = %+v", resumed.Samples)
	}
}

func TestCurrentSamplesOnlyUsesConfirmedCurrentGeneration(t *testing.T) {
	repository := NewRepository()
	// Gen 1 fully confirmed; gen 2 has a buffered sample behind a gap.
	for _, sequence := range []uint64{1, 2, 3} {
		if err := repository.Add(newTestSample("R07", 1, sequence, KindCurrent)); err != nil {
			t.Fatal(err)
		}
	}
	if err := repository.Add(newTestSample("R07", 2, 2, KindCurrent)); err != nil {
		t.Fatal(err)
	}

	samples := CurrentSamples(repository, "R07", 0)
	// Current generation is 2; its only sample is unconfirmed, so nothing yet.
	if len(samples) != 0 {
		t.Fatalf("stability window saw an unconfirmed/new-generation sample: %+v", samples)
	}

	if err := repository.Add(newTestSample("R07", 2, 1, KindCurrent)); err != nil {
		t.Fatal(err)
	}
	samples = CurrentSamples(repository, "R07", 1)
	if len(samples) != 1 || samples[0].Sequence != 2 {
		t.Fatalf("expected only confirmed gen2 sample after seq 1, got %+v", samples)
	}
}

func TestMuteManagerReleasesOnContextCancellation(t *testing.T) {
	manager := NewMuteManager(time.Now)
	manager.Acquire("mute-1", "S3", time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		manager.Run(ctx, "mute-1", time.Second)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mute renewal did not stop")
	}
	if manager.Muted("S3") {
		t.Fatal("mute remained active after cancellation")
	}
}
