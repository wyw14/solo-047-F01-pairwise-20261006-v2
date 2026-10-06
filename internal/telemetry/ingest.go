package telemetry

import (
	"context"
	"fmt"
	"time"
)

type ProjectionSink interface {
	ApplySample(context.Context, Sample) error
}

type Ingestor struct {
	repository *Repository
	sinks      []ProjectionSink
	now        func() time.Time
}

func NewIngestor(repository *Repository, now func() time.Time, sinks ...ProjectionSink) *Ingestor {
	return &Ingestor{repository: repository, sinks: sinks, now: now}
}

// IngestResult is the explicit outcome of admitting one inbound sample.
// Outcome is accepted, out_of_order or duplicate; Confirmed lists the samples
// that were actually delivered to projection sinks during this call, in
// ascending sequence order (it can contain several samples when an arrival
// closes a sequence gap).
type IngestResult struct {
	Outcome   Outcome  `json:"outcome"`
	Sample    Sample   `json:"sample"`
	Existing  Sample   `json:"existing,omitempty"`
	Confirmed []Sample `json:"confirmed,omitempty"`
}

func (i *Ingestor) Apply(ctx context.Context, sample Sample) (IngestResult, error) {
	if sample.ReceivedAt.IsZero() {
		sample.ReceivedAt = i.now().UTC()
	}
	admission, err := i.repository.Admit(sample)
	if err != nil {
		return IngestResult{}, fmt.Errorf("store telemetry sample: %w", err)
	}
	result := IngestResult{
		Outcome:  admission.Outcome,
		Sample:   admission.Sample,
		Existing: admission.Existing,
	}
	// Only confirmed samples may drive projections. Duplicates are dropped and
	// buffered out-of-order samples wait until the gap closes, at which point
	// they are delivered in sequence order.
	for _, confirmed := range admission.NewlyConfirmed {
		for _, sink := range i.sinks {
			if err := sink.ApplySample(ctx, confirmed); err != nil {
				return result, fmt.Errorf("project telemetry sample: %w", err)
			}
		}
		result.Confirmed = append(result.Confirmed, confirmed)
	}
	return result, nil
}

// ReplayReport summarizes a cursor-driven replay.
type ReplayReport struct {
	// Delivered is the number of confirmed samples handed to the sinks.
	Delivered int
	// Samples delivered during the replay, in delivery order.
	Samples []Sample
	// Cursor is advanced to the highest sequence delivered per stream. Persist
	// it after a successful replay; restarting from it neither skips nor
	// re-delivers any sample.
	Cursor *Cursor
}

// Replay re-delivers to the sinks every confirmed sample strictly past the
// cursor, in asset/generation/sequence order. It never re-admits samples, so
// history is not rewritten; the cursor makes replay idempotent. If a sink
// fails midway the returned cursor has advanced only over delivered samples,
// so resuming from it loses nothing and duplicates nothing.
func (i *Ingestor) Replay(ctx context.Context, cursor *Cursor) (ReplayReport, error) {
	pending := i.repository.ConfirmedSince(cursor)
	advanced := NewCursor()
	advanced.Merge(cursor)
	report := ReplayReport{Cursor: advanced}
	for _, sample := range pending {
		for _, sink := range i.sinks {
			if err := sink.ApplySample(ctx, sample); err != nil {
				return report, fmt.Errorf("replay telemetry sample %s: %w", sample.ID, err)
			}
		}
		advanced.Advance(sample.AssetID, sample.Generation, sample.Sequence)
		report.Samples = append(report.Samples, sample)
		report.Delivered++
	}
	return report, nil
}
