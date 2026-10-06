package telemetry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func sampleAt(asset string, generation, sequence uint64, kind Kind) Sample {
	return Sample{
		ID: "sample", AssetID: asset, Kind: kind, Value: float64(sequence),
		Sequence: sequence, Generation: generation,
		ObservedAt: time.Date(2026, 10, 5, 1, 0, int(sequence), 0, time.UTC),
	}
}

// admit 模拟“投影成功后提交”的正常路径，用于直接驱动 Repository 判定。
func admit(repository *Repository, sample Sample) (AdmitResult, error) {
	result, err := repository.Plan(sample, 0)
	if err != nil {
		return result, err
	}
	if result.Status == StatusAccepted {
		repository.Commit(result.Confirmed)
	}
	return result, nil
}

func TestRepositoryHistoryOrderedBySequence(t *testing.T) {
	repository := NewRepository()
	// 顺序确认 1..3，历史按序号升序保存。
	for _, sequence := range []uint64{1, 2, 3} {
		result, err := admit(repository, sampleAt("R07", 1, sequence, KindCurrent))
		if err != nil {
			t.Fatal(err)
		}
		if result.Status != StatusAccepted {
			t.Fatalf("seq %d should accept, got %s", sequence, result.Status)
		}
	}
	history := repository.History("R07")
	if len(history) != 3 || history[0].Sequence != 1 || history[2].Sequence != 3 {
		t.Fatalf("samples not ordered by sequence: %+v", history)
	}
}

func TestRepositoryFirstSampleEstablishesBaseline(t *testing.T) {
	repository := NewRepository()
	// 代次内首个样本即使序号不是 1 也作为基线接受（换代次后序号会重置）。
	result, err := admit(repository, sampleAt("R07", 1, 10, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusAccepted || result.Expected != 10 {
		t.Fatalf("first sample should establish baseline at seq 10, got %+v", result)
	}
	// 基线之后必须严格连续。
	if r, err := admit(repository, sampleAt("R07", 1, 12, KindCurrent)); err != nil || r.Status != StatusOutOfOrder {
		t.Fatalf("seq 12 must buffer until 11 arrives, got %+v err=%v", r, err)
	}
	filled, err := admit(repository, sampleAt("R07", 1, 11, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if filled.Status != StatusAccepted || len(filled.Confirmed) != 2 {
		t.Fatalf("seq 11 should confirm with buffered 12, got %+v", filled)
	}
}

func TestRepositoryDuplicateByAssetGenerationSequence(t *testing.T) {
	repository := NewRepository()
	first, err := admit(repository, sampleAt("R07", 1, 5, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != StatusAccepted || first.Expected != 5 {
		t.Fatalf("baseline accept: %+v", first)
	}
	dup, err := admit(repository, sampleAt("R07", 1, 5, KindTemperature))
	if err != nil {
		t.Fatal(err)
	}
	if dup.Status != StatusDuplicate {
		t.Fatalf("same asset/generation/sequence must be duplicate regardless of kind, got %s", dup.Status)
	}
	// 不同资产同序号不是重复。
	other, err := admit(repository, sampleAt("R08", 1, 5, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if other.Status != StatusAccepted {
		t.Fatalf("different asset should accept, got %s", other.Status)
	}
	if history := repository.History("R07"); len(history) != 1 {
		t.Fatalf("duplicate must not be written to history, len=%d", len(history))
	}
}

func TestRepositoryOutOfOrderFillsGapAndConfirmsInOrder(t *testing.T) {
	repository := NewRepository()
	// 基线 seq1，随后 seq3 超前缓冲。
	if r, err := admit(repository, sampleAt("R07", 1, 1, KindCurrent)); err != nil || r.Status != StatusAccepted {
		t.Fatalf("baseline: %+v err=%v", r, err)
	}
	buffered, err := admit(repository, sampleAt("R07", 1, 3, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if buffered.Status != StatusOutOfOrder || !buffered.Buffered {
		t.Fatalf("seq3 should buffer, got %+v", buffered)
	}
	if got := repository.Pending("R07"); len(got) != 1 || got[0].Sequence != 3 {
		t.Fatalf("pending mismatch: %+v", got)
	}
	// seq2 补齐缺口，应连带按序确认 2、3。
	filled, err := admit(repository, sampleAt("R07", 1, 2, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if filled.Status != StatusAccepted {
		t.Fatalf("gap fill should accept, got %s", filled.Status)
	}
	if len(filled.Confirmed) != 2 || filled.Confirmed[0].Sequence != 2 || filled.Confirmed[1].Sequence != 3 {
		t.Fatalf("expected chained confirmation [2 3], got %+v", filled.Confirmed)
	}
	if len(repository.Pending("R07")) != 0 {
		t.Fatal("pending buffer should drain after gap fill")
	}
	history := repository.History("R07")
	if len(history) != 3 || history[0].Sequence != 1 || history[2].Sequence != 3 {
		t.Fatalf("history order mismatch: %+v", history)
	}
}

func TestRepositoryGenerationRolloverResetsSequence(t *testing.T) {
	repository := NewRepository()
	for _, sequence := range []uint64{1, 2} {
		if _, err := admit(repository, sampleAt("R07", 1, sequence, KindCurrent)); err != nil {
			t.Fatal(err)
		}
	}
	// 新代次序号从小数重新开始，应作为新基线接受。
	reset, err := admit(repository, sampleAt("R07", 2, 1, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if reset.Status != StatusAccepted || reset.Generation != 2 || reset.Expected != 1 {
		t.Fatalf("generation rollover baseline mismatch: %+v", reset)
	}
	// 旧代次迟到样本：已确认序号判为重复，从未确认的缺口序号判为终态乱序。
	oldDup, err := admit(repository, sampleAt("R07", 1, 2, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if oldDup.Status != StatusDuplicate {
		t.Fatalf("confirmed old-generation sample should be duplicate, got %s", oldDup.Status)
	}
	oldLate, err := admit(repository, sampleAt("R07", 1, 9, KindCurrent))
	if err != nil {
		t.Fatal(err)
	}
	if oldLate.Status != StatusOutOfOrder || oldLate.Buffered {
		t.Fatalf("unconfirmed old-generation sample must be terminal out-of-order, got %+v", oldLate)
	}
	// 历史包含两个代次且按代次、序号排序。
	history := repository.History("R07")
	if len(history) != 3 {
		t.Fatalf("history should keep archived generations, got %d", len(history))
	}
	if history[0].Generation != 1 || history[2].Generation != 2 {
		t.Fatalf("history not ordered by generation then sequence: %+v", history)
	}
}

func TestConfirmedOnlyReturnsCurrentGenerationInOrder(t *testing.T) {
	repository := NewRepository()
	for _, sequence := range []uint64{1, 2, 3} {
		sample := sampleAt("R07", 1, sequence, KindCurrent)
		if sequence == 2 {
			sample.Kind = KindTemperature
		}
		if _, err := admit(repository, sample); err != nil {
			t.Fatal(err)
		}
	}
	// seq5 超前缓冲，绝不能出现在 Confirmed 中。
	if _, err := admit(repository, sampleAt("R07", 1, 5, KindCurrent)); err != nil {
		t.Fatal(err)
	}
	got := repository.Confirmed("R07", KindCurrent, 0)
	if len(got) != 2 || got[0].Sequence != 1 || got[1].Sequence != 3 {
		t.Fatalf("confirmed must exclude pending and other kinds, got %+v", got)
	}
	// 换代次后 Confirmed 只看当前代次。
	if _, err := admit(repository, sampleAt("R07", 2, 1, KindCurrent)); err != nil {
		t.Fatal(err)
	}
	if got := repository.Confirmed("R07", KindCurrent, 0); len(got) != 1 || got[0].Generation != 2 {
		t.Fatalf("confirmed must be scoped to current generation, got %+v", got)
	}
}

type recordingSink struct {
	mu      sync.Mutex
	samples []Sample
	failAt  *sampleKey
}

type sampleKey struct {
	asset      string
	generation uint64
	sequence   uint64
}

func (s *recordingSink) ApplySample(_ context.Context, sample Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failAt != nil && s.failAt.asset == sample.AssetID &&
		s.failAt.generation == sample.Generation && s.failAt.sequence == sample.Sequence {
		return errors.New("sink failure")
	}
	s.samples = append(s.samples, sample)
	return nil
}

func (s *recordingSink) sequences() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, len(s.samples))
	for i, sample := range s.samples {
		out[i] = sample.Sequence
	}
	return out
}

func TestIngestorClassifiesAndProjectsOnlyAccepted(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{}
	ingestor := NewIngestor(repository, time.Now, sink)
	ctx := context.Background()

	accepted, err := ingestor.Apply(ctx, sampleAt("R07", 1, 1, KindCurrent))
	if err != nil || accepted.Status != StatusAccepted || accepted.Projected != 1 {
		t.Fatalf("accept: %+v err=%v", accepted, err)
	}
	duplicate, err := ingestor.Apply(ctx, sampleAt("R07", 1, 1, KindCurrent))
	if err != nil || duplicate.Status != StatusDuplicate || duplicate.Projected != 0 {
		t.Fatalf("duplicate: %+v err=%v", duplicate, err)
	}
	outOfOrder, err := ingestor.Apply(ctx, sampleAt("R07", 1, 3, KindCurrent))
	if err != nil || outOfOrder.Status != StatusOutOfOrder || !outOfOrder.Buffered || outOfOrder.Projected != 0 {
		t.Fatalf("out of order: %+v err=%v", outOfOrder, err)
	}
	// 补齐缺口，seq2 与缓冲的 seq3 一起按序投影。
	filled, err := ingestor.Apply(ctx, sampleAt("R07", 1, 2, KindCurrent))
	if err != nil || filled.Status != StatusAccepted || filled.Projected != 2 {
		t.Fatalf("gap fill: %+v err=%v", filled, err)
	}
	if got := sink.sequences(); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("sink must see contiguous in-order samples only, got %v", got)
	}
}

type memoryCursor struct {
	mu     sync.Mutex
	offset uint64
	saved  int
	fail   bool
}

func (c *memoryCursor) LoadTelemetryCursor() (uint64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset, c.offset != 0, nil
}

func (c *memoryCursor) SaveTelemetryCursor(offset uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return errors.New("cursor failure")
	}
	c.offset = offset
	c.saved++
	return nil
}

func TestReplaySkipsBelowCursorAndAdvancesWatermark(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{}
	cursor := &memoryCursor{offset: 2}
	ingestor := NewIngestor(repository, time.Now, sink)
	events := []ReplayEvent{
		{Offset: 1, Sample: sampleAt("R07", 1, 1, KindCurrent)},
		{Offset: 2, Sample: sampleAt("R07", 1, 2, KindCurrent)},
		{Offset: 3, Sample: sampleAt("R07", 1, 3, KindCurrent)},
	}
	report, err := ingestor.ReplayFrom(context.Background(), events, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 2 || report.Applied != 1 || report.Cursor != 3 {
		t.Fatalf("replay report mismatch: %+v", report)
	}
	if got := sink.sequences(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("only post-cursor event should project, got %v", got)
	}
}

func TestReplayIsIdempotentAcrossRuns(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{}
	cursor := &memoryCursor{}
	ingestor := NewIngestor(repository, time.Now, sink)
	events := []ReplayEvent{
		{Offset: 1, Sample: sampleAt("R07", 1, 1, KindCurrent)},
		{Offset: 2, Sample: sampleAt("R07", 1, 2, KindCurrent)},
	}
	for run := 0; run < 2; run++ {
		if _, err := ingestor.ReplayFrom(context.Background(), events, cursor); err != nil {
			t.Fatal(err)
		}
	}
	// 第二遍全部被游标跳过，sink 仍只见过每个样本一次——不重。
	if got := sink.sequences(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("second replay must not re-project, got %v", got)
	}
}

func TestReplayBarrierHoldsCursorUntilGapFilled(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{}
	cursor := &memoryCursor{}
	ingestor := NewIngestor(repository, time.Now, sink)
	// seq1 正常，seq3 先到（偏移 2）形成屏障，其它资产事件偏移 3 不应让游标越过 2。
	events := []ReplayEvent{
		{Offset: 1, Sample: sampleAt("R07", 1, 1, KindCurrent)},
		{Offset: 2, Sample: sampleAt("R07", 1, 3, KindCurrent)},
		{Offset: 3, Sample: sampleAt("R08", 1, 1, KindCurrent)},
	}
	report, err := ingestor.ReplayFrom(context.Background(), events, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if report.Cursor != 1 || !report.Paused {
		t.Fatalf("cursor must stop before buffered offset 2, got %+v", report)
	}
	if got := sink.sequences(); len(got) != 2 {
		t.Fatalf("R07 seq1 and R08 seq1 project, buffered seq3 must not, got %v", got)
	}
	// 崩溃重放：游标=1，seq1 重复跳过，seq3 再次缓冲，偏移3 的 R08 已投影过但仍是终态。
	sink.samples = nil
	report, err = ingestor.ReplayFrom(context.Background(), events, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if report.Cursor != 1 {
		t.Fatalf("cursor still barred at 1, got %d", report.Cursor)
	}
	if len(sink.samples) != 0 {
		t.Fatalf("restart replay must not double-project before gap fill, got %v", sink.sequences())
	}
	// 缺口样本 seq2 到达（偏移 4），连带确认 seq3；缓冲排空，游标推进到 4。
	more := []ReplayEvent{{Offset: 4, Sample: sampleAt("R07", 1, 2, KindCurrent)}}
	report, err = ingestor.ReplayFrom(context.Background(), more, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if report.Cursor != 4 || report.Paused {
		t.Fatalf("after gap fill cursor should reach 4 with no pending barrier, got %+v", report)
	}
	// 再重放历史事件：全部被游标跳过，不重复投影，游标保持 4。
	sink.samples = nil
	report, err = ingestor.ReplayFrom(context.Background(), append(events, more...), cursor)
	if err != nil {
		t.Fatal(err)
	}
	if report.Cursor != 4 || report.Skipped != 4 || len(sink.samples) != 0 {
		t.Fatalf("post-fill replay must be skip-only, report=%+v projected=%v", report, sink.sequences())
	}
}

func TestReplayStopsAdvancingCursorOnSinkFailure(t *testing.T) {
	repository := NewRepository()
	sink := &recordingSink{failAt: &sampleKey{asset: "R07", generation: 1, sequence: 2}}
	cursor := &memoryCursor{}
	ingestor := NewIngestor(repository, time.Now, sink)
	events := []ReplayEvent{
		{Offset: 1, Sample: sampleAt("R07", 1, 1, KindCurrent)},
		{Offset: 2, Sample: sampleAt("R07", 1, 2, KindCurrent)},
	}
	if _, err := ingestor.ReplayFrom(context.Background(), events, cursor); err == nil {
		t.Fatal("expected sink failure to abort replay")
	}
	if cursor.offset != 1 {
		t.Fatalf("cursor must not pass a failed projection, got %d", cursor.offset)
	}
	// 修复后重试：seq1 已确认（重复），seq2 投影成功，游标推进到 2——不漏。
	sink.failAt = nil
	report, err := ingestor.ReplayFrom(context.Background(), events, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if report.Cursor != 2 || report.Applied != 1 || report.Skipped != 1 {
		t.Fatalf("retry replay mismatch: %+v", report)
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
