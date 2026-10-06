package telemetry

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ProjectionSink interface {
	// ApplySample 必须按（资产、代次、序号）幂等：游标回放可能重复投递同一样本。
	ApplySample(context.Context, Sample) error
}

// CursorStore 持久化投影水位线。游标之前（含）的事件都已成功投影。
type CursorStore interface {
	LoadTelemetryCursor() (uint64, bool, error)
	SaveTelemetryCursor(offset uint64) error
}

// IngestResult 是入站流程对单个样本的明确结果。
type IngestResult struct {
	Status IngestStatus `json:"status"`
	Sample Sample       `json:"sample"`
	// Generation 资产当前代次。
	Generation uint64 `json:"generation"`
	// Expected 处理后下一个等待确认的序号。
	Expected uint64 `json:"expected"`
	// Buffered 配合 out_of_order：样本已缓冲待缺口补齐（true），
	// 或属于已滚过的旧代次、永不确认（false）。
	Buffered bool `json:"buffered"`
	// Projected 本次实际投递给下游的样本数；accepted 且补齐缺口时可能大于 1。
	Projected int `json:"projected"`
}

// ReplayEvent 是按偏移量排序的一条待回放遥测事件。
type ReplayEvent struct {
	Offset uint64
	Sample Sample
}

// ReplayReport 汇总一次游标回放的结果。
type ReplayReport struct {
	Cursor     uint64 `json:"cursor"`
	Applied    int    `json:"applied"`
	Duplicate  int    `json:"duplicate"`
	OutOfOrder int    `json:"out_of_order"`
	// Skipped 是因偏移量不大于游标而跳过的事件数。
	Skipped int `json:"skipped"`
	// Paused 表示游标被乱序缓冲屏障挡住：仍有事件未投影，
	// 需要等缺口样本到达后再调用一次 ReplayFrom 推进。
	Paused bool `json:"paused"`
}

type Ingestor struct {
	repository *Repository
	sinks      []ProjectionSink
	now        func() time.Time
	// serialize 保证“计划 -> 投影 -> 提交”不被另一次入站/回放交错，
	// 同资产同序号不会被两个协程同时 Plan 成 accepted。
	serialize sync.Mutex
}

func NewIngestor(repository *Repository, now func() time.Time, sinks ...ProjectionSink) *Ingestor {
	return &Ingestor{repository: repository, sinks: sinks, now: now}
}

// Apply 处理实时入站的单个样本（无事件流偏移）。
func (i *Ingestor) Apply(ctx context.Context, sample Sample) (IngestResult, error) {
	return i.dispatch(ctx, sample, 0)
}

// dispatch 对单个样本去重并入站：
//   - accepted：样本（及补齐缺口后连带确认的乱序样本）按序号投递给所有 sink；
//   - duplicate：同一（资产、代次、序号）已存在，不再写入历史、不投影；
//   - out_of_order：超前样本已缓冲待缺口补齐（本次不投影），旧代次迟到样本直接拒绝。
func (i *Ingestor) dispatch(ctx context.Context, sample Sample, arrivalOffset uint64) (IngestResult, error) {
	if sample.ReceivedAt.IsZero() {
		sample.ReceivedAt = i.now().UTC()
	}
	i.serialize.Lock()
	defer i.serialize.Unlock()

	planned, err := i.repository.Plan(sample, arrivalOffset)
	if err != nil {
		return IngestResult{}, fmt.Errorf("store telemetry sample: %w", err)
	}
	result := IngestResult{
		Status: planned.Status, Sample: sample, Buffered: planned.Buffered,
		Generation: planned.Generation, Expected: planned.Expected,
	}
	if planned.Status != StatusAccepted {
		return result, nil
	}
	// 两阶段：先投影连锁链（按序号升序），成功后才落确认。
	for _, confirmed := range planned.Confirmed {
		for _, sink := range i.sinks {
			if err := sink.ApplySample(ctx, confirmed); err != nil {
				return result, fmt.Errorf("project telemetry sample %s/%d/%d: %w",
					confirmed.AssetID, confirmed.Generation, confirmed.Sequence, err)
			}
		}
	}
	i.repository.Commit(planned.Confirmed)
	result.Projected = len(planned.Confirmed)
	return result, nil
}

// ReplayFrom 从游标（不含游标）按偏移量顺序回放事件。
//
// 不漏：投影失败立即中止且游标不前进，重试时从同一偏移重新投递；
//
//	若某序号超前缓冲（其偏移 B 先到、缺口样本后到），游标停在 B-1，
//	缺口补齐、缓冲样本被投影后才继续，缺口样本绝不会被游标跳过。
//
// 不重：偏移量不大于游标的事件直接跳过；去重以（资产、代次、序号）为准，
//
//	sink 幂等，因此“投影已发生但游标尚未保存”的崩溃只会重复投影，不会重复确认。
//
// 旧代次迟到样本是终态拒绝，不设屏障，游标正常越过它。
func (i *Ingestor) ReplayFrom(ctx context.Context, events []ReplayEvent, cursors CursorStore) (ReplayReport, error) {
	report := ReplayReport{}
	if cursors != nil {
		if stored, found, err := cursors.LoadTelemetryCursor(); err != nil {
			return report, fmt.Errorf("load telemetry cursor: %w", err)
		} else if found {
			report.Cursor = stored
		}
	}
	for _, event := range events {
		if event.Offset <= report.Cursor {
			report.Skipped++
			continue
		}
		result, err := i.dispatch(ctx, event.Sample, event.Offset)
		if err != nil {
			return report, err
		}
		switch result.Status {
		case StatusAccepted:
			report.Applied += result.Projected
		case StatusDuplicate:
			report.Duplicate++
		case StatusOutOfOrder:
			report.OutOfOrder++
		}

		// 候选水位：终态事件可越过当前偏移；刚缓冲的事件自身不能越过。
		candidate := event.Offset
		if result.Status == StatusOutOfOrder && result.Buffered {
			if event.Offset > 0 {
				candidate = event.Offset - 1
			} else {
				candidate = 0
			}
		}
		// 全局夹紧：只要还有带偏移的缓冲样本未投影，游标必须停在最低屏障之前。
		if lowest := i.repository.LowestPendingOffset(); lowest > 0 && lowest-1 < candidate {
			candidate = lowest - 1
		}
		if candidate > report.Cursor {
			report.Cursor = candidate
			if cursors != nil {
				if err := cursors.SaveTelemetryCursor(candidate); err != nil {
					return report, fmt.Errorf("save telemetry cursor: %w", err)
				}
			}
		}
	}
	if i.repository.LowestPendingOffset() != 0 {
		// 仍有未投影的缓冲样本，等待缺口样本到达后再回放推进。
		report.Paused = true
	}
	return report, nil
}
