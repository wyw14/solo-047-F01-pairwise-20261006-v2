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

// IngestStatus 是入站样本的明确处理结果。
type IngestStatus string

const (
	// StatusAccepted 样本填补了期望序号并得到确认；可能连带确认了缓冲区中连续的乱序样本。
	StatusAccepted IngestStatus = "accepted"
	// StatusDuplicate 同一（资产、代次、序号）的样本已经确认或已在缓冲，本次写入被丢弃。
	StatusDuplicate IngestStatus = "duplicate"
	// StatusOutOfOrder 序号超前（中间存在缺口）或属于已经滚过的旧代次；
	// 超前样本会进入缓冲，待缺口补齐后确认，旧代次迟到样本不再确认。
	StatusOutOfOrder IngestStatus = "out_of_order"
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

// AdmitResult 是 Repository 对一次入站的原子判定结果。
type AdmitResult struct {
	// Status 为 accepted / duplicate / out_of_order。
	Status IngestStatus
	// Confirmed 仅在 StatusAccepted 时非空，包含本次新确认的样本，
	// 按序号升序（传入样本在前，连带补齐的乱序样本随后）。
	Confirmed []Sample
	// Buffered 配合 StatusOutOfOrder：true 表示样本已进入乱序缓冲，缺口补齐后会确认；
	// false 表示属于已经滚过的旧代次，永远不会再确认。
	Buffered bool
	// Generation 资产当前代次。
	Generation uint64
	// Expected 下一个等待确认的序号。
	Expected uint64
}

type sequenceKey struct {
	generation uint64
	sequence   uint64
}

type pendingSample struct {
	sample Sample
	// offset 是样本在事件流中的到达偏移，回放时用作游标屏障：样本未确认前游标不得越过它。
	// 实时入站（不经事件流）为 0，表示不设屏障。
	offset uint64
}

// assetState 记录单个资产在当前代次的连续确认进度。
type assetState struct {
	generation uint64
	// expected 是最近一个已确认序号；下一个有效样本必须是 expected+1。
	expected  uint64
	confirmed []Sample
	// pending 缓存已到达但序号超前的样本（序号 -> 样本）。
	pending map[uint64]pendingSample
	// archive 保存已经滚过的旧代次里已确认的样本。
	archive []Sample
	// archived 记录旧代次已确认的（代次、序号），用于识别跨代次重复。
	archived map[sequenceKey]struct{}
}

func (s *assetState) rollover(generation uint64) {
	if s.archived == nil {
		s.archived = make(map[sequenceKey]struct{})
	}
	for _, sample := range s.confirmed {
		s.archived[sequenceKey{sample.Generation, sample.Sequence}] = struct{}{}
	}
	s.archive = append(s.archive, s.confirmed...)
	s.confirmed = nil
	// 旧代次未补齐缺口的缓冲样本随代次作废，永不确认，其回放屏障也一并释放。
	s.pending = make(map[uint64]pendingSample)
	s.generation = generation
	s.expected = 0
}

func (s *assetState) wasConfirmed(generation, sequence uint64) bool {
	_, ok := s.archived[sequenceKey{generation, sequence}]
	return ok
}

type Repository struct {
	mu     sync.RWMutex
	assets map[string]*assetState
}

func NewRepository() *Repository {
	return &Repository{assets: make(map[string]*assetState)}
}

// Plan 按（资产、代次、序号）对样本做原子去重与乱序判定，但不落确认：
//   - duplicate：同一（资产、代次、序号）已确认或已缓冲；
//   - out_of_order：超前序号立即进入缓冲（缓冲本身不投影，无需回滚）；
//     旧代次迟到样本终态拒绝、不缓冲；
//   - accepted：命中期望序号，Confirmed 给出本次将确认的连锁链（含缓冲中连续补齐的样本），
//     但仓库状态不变，必须随后调用 Commit 才真正落确认。
//
// 调用方（Ingestor）负责先把 Confirmed 投影成功、再 Commit；投影失败则不 Commit，
// 重试时同一序号会重新被 Plan 接受，从而不漏。
// arrivalOffset 是样本在事件流中的偏移（游标回放时传入，实时入站传 0），
// 缓冲期间形成游标屏障，防止游标越过尚未投影的样本。
func (r *Repository) Plan(sample Sample, arrivalOffset uint64) (AdmitResult, error) {
	if err := sample.Validate(); err != nil {
		return AdmitResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.ensure(sample.AssetID, sample.Generation)

	if sample.Generation < state.generation {
		if state.wasConfirmed(sample.Generation, sample.Sequence) {
			return AdmitResult{Status: StatusDuplicate, Generation: state.generation, Expected: state.expected}, nil
		}
		// 旧代次的迟到样本无法再确认，按乱序处理且不缓冲。
		return AdmitResult{
			Status: StatusOutOfOrder, Buffered: false,
			Generation: state.generation, Expected: state.expected,
		}, nil
	}

	if sample.Generation > state.generation {
		state.rollover(sample.Generation)
	}

	if sample.Sequence <= state.expected {
		// 当前代次内 expected 之前的序号必然已确认，因此必为重复。
		return AdmitResult{Status: StatusDuplicate, Generation: state.generation, Expected: state.expected}, nil
	}
	if _, exists := state.pending[sample.Sequence]; exists {
		return AdmitResult{Status: StatusDuplicate, Generation: state.generation, Expected: state.expected}, nil
	}
	// 代次内首个样本建立基线（序号不必从 1 开始，换代次后序号会重置）；
	// 此后必须严格连续，超前序号先缓冲，等待缺口补齐。
	if state.expected != 0 && sample.Sequence != state.expected+1 {
		state.pending[sample.Sequence] = pendingSample{sample: sample, offset: arrivalOffset}
		return AdmitResult{
			Status: StatusOutOfOrder, Buffered: true,
			Generation: state.generation, Expected: state.expected,
		}, nil
	}

	// 计算连锁确认链但暂不落库：触发样本 + 缓冲区中连续的后续序号。
	chain := []Sample{sample}
	for sequence := sample.Sequence + 1; ; sequence++ {
		waited, ok := state.pending[sequence]
		if !ok {
			break
		}
		chain = append(chain, waited.sample)
	}
	return AdmitResult{
		Status: StatusAccepted, Confirmed: chain,
		Generation: state.generation, Expected: chain[len(chain)-1].Sequence,
	}, nil
}

// Commit 落定一次 accepted 计划：把连锁链写入已确认历史、移出缓冲并推进水位线。
// 仅应在 Plan 返回 accepted 且投影成功后调用。
func (r *Repository) Commit(chain []Sample) {
	if len(chain) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.assets[chain[0].AssetID]
	if state == nil {
		state = r.ensure(chain[0].AssetID, chain[0].Generation)
	}
	for _, sample := range chain {
		delete(state.pending, sample.Sequence)
		state.confirmed = append(state.confirmed, sample)
		state.expected = sample.Sequence
	}
}

func (r *Repository) ensure(assetID string, generation uint64) *assetState {
	state := r.assets[assetID]
	if state == nil {
		state = &assetState{generation: generation, pending: make(map[uint64]pendingSample)}
		r.assets[assetID] = state
	}
	return state
}

// LowestPendingOffset 返回所有资产当前缓冲中样本的最小事件流偏移；
// 没有带偏移的缓冲样本（含纯实时入站）时返回 0。游标必须停在此偏移之前。
func (r *Repository) LowestPendingOffset() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var lowest uint64
	for _, state := range r.assets {
		for _, pending := range state.pending {
			if pending.offset == 0 {
				continue
			}
			if lowest == 0 || pending.offset < lowest {
				lowest = pending.offset
			}
		}
	}
	return lowest
}

// Pending 返回资产当前代次已接收但尚未确认的样本（乱序缓冲），按序号升序。
func (r *Repository) Pending(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.assets[assetID]
	if state == nil {
		return nil
	}
	items := make([]Sample, 0, len(state.pending))
	for _, pending := range state.pending {
		items = append(items, pending.sample)
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].Sequence < items[j].Sequence })
	return items
}

// History 返回资产所有已确认样本（含已滚过的代次），按代次、序号升序；
// 重复样本和尚未补齐缺口的乱序样本不会出现。
func (r *Repository) History(assetID string) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.assets[assetID]
	if state == nil {
		return nil
	}
	items := make([]Sample, 0, len(state.archive)+len(state.confirmed))
	items = append(items, state.archive...)
	items = append(items, state.confirmed...)
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Generation != items[j].Generation {
			return items[i].Generation < items[j].Generation
		}
		if items[i].Sequence != items[j].Sequence {
			return items[i].Sequence < items[j].Sequence
		}
		return items[i].ObservedAt.Before(items[j].ObservedAt)
	})
	return items
}

// Confirmed 返回资产当前代次、指定种类且序号大于 after 的已确认样本，按序号升序。
// 乱序缓冲中的样本与旧代次样本都不会返回，供健康/稳定窗口只消费已确认数据。
func (r *Repository) Confirmed(assetID string, kind Kind, after uint64) []Sample {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.assets[assetID]
	if state == nil {
		return nil
	}
	var result []Sample
	for _, sample := range state.confirmed {
		if sample.Kind == kind && sample.Sequence > after {
			result = append(result, sample)
		}
	}
	return result
}

func (r *Repository) Latest(assetID string, kind Kind) (Sample, bool) {
	items := r.History(assetID)
	for index := len(items) - 1; index >= 0; index-- {
		if items[index].Kind == kind {
			return items[index], true
		}
	}
	return Sample{}, false
}
