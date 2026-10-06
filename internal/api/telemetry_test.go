package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

func postTelemetry(t *testing.T, server *Server, sequence, generation uint64, value float64) (int, telemetry.IngestResult) {
	t.Helper()
	body, _ := json.Marshal(TelemetryRequest{
		Kind: "temperature", Value: value, Unit: "C",
		Sequence: sequence, Generation: generation,
	})
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(
		http.MethodPost, "/api/repeaters/R07/telemetry", bytes.NewReader(body)))
	var result telemetry.IngestResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode ingest result %q: %v", recorder.Body.String(), err)
	}
	return recorder.Code, result
}

func getHistory(t *testing.T, server *Server) []telemetry.Sample {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(
		http.MethodGet, "/api/repeaters/R07/telemetry", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("history status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Samples []telemetry.Sample `json:"samples"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	return payload.Samples
}

func TestTelemetryIngestClassifiesAndDeduplicates(t *testing.T) {
	server := testServer(t)

	// seq1：有效，跳闸温度，202。
	if code, result := postTelemetry(t, server, 1, 1, 90); code != http.StatusAccepted ||
		result.Status != telemetry.StatusAccepted {
		t.Fatalf("first ingest code=%d result=%+v", code, result)
	}
	// 相同（资产、代次、序号）重发：200 duplicate，不重复入历史。
	if code, result := postTelemetry(t, server, 1, 1, 90); code != http.StatusOK ||
		result.Status != telemetry.StatusDuplicate {
		t.Fatalf("duplicate ingest code=%d result=%+v", code, result)
	}
	// seq3 超前：200 out_of_order，已缓冲但不进已确认历史。
	if code, result := postTelemetry(t, server, 3, 1, 91); code != http.StatusOK ||
		result.Status != telemetry.StatusOutOfOrder || !result.Buffered {
		t.Fatalf("out-of-order ingest code=%d result=%+v", code, result)
	}
	history := getHistory(t, server)
	if len(history) != 1 || history[0].Sequence != 1 {
		t.Fatalf("history must contain only the confirmed sample, got %+v", history)
	}
	// seq2 补齐缺口：202 accepted，连带确认 2、3。
	if code, result := postTelemetry(t, server, 2, 1, 92); code != http.StatusAccepted ||
		result.Status != telemetry.StatusAccepted || result.Projected != 2 {
		t.Fatalf("gap-fill ingest code=%d result=%+v", code, result)
	}
	history = getHistory(t, server)
	if len(history) != 3 {
		t.Fatalf("history should hold 3 confirmed samples, got %d", len(history))
	}
}

func TestTelemetryReplayIsIdempotentAfterLiveIngest(t *testing.T) {
	server := testServer(t)

	// 实时按序写入样本，其中 seq2 达到跳闸温度。
	for _, entry := range []struct {
		sequence uint64
		value    float64
	}{{1, 40}, {2, 90}, {3, 50}} {
		if _, result := postTelemetry(t, server, entry.sequence, 1, entry.value); result.Status == "" {
			t.Fatalf("seq %d not ingested", entry.sequence)
		}
	}

	// 从日志按游标回放：实时入站未推进过游标，因此全部重放，但应判为重复、
	// 健康投影水位线跳过重投，事故不会重复打开。
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(
		http.MethodPost, "/api/telemetry/replay", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var report telemetry.ReplayReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Duplicate != 3 || report.Applied != 0 {
		t.Fatalf("replay after live ingest must be duplicate-only, got %+v", report)
	}

	// 第二次回放：游标已覆盖全部事件，全部跳过且不再投影。
	recorder = httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequest(
		http.MethodPost, "/api/telemetry/replay", nil))
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Skipped != 3 || report.Applied != 0 {
		t.Fatalf("second replay must skip everything below cursor, got %+v", report)
	}

	// 历史与健康状态保持一致：历史仍是 3 条，seq2 触发的跳闸保持锁存且未被回放重置。
	if history := getHistory(t, server); len(history) != 3 {
		t.Fatalf("history changed after replays: %d samples", len(history))
	}
	target, err := server.services.Repeater("R07")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := target.Snapshot(); snapshot.Health != repeater.HealthTripped || snapshot.LastSequence != 3 {
		t.Fatalf("trip must stay latched with the same watermark after replay, got %+v", snapshot)
	}
}

func TestTelemetryReplayRebuildsFreshStateFromJournal(t *testing.T) {
	// 用共享的日志与游标目录构建“旧进程”，写入样本；再构建“新进程”从同一日志回放。
	data := t.TempDir()
	first := buildReplayServer(t, data)
	for _, entry := range []struct {
		sequence uint64
		value    float64
	}{{1, 30}, {2, 31}, {3, 32}} {
		postTelemetry(t, first, entry.sequence, 1, entry.value)
	}
	second := buildReplayServer(t, data)
	report, err := second.services.ReplayTelemetry(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if report.Applied != 3 || report.Duplicate != 0 {
		t.Fatalf("fresh-state replay must apply every journaled sample once, got %+v", report)
	}
	history := getHistory(t, second)
	if len(history) != 3 {
		t.Fatalf("rebuilt history must have 3 samples, got %d: %v", len(history), history)
	}
	// 游标持久化后再次回放不应重复投影。
	again, err := second.services.ReplayTelemetry(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if again.Skipped != 3 || again.Applied != 0 {
		t.Fatalf("persisted cursor replay must skip all, got %+v", again)
	}
}

func buildReplayServer(t *testing.T, data string) *Server {
	t.Helper()
	server := testServer(t)
	server.services.Journal = store.NewJournal(filepath.Join(data, "events.jsonl"))
	server.services.TelemetryCursors = store.NewTelemetryCursorStore(
		store.NewSnapshotStore(filepath.Join(data, "snapshots")), server.services.Clock.Now)
	return server
}
