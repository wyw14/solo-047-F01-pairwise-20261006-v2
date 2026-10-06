package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/telemetry"
)

func (s *Server) ingestTelemetry(writer http.ResponseWriter, request *http.Request) {
	repeaterID := chi.URLParam(request, "id")
	if _, err := s.services.Repeater(repeaterID); err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input TelemetryRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	sample := telemetry.Sample{
		ID: s.services.IDs.New("sample"), AssetID: repeaterID, Kind: telemetry.Kind(input.Kind),
		Value: input.Value, Unit: input.Unit, Sequence: input.Sequence, Generation: input.Generation,
		ObservedAt: s.services.Clock.Now().UTC(),
	}
	if err := sample.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	// 先持久化再投影：日志是唯一事件源。崩溃时最坏只是从游标重复投影（sink 幂等），绝不丢样本。
	// 重复样本同样落日志，回放时按（资产、代次、序号）判重跳过，审计上也能看到重发尝试。
	if err := s.services.Persist("telemetry", sample.ID, sample); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	result, err := s.services.Telemetry.Apply(request.Context(), sample)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	status := http.StatusOK
	if result.Status == telemetry.StatusAccepted {
		status = http.StatusAccepted
	}
	writeJSON(writer, status, result)
}

// ReplayTelemetry 从持久化日志按全局游标回放全部遥测事件。偏移量取记录在
// 日志中的 1-based 行序号（日志只追加，序号稳定）；去重保证回放不漏不重。
// 游标是全局的，因此不做按资产过滤，避免交错事件被跳过。
func (s *Services) ReplayTelemetry(ctx context.Context) (telemetry.ReplayReport, error) {
	records, err := s.Journal.Records()
	if err != nil {
		return telemetry.ReplayReport{}, err
	}
	events := make([]telemetry.ReplayEvent, 0, len(records))
	for index, record := range records {
		if record.Kind != "telemetry" {
			continue
		}
		var sample telemetry.Sample
		if err := json.Unmarshal(record.Payload, &sample); err != nil {
			return telemetry.ReplayReport{}, err
		}
		events = append(events, telemetry.ReplayEvent{Offset: uint64(index + 1), Sample: sample})
	}
	return s.Telemetry.ReplayFrom(ctx, events, s.TelemetryCursors)
}

func (s *Server) replayTelemetry(writer http.ResponseWriter, request *http.Request) {
	report, err := s.services.ReplayTelemetry(request.Context())
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusOK, report)
}

func (s *Server) getRepeater(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
