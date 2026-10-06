package api

import (
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
	result, err := s.services.Telemetry.Apply(request.Context(), sample)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	// Duplicates and buffered out-of-order arrivals are acknowledged, not
	// rejected. Only a genuinely novel sample is appended to the journal, so a
	// replay of it can never re-record the same (asset, generation, sequence).
	if result.Outcome != telemetry.OutcomeDuplicate {
		if err := s.services.Persist("telemetry", sample.ID, sample); err != nil {
			writeError(writer, http.StatusInternalServerError, err)
			return
		}
	}
	status := http.StatusAccepted
	if result.Outcome == telemetry.OutcomeDuplicate {
		status = http.StatusOK
	}
	writeJSON(writer, status, result)
}

func (s *Server) getRepeater(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
