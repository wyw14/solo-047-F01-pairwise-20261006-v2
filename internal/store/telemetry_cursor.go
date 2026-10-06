package store

import (
	"fmt"
	"time"
)

const telemetryCursorName = "telemetry-cursor"

// TelemetryCursorStore 用快照存储持久化遥测投影水位线，实现 telemetry.CursorStore。
type TelemetryCursorStore struct {
	snapshots *SnapshotStore
	now       func() time.Time
}

func NewTelemetryCursorStore(snapshots *SnapshotStore, now func() time.Time) *TelemetryCursorStore {
	if now == nil {
		now = time.Now
	}
	return &TelemetryCursorStore{snapshots: snapshots, now: now}
}

func (s *TelemetryCursorStore) LoadTelemetryCursor() (uint64, bool, error) {
	var cursor TelemetryCursor
	found, err := s.snapshots.Load(telemetryCursorName, &cursor)
	if err != nil || !found {
		return 0, found, err
	}
	return cursor.Offset, true, nil
}

func (s *TelemetryCursorStore) SaveTelemetryCursor(offset uint64) error {
	cursor := TelemetryCursor{Offset: offset, UpdatedAt: s.now().UTC()}
	if err := s.snapshots.Save(telemetryCursorName, cursor); err != nil {
		return fmt.Errorf("save telemetry cursor: %w", err)
	}
	return nil
}
