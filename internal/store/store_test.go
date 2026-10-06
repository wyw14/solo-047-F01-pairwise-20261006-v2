package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotAndJournalRoundTrip(t *testing.T) {
	root := t.TempDir()
	snapshots := NewSnapshotStore(filepath.Join(root, "snapshots"))
	want := RampCheckpoint{RepeaterID: "R07", StableStep: 2, UpdatedAt: time.Now().UTC().Truncate(time.Second)}
	if err := snapshots.Save("ramp-R07", want); err != nil {
		t.Fatal(err)
	}
	var got RampCheckpoint
	found, err := snapshots.Load("ramp-R07", &got)
	if err != nil || !found {
		t.Fatalf("load snapshot: found=%v err=%v", found, err)
	}
	if got.RepeaterID != want.RepeaterID || got.StableStep != want.StableStep {
		t.Fatalf("snapshot mismatch: got=%+v want=%+v", got, want)
	}

	journal := NewJournal(filepath.Join(root, "events.jsonl"))
	if err := journal.Append("ramp", "R07", got, want.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	records, err := journal.Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Kind != "ramp" || records[0].EntityID != "R07" {
		t.Fatalf("unexpected journal records: %+v", records)
	}
}

func TestTelemetryCursorRoundTrip(t *testing.T) {
	root := t.TempDir()
	cursors := NewTelemetryCursorStore(NewSnapshotStore(filepath.Join(root, "snapshots")), func() time.Time {
		return time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	})
	if _, found, err := cursors.LoadTelemetryCursor(); err != nil || found {
		t.Fatalf("fresh cursor should be absent, found=%v err=%v", found, err)
	}
	if err := cursors.SaveTelemetryCursor(42); err != nil {
		t.Fatal(err)
	}
	offset, found, err := cursors.LoadTelemetryCursor()
	if err != nil || !found || offset != 42 {
		t.Fatalf("cursor round trip mismatch: offset=%d found=%v err=%v", offset, found, err)
	}
}
