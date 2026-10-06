package store

import (
	"fmt"
	"time"
)

type RampCheckpoint struct {
	RepeaterID string    `json:"repeater_id"`
	StableStep int       `json:"stable_step"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type HeartbeatSnapshot struct {
	StationID string    `json:"station_id"`
	SeenAt    time.Time `json:"seen_at"`
	Sequence  uint64    `json:"sequence"`
}

// TelemetryCursor 是遥测投影的水位线：Offset 之前（含）的事件都已成功投影。
type TelemetryCursor struct {
	Offset    uint64    `json:"offset"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Checkpoints struct {
	snapshots *SnapshotStore
}

func NewCheckpoints(snapshots *SnapshotStore) *Checkpoints {
	return &Checkpoints{snapshots: snapshots}
}

func (c *Checkpoints) SaveRamp(value RampCheckpoint) error {
	return c.snapshots.Save(c.Key("ramp", value.RepeaterID), value)
}

func (c *Checkpoints) LoadRamp(repeaterID string) (RampCheckpoint, bool, error) {
	var value RampCheckpoint
	found, err := c.snapshots.Load(c.Key("ramp", repeaterID), &value)
	return value, found, err
}

func (c *Checkpoints) SaveHeartbeat(value HeartbeatSnapshot) error {
	return c.snapshots.Save(c.Key("heartbeat", value.StationID), value)
}

func (c *Checkpoints) LoadHeartbeat(stationID string) (HeartbeatSnapshot, bool, error) {
	var value HeartbeatSnapshot
	found, err := c.snapshots.Load(c.Key("heartbeat", stationID), &value)
	return value, found, err
}

func (c *Checkpoints) Key(kind, id string) string {
	return fmt.Sprintf("%s-%s", kind, id)
}
