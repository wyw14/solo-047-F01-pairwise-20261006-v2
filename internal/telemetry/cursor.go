package telemetry

import "sort"

// streamKey identifies one ordered sample log: an asset within a generation.
type streamKey struct {
	AssetID    string
	Generation uint64
}

// Cursor records, for each (asset, generation) stream, the highest confirmed
// sequence that has already been delivered to a consumer. It is the replay
// boundary: replaying from a cursor yields exactly the confirmed samples past
// each watermark, so restarting from a persisted cursor neither loses samples
// nor delivers them twice.
type Cursor struct {
	watermarks map[streamKey]uint64
}

func NewCursor() *Cursor {
	return &Cursor{watermarks: make(map[streamKey]uint64)}
}

// Position returns the delivered confirmed sequence for a stream.
func (c *Cursor) Position(assetID string, generation uint64) uint64 {
	return c.watermarks[streamKey{assetID, generation}]
}

// Advance raises the watermark for a stream; it never moves backwards.
func (c *Cursor) Advance(assetID string, generation, sequence uint64) {
	key := streamKey{assetID, generation}
	if sequence > c.watermarks[key] {
		c.watermarks[key] = sequence
	}
}

// Merge raises every watermark to the larger of the two cursors.
func (c *Cursor) Merge(other *Cursor) {
	if other == nil {
		return
	}
	for key, sequence := range other.watermarks {
		c.Advance(key.AssetID, key.Generation, sequence)
	}
}

// Clone returns an independent copy.
func (c *Cursor) Clone() *Cursor {
	clone := NewCursor()
	clone.Merge(c)
	return clone
}

// keys lists every stream held in the repository in deterministic order.
func (r *Repository) keys() []streamKey {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]streamKey, 0, len(r.streams))
	for assetID, generations := range r.streams {
		for generation := range generations {
			keys = append(keys, streamKey{AssetID: assetID, Generation: generation})
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].AssetID != keys[j].AssetID {
			return keys[i].AssetID < keys[j].AssetID
		}
		return keys[i].Generation < keys[j].Generation
	})
	return keys
}

// ConfirmedSince returns every confirmed sample strictly past the cursor's
// watermark for its stream, ordered by asset, generation and sequence.
func (r *Repository) ConfirmedSince(cursor *Cursor) []Sample {
	var result []Sample
	for _, key := range r.keys() {
		watermark := uint64(0)
		if cursor != nil {
			watermark = cursor.Position(key.AssetID, key.Generation)
		}
		for _, sample := range r.ConfirmedSamples(key.AssetID, key.Generation) {
			if sample.Sequence > watermark {
				result = append(result, sample)
			}
		}
	}
	return result
}
