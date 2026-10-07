package isledb

import (
	"context"
	"fmt"
	"sync"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// sstNoFragmentTransforms / sstNoReadEnv are the zero values
// NewRawRangeDelIter requires.
var (
	sstNoFragmentTransforms = sstable.NoFragmentTransforms
	sstNoReadEnv            = sstable.NoReadEnv
)

// rangeTombstoneCache stores decoded tombstones per SST. The cache is cleared
// on every manifest publish so it cannot retain entries for SSTs no longer
// referenced by the current view.
type rangeTombstoneCache struct {
	mu    sync.Mutex
	items map[string][]internal.RangeTombstone
}

func newRangeTombstoneCache() *rangeTombstoneCache {
	return &rangeTombstoneCache{items: make(map[string][]internal.RangeTombstone)}
}

func (c *rangeTombstoneCache) get(id string) ([]internal.RangeTombstone, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tombstones, ok := c.items[id]
	return tombstones, ok
}

func (c *rangeTombstoneCache) put(id string, tombstones []internal.RangeTombstone) {
	c.mu.Lock()
	c.items[id] = tombstones
	c.mu.Unlock()
}

func (c *rangeTombstoneCache) clear() {
	c.mu.Lock()
	clear(c.items)
	c.mu.Unlock()
}

// loadRangeTombstones returns every tombstone carried by meta, using the
// decoded cache when possible. It opens the SST only when its manifest
// RangeDeletions summary says it contains tombstones.
func (r *Reader) loadRangeTombstones(
	ctx context.Context,
	meta sstMetadata,
) ([]internal.RangeTombstone, error) {
	if meta.RangeDeletions.Count == 0 {
		return nil, nil
	}
	if tombstones, ok := r.rangeTombstoneCache.get(meta.ID); ok {
		return tombstones, nil
	}

	tombstones, err := r.readRangeTombstones(ctx, meta)
	if err != nil {
		return nil, err
	}
	r.rangeTombstoneCache.put(meta.ID, tombstones)
	return tombstones, nil
}

// readRangeTombstones opens the SST and reads its range-deletion block through
// Pebble's raw fragment iterator, collecting one RangeTombstone per record.
// The span returned per step may be reused by the next step, so start, end,
// and seq are copied immediately.
func (r *Reader) readRangeTombstones(
	ctx context.Context,
	meta sstMetadata,
) ([]internal.RangeTombstone, error) {
	reader, pointIter, err := r.openSSTIterBounded(ctx, meta, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("open sst %s for range tombstones: %w", meta.ID, err)
	}
	defer func() {
		_ = pointIter.Close() // closes the pebble reader too
	}()

	fragmentIter, err := reader.NewRawRangeDelIter(ctx, sstNoFragmentTransforms, sstNoReadEnv)
	if err != nil {
		return nil, fmt.Errorf("read range tombstone iterator %s: %w", meta.ID, err)
	}
	if fragmentIter == nil {
		return nil, nil
	}
	defer fragmentIter.Close()

	var tombstones []internal.RangeTombstone
	for span, spanErr := fragmentIter.First(); span != nil; span, spanErr = fragmentIter.Next() {
		if spanErr != nil {
			return nil, fmt.Errorf("scan range tombstones %s: %w", meta.ID, spanErr)
		}
		for _, key := range span.Keys {
			tombstones = append(tombstones, internal.RangeTombstone{
				Start: append([]byte(nil), span.Start...),
				End:   append([]byte(nil), span.End...),
				Seq:   uint64(key.SeqNum()),
			})
		}
	}
	return tombstones, nil
}
