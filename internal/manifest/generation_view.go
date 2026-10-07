package manifest

import (
	"context"
	"errors"
	"fmt"
)

// GenerationGraph is the immutable object graph reachable from one pinned
// CURRENT generation. Online backup materializes it without reading CURRENT a
// second time: every value here is derived from the exact CURRENT bytes the
// caller captured, so concurrent writer publications cannot enlarge, shrink,
// or drift the graph.
type GenerationGraph struct {
	// Snapshot is the optional checkpoint referenced by the pinned generation.
	Snapshot *ObjectRef

	// Pages are every manifest page reachable from CURRENT.IndexFrontier,
	// including the frontier roots and all of their index and leaf children.
	Pages []PageRef

	// SSTs are the SST metadata records live in the pinned generation's
	// topology (L0 and every non-overlapping level).
	SSTs []SSTMeta

	// ChangeBatches are the committed mutation batches still required by the
	// pinned generation: one per retained add_sstable entry whose publication
	// carried a batch. Empty when the change feed is disabled.
	ChangeBatches []ChangeBatchMeta
}

// DescribeGeneration enumerates the complete immutable object graph required
// to independently restore the supplied CURRENT value. It never reads CURRENT:
// current is the single authoritative generation the caller pinned before
// calling, and all other objects are immutable and content-verified.
//
// The Store's incremental replay caches are deliberately left untouched, so
// describing an older pinned generation cannot poison later live replays.
func (s *Store) DescribeGeneration(ctx context.Context, current *Current) (*GenerationGraph, error) {
	if current == nil {
		return nil, fmt.Errorf("%w: nil CURRENT generation", ErrInvalidManifest)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	graph := &GenerationGraph{Snapshot: current.Snapshot.Clone()}

	state, err := s.replayPinnedGeneration(ctx, current)
	if err != nil {
		return nil, err
	}
	graph.SSTs = append(graph.SSTs, state.L0SSTs...)
	for i := range state.Levels {
		graph.SSTs = append(graph.SSTs, state.Levels[i].SSTs...)
	}

	pages, err := s.collectReachablePages(ctx, current)
	if err != nil {
		return nil, err
	}
	graph.Pages = pages

	if current.ChangeFeedEnabled {
		entries, err := s.entriesInRange(ctx, current, current.ChangeFeedLogStart, current.NextSeq)
		if err != nil {
			return nil, fmt.Errorf("enumerate change batches: %w", err)
		}
		for _, entry := range entries {
			if entry.ChangeBatch != nil {
				graph.ChangeBatches = append(graph.ChangeBatches, *entry.ChangeBatch)
			}
		}
	}
	return graph, nil
}

// collectReachablePages walks every IndexFrontier root and records each root
// and child exactly once. Every traversed object is checksum-verified, exactly
// as ordinary replay would verify it.
func (s *Store) collectReachablePages(ctx context.Context, current *Current) ([]PageRef, error) {
	pages, ok := s.storage.(PageStorage)
	if !ok {
		return nil, errors.New("manifest page storage unsupported")
	}
	if len(current.IndexFrontier) == 0 {
		return nil, nil
	}
	visited := make(map[string]struct{}, len(current.IndexFrontier))
	ordered := make([]PageRef, 0, len(current.IndexFrontier))
	var walk func(ref PageRef) error
	walk = func(ref PageRef) error {
		if err := validatePageRef(ref); err != nil {
			return err
		}
		if _, seen := visited[ref.Path]; seen {
			return nil
		}
		visited[ref.Path] = struct{}{}
		ordered = append(ordered, ref)
		if ref.Level == 0 {
			return nil
		}
		data, err := pages.ReadPage(ctx, ref.Path)
		if err != nil {
			return err
		}
		if err := verifyManifestObjectRef(data, ref.ObjectRef, manifestObjectKindPage); err != nil {
			return err
		}
		page, err := DecodeCommitPage(data)
		if err != nil {
			return err
		}
		if page.Level != ref.Level || page.SeqLo != ref.SeqLo || page.SeqHi != ref.SeqHi || page.Count != ref.Count {
			return fmt.Errorf("%w: manifest page ref mismatch path=%q", ErrInvalidManifest, ref.Path)
		}
		if err := validateCommitPage(page, ref.Path); err != nil {
			return err
		}
		for _, child := range page.Children {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	for _, root := range current.IndexFrontier {
		if err := walk(root); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

// replayPinnedGeneration rebuilds topology from an arbitrary pinned CURRENT
// value. It mirrors fullReplay's snapshot, coverage, and fence filtering
// guarantees but never mutates Store caches: the pinned generation is usually
// older than the live head.
func (s *Store) replayPinnedGeneration(ctx context.Context, current *Current) (*Manifest, error) {
	var m *Manifest
	if current.Snapshot != nil {
		if err := validateManifestObjectRef(*current.Snapshot, manifestObjectKindSnapshot); err != nil {
			return nil, err
		}
		data, err := s.storage.ReadSnapshot(ctx, current.Snapshot.Path)
		if err != nil {
			return nil, err
		}
		if err := verifyManifestObjectRef(data, *current.Snapshot, manifestObjectKindSnapshot); err != nil {
			return nil, err
		}
		snap, err := DecodeSnapshot(data)
		if err != nil {
			return nil, err
		}
		if current.LogSeqStart > 0 && snap.LogSeq != current.LogSeqStart-1 {
			return nil, fmt.Errorf("%w: snapshot log_seq=%d want=%d path=%q",
				ErrInvalidManifest, snap.LogSeq, current.LogSeqStart-1, current.Snapshot.Path)
		}
		m = snap
	} else {
		m = &Manifest{Version: 2, NextEpoch: 1}
	}

	entries, err := s.entriesInRange(ctx, current, currentLogStart(current), currentNextSeq(current))
	if err != nil {
		return nil, err
	}

	var maxWriterFenceClaimEpoch uint64
	var maxCompactorFenceClaimEpoch uint64
	var maxWriterEntryEpoch uint64
	var maxCompactorEntryEpoch uint64
	for _, entry := range entries {
		if entry.Op == LogOpFenceClaim {
			if entry.Role == FenceRoleWriter && entry.Epoch > maxWriterFenceClaimEpoch {
				maxWriterFenceClaimEpoch = entry.Epoch
			} else if entry.Role == FenceRoleCompactor && entry.Epoch > maxCompactorFenceClaimEpoch {
				maxCompactorFenceClaimEpoch = entry.Epoch
			}
		}
		if entry.Role == FenceRoleWriter && entry.Epoch > maxWriterEntryEpoch {
			maxWriterEntryEpoch = entry.Epoch
		} else if entry.Role == FenceRoleCompactor && entry.Epoch > maxCompactorEntryEpoch {
			maxCompactorEntryEpoch = entry.Epoch
		}
	}

	var activeWriterEpoch uint64
	var activeCompactorEpoch uint64
	if current.WriterFence != nil &&
		(maxWriterFenceClaimEpoch == 0 ||
			(maxWriterEntryEpoch >= current.WriterFence.Epoch && maxWriterFenceClaimEpoch < current.WriterFence.Epoch)) {
		activeWriterEpoch = current.WriterFence.Epoch
	}
	if current.CompactorFence != nil &&
		(maxCompactorFenceClaimEpoch == 0 ||
			(maxCompactorEntryEpoch >= current.CompactorFence.Epoch && maxCompactorFenceClaimEpoch < current.CompactorFence.Epoch)) {
		activeCompactorEpoch = current.CompactorFence.Epoch
	}

	for _, entry := range entries {
		if entry.Op == LogOpFenceClaim {
			if entry.Role == FenceRoleWriter && entry.Epoch > activeWriterEpoch {
				activeWriterEpoch = entry.Epoch
			} else if entry.Role == FenceRoleCompactor && entry.Epoch > activeCompactorEpoch {
				activeCompactorEpoch = entry.Epoch
			}
			continue
		}
		if entry.Role == FenceRoleWriter && entry.Epoch < activeWriterEpoch {
			continue
		}
		if entry.Role == FenceRoleCompactor && entry.Epoch < activeCompactorEpoch {
			continue
		}
		if err := validateReplayEntry(entry); err != nil {
			return nil, err
		}
		m, err = ApplyLogEntry(m, entry)
		if err != nil {
			return nil, err
		}
	}

	maxEpoch := activeWriterEpoch
	if activeCompactorEpoch > maxEpoch {
		maxEpoch = activeCompactorEpoch
	}
	if maxEpoch >= m.NextEpoch {
		m.NextEpoch = maxEpoch + 1
	}
	if current.NextEpoch > m.NextEpoch {
		m.NextEpoch = current.NextEpoch
	}
	if current.NextSeq > 0 {
		m.LogSeq = current.NextSeq - 1
	}
	if err := m.ValidateLevels(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidManifest, err)
	}
	return m, nil
}
