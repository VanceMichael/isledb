package manifest

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrDBDestroyed is returned by every open, fence claim, publication, and
// refresh path once CURRENT has committed the irreversible destroyed state.
// It is a stable terminal error: callers use errors.Is rather than matching
// text, and a destroyed prefix can never return to active service.
var ErrDBDestroyed = errors.New("database destroyed")

// ensureActiveCurrent is the single gate for paths that must not run against a
// terminal CURRENT. It never inspects fences: destruction invalidates both
// roles independently of epoch comparison.
func ensureActiveCurrent(current *Current) error {
	if current != nil && current.Destroyed() {
		return ErrDBDestroyed
	}
	return nil
}

// invalidateLocalFences records terminal ownership loss in this process so
// that every later publication attempt fails before issuing a write.
func (s *Store) invalidateLocalFences() {
	s.mu.Lock()
	s.writerFence = nil
	s.compactorFence = nil
	s.destroyedObserved = true
	s.mu.Unlock()
}

// CommitDestruction performs the one irreversible terminal transition.
//
// The conditional update simultaneously sets LifecycleDestroyed and clears
// the writer/compactor fences, so the ownership invalidation and the terminal
// marker become visible atomically. Data deletion must start only after this
// returns successfully.
//
// The call is idempotent: if CURRENT is already terminal its record is
// returned unchanged. That re-read also recognizes a terminal commit whose
// original response was lost. A missing CURRENT fails closed with
// ErrCurrentUnavailable; callers distinguish an uninitialized prefix before
// invoking this method.
func (s *Store) CommitDestruction(ctx context.Context) (*Current, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	for attempt := 0; attempt < currentCASMaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current, etag, err := s.readCurrentWithETag(ctx)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, ErrCurrentUnavailable
		}
		if current.Destroyed() {
			return current.Clone(), nil
		}

		now := time.Now().UTC()
		updated := current.Clone()
		updated.Lifecycle = LifecycleDestroyed
		updated.WriterFence = nil
		updated.CompactorFence = nil
		updated.Destruction = &DestructionRecord{
			DestroyedAt:   now,
			PinnedViewAge: current.PinnedViewAge(),
		}
		if err := s.writeCurrentWithCAS(ctx, updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				if err := sleepBeforeCurrentCASRetry(ctx, attempt); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		s.invalidateLocalFences()
		return updated.Clone(), nil
	}
	return nil, ErrFenceConflict
}

// ReadDestructionState returns the terminal record when CURRENT is destroyed.
// It performs a fresh read so callers can recognize a terminal commit whose
// original response was lost. A nil record with nil error means the database
// is still active.
func (s *Store) ReadDestructionState(ctx context.Context) (*DestructionRecord, error) {
	current, err := s.readCurrentData(ctx)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, nil
	}
	return current.Destruction.Clone(), nil
}

// PersistDestructionProgress advances the durable sweep cursor stored in the
// terminal CURRENT. When swept is true the record is finalized with SweptAt.
// The method is idempotent:
//
//   - an already-swept CURRENT is returned unchanged;
//   - a cursor that does not advance is not written;
//   - precondition conflicts re-read the terminal CURRENT and retry, so two
//     destroyers (or a restarted process) converge on persisted progress.
func (s *Store) PersistDestructionProgress(
	ctx context.Context,
	cursor string,
	swept bool,
) (*Current, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	for attempt := 0; attempt < currentCASMaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current, etag, err := s.readCurrentWithETag(ctx)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, ErrCurrentUnavailable
		}
		if !current.Destroyed() || current.Destruction == nil {
			return nil, fmt.Errorf("%w: cannot persist sweep progress on active CURRENT", ErrInvalidManifest)
		}
		if current.Destruction.Swept {
			return current.Clone(), nil
		}

		updated := current.Clone()
		record := updated.Destruction.Clone()
		changed := false
		if swept {
			record.Swept = true
			if record.SweptAt.IsZero() {
				record.SweptAt = time.Now().UTC()
			}
			changed = true
		}
		// The cursor is a lexicographic lower bound: only advance it.
		if cursor > record.SweepCursor {
			record.SweepCursor = cursor
			changed = true
		}
		if !changed {
			return updated, nil
		}
		updated.Destruction = record
		if err := s.writeCurrentWithCAS(ctx, updated, etag); err != nil {
			if errors.Is(err, ErrPreconditionFailed) {
				if err := sleepBeforeCurrentCASRetry(ctx, attempt); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		return updated.Clone(), nil
	}
	return nil, ErrFenceConflict
}
