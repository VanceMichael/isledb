package manifest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

func newDestructionTestStore(t *testing.T, prefix string) (*blobstore.Store, *Store) {
	t.Helper()
	store := blobstore.NewMemory(prefix)
	return store, NewStore(store)
}

func claimWriterForDestructionTest(t *testing.T, ctx context.Context, ms *Store) *FenceToken {
	t.Helper()
	token, err := ms.ClaimWriterWithPolicy(ctx, "writer-owner", time.Minute)
	if err != nil {
		t.Fatalf("claim writer: %v", err)
	}
	return token
}

func TestCommitDestruction_TerminalTransitionAndIdempotence(t *testing.T) {
	ctx := context.Background()
	store, ms := newDestructionTestStore(t, "tenant/terminal")
	claimWriterForDestructionTest(t, ctx, ms)

	before := ms.CurrentData()
	if before == nil || before.Destroyed() {
		t.Fatalf("expected active CURRENT before destruction")
	}

	terminal, err := ms.CommitDestruction(ctx)
	if err != nil {
		t.Fatalf("commit destruction: %v", err)
	}
	if !terminal.Destroyed() || terminal.Destruction == nil {
		t.Fatalf("commit did not return terminal CURRENT: %+v", terminal)
	}
	if terminal.WriterFence != nil || terminal.CompactorFence != nil {
		t.Fatalf("fences must be invalidated atomically: writer=%v compactor=%v",
			terminal.WriterFence, terminal.CompactorFence)
	}
	if terminal.Destruction.DestroyedAt.IsZero() {
		t.Fatalf("destroyed_at not recorded")
	}
	if terminal.Destruction.PinnedViewAge != time.Minute {
		t.Fatalf("pinned view age=%s, want %s", terminal.Destruction.PinnedViewAge, time.Minute)
	}
	if terminal.Destruction.Swept || !terminal.Destruction.SweptAt.IsZero() {
		t.Fatalf("fresh terminal record must not be swept")
	}
	if !terminal.Destruction.NotBefore().Equal(terminal.Destruction.DestroyedAt.Add(time.Minute)) {
		t.Fatalf("unexpected not-before: %s", terminal.Destruction.NotBefore())
	}

	// The committed object must decode through the strict CURRENT path.
	raw, _, err := store.Read(ctx, store.ManifestPath())
	if err != nil {
		t.Fatalf("read CURRENT object: %v", err)
	}
	decoded, err := DecodeCurrent(raw)
	if err != nil {
		t.Fatalf("terminal CURRENT must decode: %v", err)
	}
	if !decoded.Destroyed() {
		t.Fatalf("decoded CURRENT is not terminal")
	}

	// Idempotent repeat through a fresh process-local store.
	other := NewStore(store)
	again, err := other.CommitDestruction(ctx)
	if err != nil {
		t.Fatalf("repeat commit destruction: %v", err)
	}
	if !again.Destroyed() || again.Destruction.DestroyedAt != terminal.Destruction.DestroyedAt {
		t.Fatalf("repeat commit must adopt the existing terminal record")
	}
}

func TestCommitDestruction_MissingCurrentFailsClosed(t *testing.T) {
	ctx := context.Background()
	_, ms := newDestructionTestStore(t, "tenant/missing")
	_, err := ms.CommitDestruction(ctx)
	if !errors.Is(err, ErrCurrentUnavailable) {
		t.Fatalf("missing CURRENT commit: %v, want ErrCurrentUnavailable", err)
	}
}

func TestCommitDestruction_LostResponseRecognizedOnReread(t *testing.T) {
	ctx := context.Background()
	store, ms := newDestructionTestStore(t, "tenant/lost-response")
	claimWriterForDestructionTest(t, ctx, ms)

	fault := &destructionFaultStorage{BlobStoreBackend: NewBlobStoreBackend(store)}
	fault.arm(destructionFaultWriteAfter)
	faulty := NewStoreWithStorage(fault)

	if _, err := faulty.CommitDestruction(ctx); err == nil {
		t.Fatalf("expected injected response-loss error")
	}
	fault.assertFired(t)

	// The terminal CAS landed; the store must not rewrite it, and a fresh
	// store recognizes the committed terminal state.
	current, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("reread CURRENT: %v", err)
	}
	if !current.Destroyed() {
		t.Fatalf("terminal commit was not recognized after response loss")
	}
	other := NewStore(store)
	terminal, err := other.CommitDestruction(ctx)
	if err != nil {
		t.Fatalf("recognize terminal after loss: %v", err)
	}
	if !terminal.Destroyed() {
		t.Fatalf("expected terminal CURRENT")
	}
}

func TestDestruction_StaleOwnersCannotPublish(t *testing.T) {
	ctx := context.Background()
	store, ms := newDestructionTestStore(t, "tenant/stale")

	staleWriter := NewStore(store)
	writerToken, err := staleWriter.ClaimWriterWithPolicy(ctx, "stale-writer", time.Minute)
	if err != nil {
		t.Fatalf("claim stale writer: %v", err)
	}
	staleCompactor := NewStore(store)
	compactorToken, err := staleCompactor.ClaimCompactor(ctx, "stale-compactor")
	if err != nil {
		t.Fatalf("claim stale compactor: %v", err)
	}
	staleMaintenance := NewStore(store)
	maintenanceToken, err := staleMaintenance.ClaimMaintenance(ctx, "stale-maintenance")
	if err != nil {
		t.Fatalf("claim stale maintenance: %v", err)
	}
	// A command already waiting in the mailbox when destruction lands must not
	// be applicable by the stale writer afterward.
	pendingHead := &MaintenanceHead{
		LayoutVersion: MaintenanceHeadLayoutVersion,
		Epoch:         maintenanceToken.Epoch,
		OwnerID:       maintenanceToken.Owner,
		ClaimedAt:     maintenanceToken.ClaimedAt,
		Generation:    2,
		Pending: &MaintenanceCommand{
			ID:         "cmd-pending",
			Epoch:      maintenanceToken.Epoch,
			Generation: 2,
			Kind:       MaintenanceCommandChangeFeedFloor,
			CreatedAt:  time.Now().UTC(),
			ChangeFeedFloor: &AdvanceFloorCommand{},
		},
	}
	headBytes, err := EncodeMaintenanceHead(pendingHead)
	if err != nil {
		t.Fatalf("encode head: %v", err)
	}
	backend := NewBlobStoreBackend(store)
	_, headEtag, err := backend.ReadMaintenanceHead(ctx)
	if err != nil {
		t.Fatalf("read seeded head: %v", err)
	}
	if _, err := backend.WriteMaintenanceHeadCAS(ctx, headBytes, headEtag); err != nil {
		t.Fatalf("seed pending head: %v", err)
	}

	terminal, err := ms.CommitDestruction(ctx)
	if err != nil {
		t.Fatalf("commit destruction: %v", err)
	}
	committedNextSeq := terminal.NextSeq

	appendEntry := func() *ManifestLogEntry {
		return &ManifestLogEntry{
			Op:      LogOpAddSSTable,
			SSTable: &SSTMeta{ID: "stale.sst", Epoch: writerToken.Epoch, Level: 0, CreatedAt: time.Now().UTC()},
		}
	}

	if err := staleWriter.AppendWithWriterFence(ctx, appendEntry()); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("stale writer append: %v, want ErrDBDestroyed", err)
	}
	if err := staleWriter.CheckWriterFence(ctx); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("stale writer fence check: %v", err)
	}
	if err := staleWriter.EnableChangeFeed(ctx, ChangeFeedPayloadKeysOnly); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("enable change feed after destroy: %v", err)
	}
	if err := staleCompactor.AppendWithCompactorFence(ctx, &ManifestLogEntry{
		Op:               LogOpRemoveSSTable,
		RemoveSSTableIDs: []string{"gone.sst"},
		RetiredObjects: []RetiredObject{{
			Kind: RetiredObjectSST,
			ID:   "gone.sst",
			Key:  "tenant/stale/sstable/fff/gone.sst",
			Size: 1,
		}},
	}); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("stale compactor append: %v, want ErrDBDestroyed", err)
	}
	if err := staleCompactor.CheckCompactorFenceToken(ctx, compactorToken); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("stale compactor token check: %v", err)
	}
	if _, err := staleCompactor.ClaimCompactor(ctx, "new-compactor"); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("compactor re-claim: %v", err)
	}
	if _, err := staleWriter.ClaimWriterWithPolicy(ctx, "new-writer", time.Minute); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("writer re-claim: %v", err)
	}
	if _, err := staleMaintenance.StageMaintenance(ctx, MaintenanceCommand{}, maintenanceToken); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("stale maintenance stage: %v", err)
	}
	if _, err := staleMaintenance.ClearMaintenance(ctx, "cmd", 1, 1, maintenanceToken); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("stale maintenance clear: %v", err)
	}
	if _, err := staleMaintenance.ClaimMaintenance(ctx, "new-maintenance"); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("maintenance re-claim: %v", err)
	}
	if _, err := staleWriter.AdvanceChangeFeedLogStart(ctx, 1, writerToken); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("advance feed floor: %v", err)
	}
	if _, err := staleWriter.PrepareCheckpoint(ctx); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("prepare checkpoint: %v", err)
	}
	if _, err := staleWriter.ApplyPendingMaintenance(ctx); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("apply pending maintenance: %v", err)
	}

	// CURRENT content must be untouched by every rejected publication.
	after, err := ms.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("reread CURRENT: %v", err)
	}
	if !after.Destroyed() || after.NextSeq != committedNextSeq {
		t.Fatalf("CURRENT changed after rejected publications: destroyed=%v nextSeq=%d want=%d",
			after.Destroyed(), after.NextSeq, committedNextSeq)
	}
}

func TestDestruction_ReadAndRefreshPathsFailStable(t *testing.T) {
	ctx := context.Background()
	_, ms := newDestructionTestStore(t, "tenant/reads")
	claimWriterForDestructionTest(t, ctx, ms)
	if _, err := ms.CommitDestruction(ctx); err != nil {
		t.Fatalf("commit destruction: %v", err)
	}

	fresh := NewStoreWithStorage(ms.storage)
	if _, err := fresh.Replay(ctx); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("Replay: %v", err)
	}
	if _, err := fresh.ReplayWithArtifactValidation(ctx); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("ReplayWithArtifactValidation: %v", err)
	}
	if _, _, err := fresh.ReplayWithCurrent(ctx); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("ReplayWithCurrent: %v", err)
	}
	if _, err := fresh.LoadChangeFeedView(ctx); !errors.Is(err, ErrDBDestroyed) {
		t.Fatalf("LoadChangeFeedView: %v", err)
	}
	state, err := fresh.ReadDestructionState(ctx)
	if err != nil {
		t.Fatalf("ReadDestructionState: %v", err)
	}
	if state == nil || state.DestroyedAt.IsZero() {
		t.Fatalf("destruction state not readable: %+v", state)
	}
}

func TestPersistDestructionProgress_CursorAndSwept(t *testing.T) {
	ctx := context.Background()
	_, ms := newDestructionTestStore(t, "tenant/progress")
	claimWriterForDestructionTest(t, ctx, ms)
	current, err := ms.CommitDestruction(ctx)
	if err != nil {
		t.Fatalf("commit destruction: %v", err)
	}
	if current.Destruction.Swept {
		t.Fatalf("unexpected swept")
	}

	updated, err := ms.PersistDestructionProgress(ctx, "tenant/progress/sstable/00a/key-2", false)
	if err != nil {
		t.Fatalf("persist cursor: %v", err)
	}
	if updated.Destruction.SweepCursor != "tenant/progress/sstable/00a/key-2" || updated.Destruction.Swept {
		t.Fatalf("unexpected progress record: %+v", updated.Destruction)
	}

	// The cursor only advances.
	updated, err = ms.PersistDestructionProgress(ctx, "tenant/progress/sstable/00a/key-1", false)
	if err != nil {
		t.Fatalf("persist older cursor: %v", err)
	}
	if updated.Destruction.SweepCursor != "tenant/progress/sstable/00a/key-2" {
		t.Fatalf("cursor regressed to %q", updated.Destruction.SweepCursor)
	}

	// A restarted process observes durable progress and finalizes.
	other := NewStoreWithStorage(ms.storage)
	record, err := other.ReadDestructionState(ctx)
	if err != nil {
		t.Fatalf("read progress after restart: %v", err)
	}
	if record.SweepCursor != "tenant/progress/sstable/00a/key-2" {
		t.Fatalf("progress not durable: %q", record.SweepCursor)
	}
	final, err := other.PersistDestructionProgress(ctx, "zzz", true)
	if err != nil {
		t.Fatalf("mark swept: %v", err)
	}
	if !final.Destruction.Swept || final.Destruction.SweptAt.IsZero() {
		t.Fatalf("swept not finalized: %+v", final.Destruction)
	}

	// Further progress calls are no-ops once swept.
	again, err := other.PersistDestructionProgress(ctx, "", false)
	if err != nil {
		t.Fatalf("post-swept progress: %v", err)
	}
	if !again.Destruction.Swept || again.Destruction.SweptAt != final.Destruction.SweptAt {
		t.Fatalf("swept state mutated: %+v", again.Destruction)
	}
}

func TestValidateLifecycle(t *testing.T) {
	now := time.Now().UTC()
	valid := &Current{
		LayoutVersion:    LayoutVersion,
		Format:           CurrentFormat,
		NextEpoch:        1,
		MaxPinnedViewAge: time.Minute,
		Lifecycle:        LifecycleDestroyed,
		Destruction: &DestructionRecord{
			DestroyedAt:   now,
			PinnedViewAge: time.Minute,
		},
	}
	if _, err := EncodeCurrent(valid); err != nil {
		t.Fatalf("valid terminal CURRENT rejected: %v", err)
	}

	cases := []struct {
		name    string
		mutate  func(*Current)
		wantErr bool
	}{
		{"unknown lifecycle", func(c *Current) { c.Lifecycle = "archived" }, true},
		{"terminal without record", func(c *Current) { c.Destruction = nil }, true},
		{"zero destroyed_at", func(c *Current) { c.Destruction.DestroyedAt = time.Time{} }, true},
		{"zero pinned age", func(c *Current) { c.Destruction.PinnedViewAge = 0 }, true},
		{"swept without swept_at", func(c *Current) {
			c.Destruction.Swept = true
		}, true},
		{"active with record", func(c *Current) {
			c.Lifecycle = LifecycleActive
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := valid.Clone()
			tc.mutate(c)
			err := validateLifecycle(c)
			if tc.wantErr && err == nil {
				t.Fatalf("expected validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

// destructionFaultStorage fails one armed CURRENT write after the provider
// mutation succeeded, modeling a lost terminal-commit response.
type destructionFaultPoint string

const destructionFaultWriteAfter destructionFaultPoint = "write_current_after"

type destructionFaultStorage struct {
	*BlobStoreBackend
	mu    sync.Mutex
	armed destructionFaultPoint
	fired bool
}

func (s *destructionFaultStorage) arm(point destructionFaultPoint) {
	s.mu.Lock()
	s.armed = point
	s.fired = false
	s.mu.Unlock()
}

func (s *destructionFaultStorage) fail() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fired || s.armed != destructionFaultWriteAfter {
		return false
	}
	s.fired = true
	return true
}

func (s *destructionFaultStorage) assertFired(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fired {
		t.Fatalf("armed fault did not fire")
	}
}

func (s *destructionFaultStorage) WriteCurrentCAS(ctx context.Context, data []byte, expectedETag string) (string, error) {
	etag, err := s.BlobStoreBackend.WriteCurrentCAS(ctx, data, expectedETag)
	if err != nil {
		return "", err
	}
	if s.fail() {
		return "", fmt.Errorf("injected lost terminal commit response")
	}
	return etag, nil
}
