package isledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
)

var errInjectedDestroySweep = errors.New("injected destruction sweep fault")

func newDestroyTestBucket(t *testing.T) *blob.Bucket {
	t.Helper()
	return memblob.OpenBucket(nil)
}

func listPrefixKeys(t *testing.T, ctx context.Context, store *blobstore.Store) []string {
	t.Helper()
	result, err := store.List(ctx, blobstore.ListOptions{})
	if err != nil {
		t.Fatalf("list prefix: %v", err)
	}
	keys := make([]string, 0, len(result.Objects))
	for _, obj := range result.Objects {
		if obj.IsDir {
			continue
		}
		keys = append(keys, obj.Key)
	}
	return keys
}

func TestDestroy_FullLifecycleAndHandleConvergence(t *testing.T) {
	ctx := context.Background()
	bucket := newDestroyTestBucket(t)
	defer bucket.Close()

	const prefix = "tenants/full"
	const pinnedAge = 1500 * time.Millisecond
	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix: prefix,
		Policy: StorePolicy{MaxPinnedViewAge: pinnedAge},
	})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := writer.Put(ctx, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := writer.Put(ctx, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Put k2: %v", err)
	}

	readerOpts := DefaultReaderOpenOptions(t.TempDir())
	readerOpts.Views.RefreshAfter = time.Hour
	reader, err := db.OpenReader(ctx, readerOpts)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	if _, found, err := reader.Get(ctx, []byte("k1")); err != nil || !found {
		t.Fatalf("Get before destroy: found=%v err=%v", found, err)
	}
	snapshot, err := reader.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot before destroy: %v", err)
	}
	if got, found, err := snapshot.Get(ctx, []byte("k1")); err != nil || !found || string(got) != "v1" {
		t.Fatalf("snapshot Get before destroy: got=%q found=%v err=%v", got, found, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("snapshot close: %v", err)
	}

	// Operations takes the prefix down through an independent process-local
	// store, while the writer/reader processes keep their handles.
	opsStore := blobstore.New(bucket, "memory", prefix)
	first, err := destroyPrefix(ctx, opsStore)
	if err != nil {
		t.Fatalf("first destroy: %v", err)
	}
	if !first.Terminal || first.Swept {
		t.Fatalf("first destroy result=%+v, want terminal/not swept", first)
	}
	if first.RetryAfter <= 0 || first.RetryAfter > pinnedAge {
		t.Fatalf("RetryAfter=%s, want within (%s)", first.RetryAfter, pinnedAge)
	}

	// New opens against the same prefix fail with the stable terminal error.
	if _, err := OpenBucket(ctx, bucket, "memory", DBOptions{Prefix: prefix}); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("OpenBucket after destroy: %v", err)
	}
	destroyedDB, err := OpenBucket(ctx, bucket, "memory", DBOptions{Prefix: prefix})
	if err == nil {
		_ = destroyedDB.Close()
		t.Fatalf("expected OpenBucket to fail")
	}

	// The stale writer can no longer publish; CURRENT must not advance.
	stored := newManifestStore(opsStore, nil)
	currentBefore, err := stored.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if !currentBefore.Destroyed() || currentBefore.WriterFence != nil {
		t.Fatalf("terminal CURRENT not committed: destroyed=%v", currentBefore.Destroyed())
	}
	if err := writer.Flush(ctx); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("stale writer Flush: %v", err)
	}
	if err := writer.Close(ctx); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("stale writer Close: %v", err)
	}
	currentAfter, err := stored.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("reread current: %v", err)
	}
	if currentAfter.NextSeq != currentBefore.NextSeq {
		t.Fatalf("stale writer advanced CURRENT: %d -> %d", currentBefore.NextSeq, currentAfter.NextSeq)
	}

	// A replacement writer must not open against the existing DB handle.
	if _, err := db.OpenWriter(ctx, WriterOptions{}); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("OpenWriter after destroy: %v", err)
	}
	if _, err := db.OpenMaintenance(ctx, MaintenanceOptions{}); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("OpenMaintenance after destroy: %v", err)
	}
	if _, err := db.OpenChangeReader(ctx); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("OpenChangeReader after destroy: %v", err)
	}

	// Forced refresh fails with the terminal error but does not replace the
	// pinned view, which remains readable until its existing age boundary.
	if err := reader.Refresh(ctx); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("reader Refresh after destroy: %v", err)
	}
	if _, found, err := reader.Get(ctx, []byte("k1")); err != nil || !found {
		t.Fatalf("pinned read after failed refresh: found=%v err=%v", found, err)
	}

	// Once the pinned-view boundary passes, reads converge to the terminal
	// error and can never refresh back to an active state.
	time.Sleep(pinnedAge + 200*time.Millisecond)
	if _, _, err := reader.Get(ctx, []byte("k1")); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("reader Get after view expiry: %v", err)
	}
	if _, err := reader.Snapshot(ctx); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("reader Snapshot after view expiry: %v", err)
	}
	if _, err := reader.NewIterator(ctx, IteratorOptions{}); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("reader NewIterator after view expiry: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("reader close: %v", err)
	}

	// A replacement reader (slot now free) also fails with the stable error.
	if _, err := db.OpenReader(ctx, readerOpts); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("OpenReader after destroy: %v", err)
	}

	// A repeated, restarted call completes the sweep after the safety window.
	resumeStore := blobstore.New(bucket, "memory", prefix)
	second, err := destroyPrefix(ctx, resumeStore)
	if err != nil {
		t.Fatalf("second destroy: %v", err)
	}
	if !second.Terminal || !second.Swept {
		t.Fatalf("second destroy result=%+v, want terminal+swept", second)
	}
	keys := listPrefixKeys(t, ctx, resumeStore)
	if len(keys) != 1 || keys[0] != resumeStore.ManifestPath() {
		t.Fatalf("after sweep keys=%v, want only %q", keys, resumeStore.ManifestPath())
	}

	// Repeated destruction stays idempotent.
	third, err := destroyPrefix(ctx, blobstore.New(bucket, "memory", prefix))
	if err != nil {
		t.Fatalf("third destroy: %v", err)
	}
	if !third.Terminal || !third.Swept {
		t.Fatalf("idempotent destroy result=%+v", third)
	}
	if keys := listPrefixKeys(t, ctx, resumeStore); len(keys) != 1 {
		t.Fatalf("repeated destroy changed prefix: %v", keys)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}
}

func TestDestroy_StaleMaintenanceStops(t *testing.T) {
	ctx := context.Background()
	bucket := newDestroyTestBucket(t)
	defer bucket.Close()

	const prefix = "tenants/maintenance"
	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix: prefix,
		Policy: StorePolicy{MaxPinnedViewAge: time.Second},
	})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := writer.Put(ctx, []byte("mk"), []byte("mv")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("OpenMaintenance: %v", err)
	}

	if _, err := destroyPrefix(ctx, blobstore.New(bucket, "memory", prefix)); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := maintenance.RunOnce(ctx); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("stale maintenance RunOnce: %v", err)
	}
	if err := maintenance.Close(ctx); err != nil {
		t.Fatalf("maintenance close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}
}

func TestDestroy_ChangeReaderConverges(t *testing.T) {
	ctx := context.Background()
	bucket := newDestroyTestBucket(t)
	defer bucket.Close()

	const prefix = "tenants/changes"
	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix:     prefix,
		ChangeFeed: &ChangeFeedOptions{Payload: ChangeFeedKeysOnly},
		Policy:     StorePolicy{MaxPinnedViewAge: time.Second},
	})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	writer, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := writer.Put(ctx, []byte("ck"), []byte("cv")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	changeReader, err := db.OpenChangeReader(ctx)
	if err != nil {
		t.Fatalf("OpenChangeReader: %v", err)
	}
	page, err := changeReader.Read(ctx, ChangeCursor{}, DefaultChangeReadOptions())
	if err != nil {
		t.Fatalf("Read before destroy: %v", err)
	}
	if len(page.Changes) != 1 {
		t.Fatalf("changes=%d, want 1", len(page.Changes))
	}

	if _, err := destroyPrefix(ctx, blobstore.New(bucket, "memory", prefix)); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := changeReader.Read(ctx, page.Next, DefaultChangeReadOptions()); !errors.Is(err, ErrDatabaseDestroyed) {
		t.Fatalf("change read after destroy: %v", err)
	}
	if err := changeReader.Close(); err != nil {
		t.Fatalf("change reader close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}
}

func TestDestroy_RejectsUninitializedAndHeadlessPrefixes(t *testing.T) {
	ctx := context.Background()
	bucket := newDestroyTestBucket(t)
	defer bucket.Close()

	// Never-initialized prefix: no terminal marker must be synthesized.
	empty := blobstore.New(bucket, "memory", "tenants/empty")
	if _, err := destroyPrefix(ctx, empty); !errors.Is(err, ErrManifestUnavailable) {
		t.Fatalf("destroy empty prefix: %v", err)
	}
	if keys := listPrefixKeys(t, ctx, empty); len(keys) != 0 {
		t.Fatalf("empty prefix gained objects: %v", keys)
	}

	// Non-empty prefix whose CURRENT vanished: fail closed, do not destroy.
	const prefix = "tenants/headless"
	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{Prefix: prefix})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	w, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	if err := w.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}
	if err := bucket.Delete(ctx, prefix+"/manifest/CURRENT"); err != nil {
		t.Fatalf("delete CURRENT: %v", err)
	}
	headless := blobstore.New(bucket, "memory", prefix)
	if _, err := destroyPrefix(ctx, headless); !errors.Is(err, ErrManifestUnavailable) {
		t.Fatalf("destroy headless prefix: %v", err)
	}
	// Data objects must remain untouched.
	if keys := listPrefixKeys(t, ctx, headless); len(keys) == 0 {
		t.Fatalf("headless prefix objects were deleted")
	}
}

func TestDestroy_SweepResumesAfterPartialFailures(t *testing.T) {
	ctx := context.Background()
	bucket := newDestroyTestBucket(t)
	defer bucket.Close()

	const prefix = "tenants/faulty"
	const faultyPinnedAge = 50 * time.Millisecond
	db, err := OpenBucket(ctx, bucket, "memory", DBOptions{
		Prefix: prefix,
		Policy: StorePolicy{MaxPinnedViewAge: faultyPinnedAge},
	})
	if err != nil {
		t.Fatalf("OpenBucket: %v", err)
	}
	w, err := db.OpenWriter(ctx, WriterOptions{})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := w.Put(ctx, []byte(fmt.Sprintf("k%02d", i)), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := w.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatalf("writer close: %v", err)
	}
	// Seed control objects so the immediate phase has work beyond CURRENT.
	if err := bucket.WriteAll(ctx, prefix+"/maintenance/HEAD", []byte("{}"), nil); err != nil {
		t.Fatalf("seed head: %v", err)
	}
	if err := bucket.WriteAll(ctx, prefix+"/manifest/gc/sst/plans/p.json", []byte("{}"), nil); err != nil {
		t.Fatalf("seed gc plan: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("DB.Close: %v", err)
	}

	// The safety window starts at the terminal commit, so commit first and
	// wait for that window to elapse before the restarted sweep resumes.
	committer := newManifestStore(blobstore.New(bucket, "memory", prefix), nil)
	terminal, err := committer.CommitDestruction(ctx)
	if err != nil {
		t.Fatalf("commit destruction: %v", err)
	}
	time.Sleep(terminal.Destruction.NotBefore().Sub(time.Now()) + 10*time.Millisecond)

	// Fault 1: the first data-listing call fails before any batch completes.
	listFault := &destroyFaultStore{
		Store:         blobstore.New(bucket, "memory", prefix),
		failListCalls: 1,
	}
	ms := newManifestStore(listFault.Store, nil)
	if err := sweepDataObjects(ctx, listFault, ms, ""); !errors.Is(err, errInjectedDestroySweep) {
		t.Fatalf("listing fault: %v", err)
	}
	listFault.assertFired(t)

	// Progress must not have been persisted for the failed batch, and CURRENT
	// must remain terminal.
	record, err := ms.ReadDestructionState(ctx)
	if err != nil {
		t.Fatalf("read destruction: %v", err)
	}
	if record == nil || record.SweepCursor != "" || record.Swept {
		t.Fatalf("progress advanced after listing failure: %+v", record)
	}

	// Fault 2: the first batch delete partially succeeds then reports failure.
	deleteFault := &destroyFaultStore{
		Store:           blobstore.New(bucket, "memory", prefix),
		failDeleteCalls: 1,
		partialDelete:   true,
	}
	if err := sweepDataObjects(ctx, deleteFault, newManifestStore(deleteFault.Store, nil), ""); !errors.Is(err, errInjectedDestroySweep) {
		t.Fatalf("partial batch delete error=%v, want injected fault", err)
	}
	deleteFault.assertFired(t)

	// A restarted process resumes from persisted progress and repeated,
	// idempotent deletes; the prefix converges to just CURRENT.
	resume := blobstore.New(bucket, "memory", prefix)
	result, err := destroyPrefix(ctx, resume)
	if err != nil {
		t.Fatalf("resume destroy: %v", err)
	}
	if !result.Terminal || !result.Swept {
		t.Fatalf("resume result=%+v", result)
	}
	keys := listPrefixKeys(t, ctx, resume)
	if len(keys) != 1 || keys[0] != resume.ManifestPath() {
		t.Fatalf("after resume keys=%v", keys)
	}
}

// destroyFaultStore injects one bounded failure class into the sweep.
type destroyFaultStore struct {
	*blobstore.Store
	mu              sync.Mutex
	failListCalls   int
	failDeleteCalls int
	partialDelete   bool
	fired           bool
}

func (s *destroyFaultStore) markFired() {
	s.mu.Lock()
	s.fired = true
	s.mu.Unlock()
}

func (s *destroyFaultStore) assertFired(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fired {
		t.Fatalf("injected sweep fault did not fire")
	}
}

func (s *destroyFaultStore) ListPage(
	ctx context.Context,
	pageToken []byte,
	pageSize int,
	opts blobstore.ListOptions,
) (*blobstore.ListResult, []byte, error) {
	s.mu.Lock()
	fail := s.failListCalls > 0
	if fail {
		s.failListCalls--
	}
	s.mu.Unlock()
	if fail {
		s.markFired()
		return nil, nil, errInjectedDestroySweep
	}
	return s.Store.ListPage(ctx, pageToken, pageSize, opts)
}

func (s *destroyFaultStore) BatchDelete(ctx context.Context, keys []string) error {
	s.mu.Lock()
	fail := s.failDeleteCalls > 0
	if fail {
		s.failDeleteCalls--
	}
	s.mu.Unlock()
	if !fail {
		return s.Store.BatchDelete(ctx, keys)
	}
	s.markFired()
	if !s.partialDelete {
		return errInjectedDestroySweep
	}
	// Model a provider batch where some deletes landed and the response was a
	// failure. Resume must safely repeat the whole batch.
	keep := len(keys) / 2
	if keep == 0 {
		keep = 1
	}
	if err := s.Store.BatchDelete(ctx, keys[:keep]); err != nil {
		return err
	}
	return errInjectedDestroySweep
}
