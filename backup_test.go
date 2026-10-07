package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

func drainBackupObjects(t *testing.T, ctx context.Context, backup *Backup, pageSize int) []BackupObject {
	t.Helper()
	var all []BackupObject
	var cursor *BackupListCursor
	for {
		page, next, err := backup.ListObjects(ctx, pageSize, cursor)
		if err != nil {
			t.Fatalf("list backup objects at %+v: %v", cursor, err)
		}
		all = append(all, page...)
		if next == nil {
			return all
		}
		cursor = next
	}
}

func openBackupTestDB(t *testing.T, ctx context.Context, prefix string, opts dbOpenOptions) (*blobstore.Store, *DB) {
	t.Helper()
	store := blobstore.NewMemory(prefix)
	db, err := openDB(ctx, store, opts)
	if err != nil {
		store.Close()
		t.Fatalf("open DB: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_ = store.Close()
	})
	return store, db
}

func backupTestWriter(t *testing.T, ctx context.Context, db *DB, owner string) *Writer {
	t.Helper()
	opts := DefaultWriterOptions()
	opts.OwnerID = owner
	opts.Flush.Interval = 0
	writer, err := db.OpenWriter(ctx, opts)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() { _ = writer.Close(context.Background()) })
	return writer
}

func verifyBackupObject(t *testing.T, ctx context.Context, store *blobstore.Store, object BackupObject) {
	t.Helper()
	data, _, err := store.Read(ctx, object.Path)
	if err != nil {
		t.Fatalf("read backup object %q: %v", object.Path, err)
	}
	switch object.Kind {
	case BackupObjectSST, BackupObjectChangeBatch:
		// Manifest size/checksum cover the immutable payload prefix; the stored
		// object additionally carries bloom/index trailer regions.
		if object.Size <= 0 || int64(len(data)) < object.Size {
			t.Fatalf("object %q bytes=%d want at least=%d", object.Path, len(data), object.Size)
		}
		sum := sha256.Sum256(data[:object.Size])
		if want := "sha256:" + hex.EncodeToString(sum[:]); want != object.Checksum {
			t.Fatalf("object %q payload checksum=%s want=%s", object.Path, want, object.Checksum)
		}
	case BackupObjectManifestSnapshot, BackupObjectManifestPage, BackupObjectCurrent:
		if object.EncodedBytes == 0 || uint64(len(data)) != object.EncodedBytes {
			t.Fatalf("object %q encoded bytes=%d want=%d", object.Path, len(data), object.EncodedBytes)
		}
		sum := sha256.Sum256(data)
		if want := "sha256:" + hex.EncodeToString(sum[:]); want != object.Checksum {
			t.Fatalf("object %q checksum=%s want=%s", object.Path, want, object.Checksum)
		}
	default:
		t.Fatalf("unknown backup object kind %q", object.Kind)
	}
}

func TestOnlineBackupStableManifestAcrossWritesAndRestart(t *testing.T) {
	ctx := context.Background()
	store, db := openBackupTestDB(t, ctx, "backup-stable", dbOpenOptions{})
	writer := backupTestWriter(t, ctx, db, "backup-stable-writer")
	for _, key := range []string{"k1", "k2", "k3"} {
		if err := writer.Put(ctx, []byte(key), []byte("v-"+key)); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	liveAtBegin, err := db.manifestStore.Replay(ctx)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	wantSSTs := map[string]manifest.SSTMeta{}
	for _, sst := range liveAtBegin.AllSSTIDs() {
		meta := liveAtBegin.LookupSST(sst)
		if meta == nil {
			t.Fatalf("missing sst meta %q", sst)
		}
		wantSSTs[store.SSTPath(sst)] = *meta
	}

	backup, err := db.BeginBackup(ctx, BackupOptions{Owner: "test-agent", LeaseTTL: time.Hour})
	if err != nil {
		t.Fatalf("begin backup: %v", err)
	}
	token, err := backup.Token()
	if err != nil {
		t.Fatalf("token: %v", err)
	}
	info := backup.Info()
	if info.ViewID == "" || info.EntryCount != 1+len(wantSSTs) {
		t.Fatalf("backup info=%+v ssts=%d", info, len(wantSSTs))
	}

	first := drainBackupObjects(t, ctx, backup, 1)
	if len(first) != info.EntryCount {
		t.Fatalf("listed=%d want=%d", len(first), info.EntryCount)
	}
	if first[0].Kind != BackupObjectCurrent || first[0].LogicalPath != store.ManifestPath() {
		t.Fatalf("first row=%+v", first[0])
	}
	verifyBackupObject(t, ctx, store, first[0])
	if first[0].Checksum != "sha256:"+info.ViewID {
		t.Fatalf("current checksum=%q view=%q", first[0].Checksum, info.ViewID)
	}
	gotSSTs := map[string]BackupObject{}
	for _, object := range first[1:] {
		if object.Kind != BackupObjectSST {
			t.Fatalf("unexpected object kind=%q in no-feed backup", object.Kind)
		}
		verifyBackupObject(t, ctx, store, object)
		meta, ok := wantSSTs[object.Path]
		if !ok {
			t.Fatalf("unexpected SST in manifest %q", object.Path)
		}
		if object.Size != meta.Size || object.Checksum != meta.Checksum {
			t.Fatalf("SST metadata mismatch path=%q listed=%+v want size=%d checksum=%s",
				object.Path, object, meta.Size, meta.Checksum)
		}
		gotSSTs[object.Path] = object
	}
	if len(gotSSTs) != len(wantSSTs) {
		t.Fatalf("listed ssts=%d want=%d", len(gotSSTs), len(wantSSTs))
	}

	// Later writes advance the live generation; the pinned manifest must not
	// change.
	for _, key := range []string{"k4", "k5", "k6"} {
		if err := writer.Put(ctx, []byte(key), []byte("v-"+key)); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	again := drainBackupObjects(t, ctx, backup, 2)
	if len(again) != len(first) {
		t.Fatalf("manifest drifted after writes: %d want=%d", len(again), len(first))
	}
	for i := range again {
		if again[i] != first[i] {
			t.Fatalf("row %d drifted: %+v want %+v", i, again[i], first[i])
		}
	}

	liveCurrent, _, err := db.manifestStore.Storage().ReadCurrent(ctx)
	if err != nil {
		t.Fatalf("read live current: %v", err)
	}
	liveSum := sha256.Sum256(liveCurrent)
	if hex.EncodeToString(liveSum[:]) == info.ViewID {
		t.Fatalf("pinned generation unexpectedly equals the post-write live generation")
	}

	// Simulate process restart: reopen the prefix and resume with only the
	// persisted token. The resumed view must be identical.
	tokenText := token.String()
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	reopened, err := openDB(ctx, store, dbOpenOptions{})
	if err != nil {
		t.Fatalf("reopen DB: %v", err)
	}
	resumeToken, err := DecodeBackupToken(tokenText)
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	resumed, err := reopened.OpenBackup(ctx, resumeToken)
	if err != nil {
		t.Fatalf("resume backup after restart: %v", err)
	}
	rowsAfterRestart := drainBackupObjects(t, ctx, resumed, 3)
	if len(rowsAfterRestart) != len(first) {
		t.Fatalf("resumed manifest rows=%d want=%d", len(rowsAfterRestart), len(first))
	}
	for i := range rowsAfterRestart {
		if rowsAfterRestart[i] != first[i] {
			t.Fatalf("resumed row %d drifted: %+v want %+v", i, rowsAfterRestart[i], first[i])
		}
	}
	if resumed.Info().ViewID != info.ViewID || resumed.Info().EntryCount != info.EntryCount {
		t.Fatalf("resumed info=%+v want view=%s count=%d", resumed.Info(), info.ViewID, info.EntryCount)
	}
}

func TestOnlineBackupIncludesChangeBatchesSnapshotAndPages(t *testing.T) {
	ctx := context.Background()
	store, db := openBackupTestDB(t, ctx, "backup-fullgraph", dbOpenOptions{
		changeFeedPayload: manifest.ChangeFeedPayloadFullValues,
	})
	writer := backupTestWriter(t, ctx, db, "backup-fullgraph-writer")

	const flushes = 66
	for i := 0; i < flushes; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if err := writer.Put(ctx, []byte(key), []byte("value-"+key)); err != nil {
			t.Fatalf("put: %v", err)
		}
		if err := writer.Flush(ctx); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	// Force a checkpoint through the ordinary maintenance command path.
	maintenance, err := db.OpenMaintenance(ctx, DefaultMaintenanceOptions())
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	t.Cleanup(func() { _ = maintenance.Close(ctx) })
	checkpoint, err := db.manifestStore.PrepareCheckpoint(ctx)
	if err != nil {
		t.Fatalf("prepare checkpoint: %v", err)
	}
	if err := maintenance.stageCommand(ctx, manifest.MaintenanceCommand{
		Kind:       manifest.MaintenanceCommandCheckpoint,
		Checkpoint: &checkpoint,
	}); err != nil {
		t.Fatalf("stage checkpoint: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("publish checkpoint: %v", err)
	}
	if _, err := maintenance.RunOnce(ctx); err != nil {
		t.Fatalf("reconcile checkpoint: %v", err)
	}

	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatalf("read current: %v", err)
	}
	if current.Snapshot == nil || len(current.IndexFrontier) == 0 {
		t.Fatalf("checkpoint did not publish snapshot/pages: snapshot=%v frontier=%d",
			current.Snapshot, len(current.IndexFrontier))
	}

	backup, err := db.BeginBackup(ctx, BackupOptions{LeaseTTL: time.Hour})
	if err != nil {
		t.Fatalf("begin backup: %v", err)
	}
	rows := drainBackupObjects(t, ctx, backup, 7)

	byKind := map[BackupObjectKind][]BackupObject{}
	for _, row := range rows {
		byKind[row.Kind] = append(byKind[row.Kind], row)
	}
	if len(byKind[BackupObjectCurrent]) != 1 {
		t.Fatalf("current rows=%d want=1", len(byKind[BackupObjectCurrent]))
	}
	if len(byKind[BackupObjectManifestSnapshot]) != 1 {
		t.Fatalf("snapshot rows=%d want=1", len(byKind[BackupObjectManifestSnapshot]))
	}
	if got := byKind[BackupObjectManifestSnapshot][0].Path; got != current.Snapshot.Path {
		t.Fatalf("snapshot path=%q want=%q", got, current.Snapshot.Path)
	}
	if len(byKind[BackupObjectManifestPage]) == 0 {
		t.Fatalf("expected reachable manifest pages")
	}
	if len(byKind[BackupObjectSST]) != flushes {
		t.Fatalf("sst rows=%d want=%d", len(byKind[BackupObjectSST]), flushes)
	}
	if len(byKind[BackupObjectChangeBatch]) != flushes {
		t.Fatalf("change batch rows=%d want=%d", len(byKind[BackupObjectChangeBatch]), flushes)
	}

	// Every family's rows are path-sorted within their group.
	for kind, familyRows := range byKind {
		paths := make([]string, len(familyRows))
		for i := range familyRows {
			paths[i] = familyRows[i].Path
		}
		if !sort.StringsAreSorted(paths) {
			t.Fatalf("rows for %q are not path sorted", kind)
		}
	}
	for _, row := range rows {
		verifyBackupObject(t, ctx, store, row)
	}

	// The page list must equal exactly the refs reachable from the frontier.
	graph, err := db.manifestStore.DescribeGeneration(ctx, current)
	if err != nil {
		t.Fatalf("describe generation: %v", err)
	}
	wantPages := map[string]struct{}{}
	for _, page := range graph.Pages {
		wantPages[page.Path] = struct{}{}
	}
	gotPages := map[string]struct{}{}
	for _, row := range byKind[BackupObjectManifestPage] {
		gotPages[row.Path] = struct{}{}
	}
	if len(gotPages) != len(wantPages) {
		t.Fatalf("page rows=%d want=%d", len(gotPages), len(wantPages))
	}
	for path := range wantPages {
		if _, ok := gotPages[path]; !ok {
			t.Fatalf("reachable page missing from manifest %q", path)
		}
	}
}

func TestOnlineBackupPinsSSTReclamationUntilRelease(t *testing.T) {
	ctx := context.Background()
	store, db := openBackupTestDB(t, ctx, "backup-pins-reclaim", dbOpenOptions{})
	writer := backupTestWriter(t, ctx, db, "backup-pins-writer")
	if err := writer.Put(ctx, []byte("k"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := writer.Put(ctx, []byte("k"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Backup pins the generation whose topology contains the two SSTs.
	backup, err := db.BeginBackup(ctx, BackupOptions{Owner: "reclaim-test", LeaseTTL: 24 * time.Hour})
	if err != nil {
		t.Fatalf("begin backup: %v", err)
	}
	pinnedRows := drainBackupObjects(t, ctx, backup, 50)
	pinnedSSTs := make(map[string]struct{})
	for _, row := range pinnedRows {
		if row.Kind == BackupObjectSST {
			pinnedSSTs[row.Path] = struct{}{}
		}
	}
	if len(pinnedSSTs) != 2 {
		t.Fatalf("pinned ssts=%d want=2", len(pinnedSSTs))
	}

	opts := DefaultMaintenanceOptions()
	opts.SSTCompaction.L0TriggerSSTs = 2
	opts.SSTCompaction.BaseLevelBytes = 1 << 60
	opts.ManifestCheckpoint.TargetReplayPages = ^uint64(0)
	opts.ManifestCheckpoint.TargetReplayBytes = ^uint64(0)
	maintenance, err := db.OpenMaintenance(ctx, opts)
	if err != nil {
		t.Fatalf("open maintenance: %v", err)
	}
	t.Cleanup(func() { _ = maintenance.Close(ctx) })
	if _, err := maintenance.RunOnce(ctx); err != nil {
		t.Fatalf("stage compaction: %v", err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("publish compaction: %v", err)
	}
	if _, err := maintenance.RunOnce(ctx); err != nil {
		t.Fatalf("reconcile compaction: %v", err)
	}
	plans := listSSTDeletionPlans(t, ctx, store)
	if len(plans) != 1 {
		t.Fatalf("deletion plans=%d want=1", len(plans))
	}

	// Process the now-due plan through the same guarded deleter maintenance
	// uses. The durable plan predates nothing here, but the guard must block
	// its pinned targets regardless of plan age.
	guarded := newLeaseGuardedDeleter(store, db.backupGuard)
	dueNow := time.Now().UTC().Add(3 * time.Hour)
	stats, err := runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, dueNow, guarded)
	if err != nil {
		t.Fatalf("guarded reclamation: %v", err)
	}
	if stats.Deleted != 0 || stats.Deferred != len(pinnedSSTs) {
		t.Fatalf("pinned reclaim stats=%+v", stats)
	}
	for key := range pinnedSSTs {
		if _, _, err := store.Read(ctx, key); err != nil {
			t.Fatalf("pinned SST %q deleted while lease active: %v", key, err)
		}
	}
	readyPath := sstDeletionPlanReadyPath(store, plans[0].NotBefore, plans[0].PlanID)
	requireObjectExists(t, ctx, store, readyPath, true)
	requireObjectExists(t, ctx, store, sstDeletionPlanCanonicalPath(store, plans[0].PlanID), true)

	if err := backup.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	// The released lease must not protect anything on a later pass.
	db.backupGuard.beginPass()
	stats, err = runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, dueNow, guarded)
	if err != nil {
		t.Fatalf("post-release reclamation: %v", err)
	}
	if stats.Deleted != len(pinnedSSTs) || stats.PlansDeleted != 1 {
		t.Fatalf("post-release stats=%+v", stats)
	}
	for key := range pinnedSSTs {
		if _, _, err := store.Read(ctx, key); err == nil {
			t.Fatalf("pinned SST %q retained after release", key)
		}
	}
}

func TestOnlineBackupPlanCreatedBeforeLeaseStillBlocked(t *testing.T) {
	ctx := context.Background()
	store, current, command, receipt, target := newSSTDeletionPlanFixture(t, ctx, "plan-before-lease")
	t.Cleanup(func() { _ = store.Close() })
	cleaner := newSSTCleaner(store, sstCleanerOptions{
		Now:          func() time.Time { return time.Now().UTC() },
		SafetyMargin: -1,
		Deleter:      store,
	})
	if _, err := cleaner.markCommandOutcome(ctx, current, command, receipt); err != nil {
		t.Fatalf("store plan: %v", err)
	}

	// The lease is established only after the durable deletion plan exists.
	viewID, root := writeSyntheticBackupView(t, ctx, store, []string{target.Key})
	now := time.Now().UTC()
	lease := &backupLeaseRecord{
		Version:      backupLeaseVersion,
		Kind:         backupLeaseKind,
		LeaseID:      "planbeforelease0000000000001",
		Owner:        "test",
		ViewID:       viewID,
		RootPath:     storeKey(store, backupRootPath(viewID)),
		RootChecksum: root.Checksum,
		CreatedAt:    now,
		ExpiresAt:    now.Add(24 * time.Hour),
		TTL:          24 * time.Hour,
		Status:       backupLeaseStatusActive,
	}
	if _, err := createBackupLease(ctx, store, lease); err != nil {
		t.Fatalf("create lease: %v", err)
	}

	guard := newBackupLeaseGuard(store, func() time.Time { return now.Add(2 * time.Hour) })
	guarded := newLeaseGuardedDeleter(store, guard)
	stats, err := runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, now.Add(2*time.Hour), guarded)
	if err != nil {
		t.Fatalf("reclaim with newer lease: %v", err)
	}
	if stats.Deleted != 0 || stats.Deferred != 1 {
		t.Fatalf("plan-before-lease stats=%+v", stats)
	}
	if _, _, err := store.Read(ctx, target.Key); err != nil {
		t.Fatalf("target deleted despite lease established after plan: %v", err)
	}

	if _, err := releaseBackupLease(ctx, store, lease.LeaseID, now.Add(2*time.Hour)); err != nil {
		t.Fatalf("release: %v", err)
	}
	guard.beginPass()
	if _, err := runSSTDeletionPlanReclaimer(ctx, store, 128, 1024, now.Add(2*time.Hour), guarded); err != nil {
		t.Fatalf("reclaim after release: %v", err)
	}
	if _, _, err := store.Read(ctx, target.Key); err == nil {
		t.Fatalf("target retained after lease release")
	}
}

func writeSyntheticBackupView(t *testing.T, ctx context.Context, store *blobstore.Store, sstKeys []string) (string, *backupViewRoot) {
	t.Helper()
	sum := sha256.Sum256([]byte("synthetic-pinned-current"))
	viewID := hex.EncodeToString(sum[:])
	entries := make([]backupObjectEntry, 0, len(sstKeys))
	for _, key := range sstKeys {
		entries = append(entries, backupObjectEntry{Kind: backupFamilySST, Path: key, Size: 1})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	chunk := &backupObjectChunk{
		Version: backupViewVersion,
		Kind:    backupViewKind,
		ViewID:  viewID,
		Family:  backupFamilySST,
		Index:   0,
		First:   entries[0].Path,
		Last:    entries[len(entries)-1].Path,
		Count:   len(entries),
		Entries: entries,
	}
	chunk.Checksum = backupChunkChecksum(chunk)
	chunkPayload, err := json.Marshal(chunk)
	if err != nil {
		t.Fatal(err)
	}
	chunkPath := storeKey(store, backupChunkPath(viewID, backupFamilySST, 0))
	if _, err := store.Write(ctx, chunkPath, chunkPayload); err != nil {
		t.Fatal(err)
	}
	root := &backupViewRoot{
		Version:    backupViewVersion,
		Kind:       backupViewKind,
		ViewID:     viewID,
		Current:    backupObjectEntry{Kind: "current", Path: storeKey(store, backupPinnedCurrentPath(viewID)), LogicalPath: store.ManifestPath(), EncodedBytes: uint64(len("synthetic-pinned-current")), Checksum: "sha256:" + viewID},
		CreatedAt:  time.Now().UTC(),
		EntryCount: 1 + len(entries),
		Families: []backupFamilyIndex{{
			Family: backupFamilySST,
			Count:  len(entries),
			Chunks: []backupChunkRef{{
				Family: backupFamilySST, Index: 0, Path: chunkPath,
				First: chunk.First, Last: chunk.Last, Count: chunk.Count,
				EncodedBytes: uint64(len(chunkPayload)), Checksum: chunk.Checksum,
			}},
		}},
	}
	root.Checksum = backupRootChecksum(root)
	rootPayload, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Write(ctx, storeKey(store, backupRootPath(viewID)), rootPayload); err != nil {
		t.Fatal(err)
	}
	return viewID, root
}

func TestOnlineBackupIdempotentBeginDoesNotRecapture(t *testing.T) {
	ctx := context.Background()
	_, db := openBackupTestDB(t, ctx, "backup-idem", dbOpenOptions{})
	writer := backupTestWriter(t, ctx, db, "backup-idem-writer")
	if err := writer.Put(ctx, []byte("k"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := db.BeginBackup(ctx, BackupOptions{
		Owner:          "idem-agent",
		IdempotencyKey: "stable-key",
		LeaseTTL:       time.Hour,
	})
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	firstToken, _ := first.Token()

	if err := writer.Put(ctx, []byte("k"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginBackup(ctx, BackupOptions{
		Owner:          "idem-agent",
		IdempotencyKey: "stable-key",
		LeaseTTL:       time.Hour,
	})
	if err != nil {
		t.Fatalf("idempotent retry begin: %v", err)
	}
	secondToken, _ := second.Token()
	if secondToken != firstToken {
		t.Fatalf("idempotent retry created a new lease: first=%+v second=%+v", firstToken, secondToken)
	}
	if second.Info().ViewID != first.Info().ViewID || second.Info().EntryCount != first.Info().EntryCount {
		t.Fatalf("idempotent retry recaptured generation: first=%+v second=%+v", first.Info(), second.Info())
	}

	other, err := db.BeginBackup(ctx, BackupOptions{Owner: "idem-agent", IdempotencyKey: "other-key", LeaseTTL: time.Hour})
	if err != nil {
		t.Fatalf("second backup begin: %v", err)
	}
	if other.Info().ViewID == first.Info().ViewID {
		t.Fatalf("distinct key unexpectedly captured the older generation")
	}
}

func TestOnlineBackupRenewReleaseExpire(t *testing.T) {
	ctx := context.Background()
	store, db := openBackupTestDB(t, ctx, "backup-ttl", dbOpenOptions{})
	writer := backupTestWriter(t, ctx, db, "backup-ttl-writer")
	if err := writer.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	clock := &controlledClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	db.backupClock = clock.now
	db.backupGuard = newBackupLeaseGuard(store, clock.now)

	backup, err := db.BeginBackup(ctx, BackupOptions{LeaseTTL: time.Hour})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	token, _ := backup.Token()

	clock.advance(30 * time.Minute)
	if _, err := db.OpenBackup(ctx, token); err != nil {
		t.Fatalf("resume within ttl: %v", err)
	}
	newExpiry, err := backup.Renew(ctx, time.Hour)
	if err != nil {
		t.Fatalf("renew: %v", err)
	}
	if !newExpiry.Equal(clock.now().Add(time.Hour)) {
		t.Fatalf("renew expiry=%s want=%s", newExpiry, clock.now().Add(time.Hour))
	}

	clock.advance(90 * time.Minute)
	if _, err := db.OpenBackup(ctx, token); !errors.Is(err, ErrBackupLeaseExpired) {
		t.Fatalf("expired resume err=%v want=%v", err, ErrBackupLeaseExpired)
	}
	if !backup.ExpiredAt(clock.now()) {
		t.Fatalf("backup not reported expired at %s", clock.now())
	}

	// Expiry removes protection even without an explicit release.
	guarded := newLeaseGuardedDeleter(store, db.backupGuard)
	current, err := db.manifestStore.ReadCurrentData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	graph, err := db.manifestStore.DescribeGeneration(ctx, current)
	if err != nil {
		t.Fatal(err)
	}
	liveKey := store.SSTPath(graph.SSTs[0].ID)
	if err := guarded.Delete(ctx, liveKey); err != nil {
		t.Fatalf("expired lease still protects %q: %v", liveKey, err)
	}

	// Release is idempotent after expiry and keeps late renewals rejected.
	if err := backup.Release(ctx); err != nil {
		t.Fatalf("release expired: %v", err)
	}
	if _, err := backup.Renew(ctx, time.Hour); !errors.Is(err, ErrBackupLeaseReleased) {
		t.Fatalf("renew after release err=%v want=%v", err, ErrBackupLeaseReleased)
	}
}

func TestLeaseGuardedDeleterSkipsGuardIOForNonDataKeys(t *testing.T) {
	ctx := context.Background()
	store := blobstore.NewMemory("backup-guard-io")
	t.Cleanup(func() { _ = store.Close() })
	recorder := &recordingDeleter{}
	guard := newBackupLeaseGuard(store, nil)
	deleter := newLeaseGuardedDeleter(recorder, guard)

	gcKey := "backup-guard-io/manifest/gc/sst/plans/does-not-exist.json"
	if err := deleter.Delete(ctx, gcKey); err != nil {
		t.Fatalf("delete gc key: %v", err)
	}
	guard.mu.Lock()
	loaded := guard.snapshot != nil
	guard.mu.Unlock()
	if loaded {
		t.Fatalf("guard loaded leases for a non-data key")
	}

	sstKey := "backup-guard-io/sstable/abc/some-sst"
	if err := deleter.Delete(ctx, sstKey); err != nil {
		t.Fatalf("delete data key with no leases: %v", err)
	}
	guard.mu.Lock()
	loaded = guard.snapshot != nil
	guard.mu.Unlock()
	if !loaded {
		t.Fatalf("guard did not load leases before deleting a data key")
	}
	if len(recorder.deleted) != 2 {
		t.Fatalf("base deleter calls=%d want=2 (%v)", len(recorder.deleted), recorder.deleted)
	}
}

func TestBackupChunkForKeyBoundary(t *testing.T) {
	index := backupFamilyIndex{Family: backupFamilySST, Chunks: []backupChunkRef{
		{First: "sstable/000/a", Last: "sstable/000/m", Path: "c0"},
		{First: "sstable/001/n", Last: "sstable/001/z", Path: "c1"},
	}}
	cases := []struct {
		key  string
		want string
	}{
		{"sstable/000/a", "c0"},
		{"sstable/000/g", "c0"},
		{"sstable/000/m", "c0"},
		{"sstable/001/n", "c1"},
		{"sstable/001/z", "c1"},
		{"sstable/002/aa", ""},
		{"sstable/00", ""},
	}
	for _, tc := range cases {
		ref := backupChunkForKey(&index, tc.key)
		got := ""
		if ref != nil {
			got = ref.Path
		}
		if got != tc.want {
			t.Errorf("key=%q chunk=%q want=%q", tc.key, got, tc.want)
		}
	}
}

func TestBackupLeaseCreateConvergesAfterUnknownPut(t *testing.T) {
	ctx := context.Background()
	_, db := openBackupTestDB(t, ctx, "backup-lease-retry", dbOpenOptions{})
	writer := backupTestWriter(t, ctx, db, "backup-lease-retry-writer")
	if err := writer.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := db.BeginBackup(ctx, BackupOptions{IdempotencyKey: "unknown-put-key", LeaseTTL: time.Hour})
	if err != nil {
		t.Fatalf("first begin: %v", err)
	}
	firstToken, _ := first.Token()

	// Simulate retrying Begin after an unknown lease-PUT result: the
	// idempotent key reads the durable lease before touching CURRENT, even
	// though a newer generation was committed meanwhile.
	if err := writer.Put(ctx, []byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := db.BeginBackup(ctx, BackupOptions{IdempotencyKey: "unknown-put-key", LeaseTTL: time.Hour})
	if err != nil {
		t.Fatalf("converging retry: %v", err)
	}
	secondToken, _ := second.Token()
	if secondToken != firstToken {
		t.Fatalf("unknown-put retry did not converge on the durable lease")
	}
}

func TestOnlineBackupOnEmptyDatabaseFails(t *testing.T) {
	ctx := context.Background()
	_, db := openBackupTestDB(t, ctx, "backup-empty", dbOpenOptions{})
	if _, err := db.BeginBackup(ctx, BackupOptions{LeaseTTL: time.Hour}); err == nil {
		t.Fatal("begin backup on empty database succeeded")
	}
}

type controlledClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *controlledClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *controlledClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type recordingDeleter struct {
	mu      sync.Mutex
	deleted []string
}

func (d *recordingDeleter) Delete(_ context.Context, key string) error {
	d.mu.Lock()
	d.deleted = append(d.deleted, key)
	d.mu.Unlock()
	return nil
}

func (d *recordingDeleter) BatchDelete(_ context.Context, keys []string) error {
	d.mu.Lock()
	d.deleted = append(d.deleted, keys...)
	d.mu.Unlock()
	return nil
}
