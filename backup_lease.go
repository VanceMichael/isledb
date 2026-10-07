package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
)

const (
	backupLeaseVersion = 1
	backupLeaseKind    = "isledb_backup_lease_v1"

	backupLeaseStatusActive   = "active"
	backupLeaseStatusReleased = "released"

	backupLeaseMaxEncodedBytes = 16 << 10
	backupLeaseCASMaxRetries   = 8
	backupGuardLeaseScanLimit  = 1000

	defaultBackupLeaseTTL = 24 * time.Hour
	minBackupLeaseTTL     = time.Minute
	maxBackupLeaseTTL     = 7 * 24 * time.Hour
)

// Public backup lifecycle errors.
var (
	ErrBackupLeaseNotFound = errors.New("backup lease not found")
	ErrBackupLeaseExpired  = errors.New("backup lease expired")
	ErrBackupLeaseReleased = errors.New("backup lease released")
	ErrBackupTokenInvalid  = errors.New("invalid backup token")
	ErrBackupLeaseConflict = errors.New("backup lease conflict")
)

// errBackupObjectPinned wraps every object key that a valid backup lease still
// protects. Physical reclamation treats it as a deferral, never a failure: the
// deletion plan stays intact and is retried after release or expiry.
type backupPinnedError struct {
	protected []string
}

func (e *backupPinnedError) Error() string {
	if e == nil || len(e.protected) == 0 {
		return "object pinned by active backup lease"
	}
	return fmt.Sprintf("object(s) pinned by active backup lease: %d protected, first %q",
		len(e.protected), e.protected[0])
}

func isBackupPinnedError(err error) (*backupPinnedError, bool) {
	var pinned *backupPinnedError
	if errors.As(err, &pinned) {
		return pinned, true
	}
	return nil, false
}

type backupLeaseRecord struct {
	Version int    `json:"version"`
	Kind    string `json:"kind"`

	LeaseID string `json:"lease_id"`
	Owner   string `json:"owner"`

	ViewID       string `json:"view_id"`
	RootPath     string `json:"root_path"`
	RootChecksum string `json:"root_checksum"`

	CreatedAt time.Time     `json:"created_at"`
	ExpiresAt time.Time     `json:"expires_at"`
	TTL       time.Duration `json:"ttl_nanos"`
	Renewals  int64         `json:"renewals"`
	Status    string        `json:"status"`
}

// active reports whether the lease currently forbids physical deletion of its
// view's objects.
func (r *backupLeaseRecord) active(now time.Time) bool {
	return r != nil && r.Status == backupLeaseStatusActive && now.Before(r.ExpiresAt.UTC())
}

func validateBackupLeaseID(leaseID string) bool {
	if len(leaseID) < 16 || len(leaseID) > 64 {
		return false
	}
	for i := 0; i < len(leaseID); i++ {
		c := leaseID[i]
		// KSUIDs use base62; caller-supplied idempotency identifiers use
		// lowercase hex. Both are safe as single object-key segments.
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
			return false
		}
	}
	return true
}

func validateBackupLease(store *blobstore.Store, record *backupLeaseRecord, objectPath string) error {
	if record == nil {
		return errors.New("nil backup lease")
	}
	if record.Version != backupLeaseVersion || record.Kind != backupLeaseKind {
		return fmt.Errorf("unsupported backup lease version=%d kind=%q", record.Version, record.Kind)
	}
	if !validateBackupLeaseID(record.LeaseID) {
		return fmt.Errorf("invalid backup lease id %q", record.LeaseID)
	}
	wantPath := storeKey(store, backupLeasePath(record.LeaseID))
	if objectPath != wantPath {
		return fmt.Errorf("backup lease path mismatch %q want %q", objectPath, wantPath)
	}
	if len(record.ViewID) != sha256.Size*2 || record.RootPath == "" || record.RootChecksum == "" {
		return fmt.Errorf("incomplete backup lease view reference id=%q", record.LeaseID)
	}
	if record.RootPath != storeKey(store, backupRootPath(record.ViewID)) {
		return fmt.Errorf("backup lease root path mismatch id=%q", record.LeaseID)
	}
	if record.CreatedAt.IsZero() || record.ExpiresAt.IsZero() || !record.ExpiresAt.After(record.CreatedAt) {
		return fmt.Errorf("incomplete backup lease timing id=%q", record.LeaseID)
	}
	if record.TTL < minBackupLeaseTTL || record.TTL > maxBackupLeaseTTL {
		return fmt.Errorf("invalid backup lease ttl=%s id=%q", record.TTL, record.LeaseID)
	}
	if record.Renewals < 0 {
		return fmt.Errorf("negative backup lease renewals id=%q", record.LeaseID)
	}
	switch record.Status {
	case backupLeaseStatusActive, backupLeaseStatusReleased:
	default:
		return fmt.Errorf("invalid backup lease status %q id=%q", record.Status, record.LeaseID)
	}
	return nil
}

func encodeBackupLease(record *backupLeaseRecord) ([]byte, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(payload) > backupLeaseMaxEncodedBytes {
		return nil, fmt.Errorf("backup lease bytes=%d max=%d", len(payload), backupLeaseMaxEncodedBytes)
	}
	return payload, nil
}

func decodeBackupLease(store *blobstore.Store, objectPath string, payload []byte) (*backupLeaseRecord, error) {
	if len(payload) == 0 || len(payload) > backupLeaseMaxEncodedBytes {
		return nil, fmt.Errorf("invalid backup lease bytes=%d", len(payload))
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record backupLeaseRecord
	if err := decoder.Decode(&record); err != nil {
		return nil, fmt.Errorf("decode backup lease %q: %w", objectPath, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("backup lease %q has trailing JSON", objectPath)
		}
		return nil, fmt.Errorf("decode backup lease %q trailer: %w", objectPath, err)
	}
	if err := validateBackupLease(store, &record, objectPath); err != nil {
		return nil, err
	}
	return &record, nil
}

// createBackupLease durably establishes a lease. Create-if-not-exist plus
// read-back validation makes retries after an unknown PUT result or a caller
// restart converge on the first durable record.
func createBackupLease(ctx context.Context, store *blobstore.Store, record *backupLeaseRecord) (*backupLeaseRecord, error) {
	leasePath := storeKey(store, backupLeasePath(record.LeaseID))
	payload, err := encodeBackupLease(record)
	if err != nil {
		return nil, err
	}
	if _, err := store.WriteIfNotExist(ctx, leasePath, payload); err == nil {
		return record, nil
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return nil, err
	}
	existing, _, err := store.Read(ctx, leasePath)
	if err != nil {
		return nil, fmt.Errorf("read existing backup lease %q: %w", leasePath, err)
	}
	stored, err := decodeBackupLease(store, leasePath, existing)
	if err != nil {
		return nil, fmt.Errorf("validate existing backup lease: %w", err)
	}
	if stored.ViewID != record.ViewID || stored.RootChecksum != record.RootChecksum {
		return nil, fmt.Errorf("%w: lease id=%q references a different backup view",
			ErrBackupLeaseConflict, record.LeaseID)
	}
	return stored, nil
}

func loadBackupLease(ctx context.Context, store *blobstore.Store, leaseID string) (*backupLeaseRecord, error) {
	if !validateBackupLeaseID(leaseID) {
		return nil, fmt.Errorf("%w: id=%q", ErrBackupTokenInvalid, leaseID)
	}
	leasePath := storeKey(store, backupLeasePath(leaseID))
	payload, _, err := store.Read(ctx, leasePath)
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) {
			return nil, ErrBackupLeaseNotFound
		}
		return nil, err
	}
	return decodeBackupLease(store, leasePath, payload)
}

// renewBackupLease extends an active lease with conditional-write semantics.
// A precondition failure re-reads durable state and converges: a concurrent
// renewal is adopted, while release or expiry is reported to the caller.
func renewBackupLease(
	ctx context.Context,
	store *blobstore.Store,
	leaseID string,
	ttl time.Duration,
	now time.Time,
) (*backupLeaseRecord, error) {
	ttl, err := normalizeBackupLeaseTTL(ttl)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < backupLeaseCASMaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, matchToken, err := readBackupLeaseForUpdate(ctx, store, leaseID, now)
		if err != nil {
			return nil, err
		}
		updated := *record
		newExpiry := now.Add(ttl)
		if !newExpiry.After(updated.ExpiresAt) {
			// Another renewal already extended at least as far.
			return &updated, nil
		}
		updated.ExpiresAt = newExpiry
		updated.TTL = ttl
		updated.Renewals++
		payload, err := encodeBackupLease(&updated)
		if err != nil {
			return nil, err
		}
		if err := writeObjectCAS(ctx, store, storeKey(store, backupLeasePath(leaseID)), payload, matchToken, true); err == nil {
			return &updated, nil
		} else if !isGCMarkCASConflict(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("renew backup lease after %d CAS retries: %w", backupLeaseCASMaxRetries, ErrBackupLeaseConflict)
}

// releaseBackupLease marks a lease released. Releasing an already released or
// expired lease is idempotent. The durable tombstone keeps expiry visible so
// late renewals cannot resurrect protection.
func releaseBackupLease(
	ctx context.Context,
	store *blobstore.Store,
	leaseID string,
	now time.Time,
) (*backupLeaseRecord, error) {
	for attempt := 0; attempt < backupLeaseCASMaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		record, matchToken, exists, err := readBackupLeaseAnyState(ctx, store, leaseID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrBackupLeaseNotFound
		}
		if record.Status == backupLeaseStatusReleased {
			return record, nil
		}
		updated := *record
		updated.Status = backupLeaseStatusReleased
		// The tombstone may be swept once it has been visibly inactive for a
		// full TTL; ExpiresAt continues to anchor that delay.
		if now.After(updated.ExpiresAt) {
			updated.ExpiresAt = now.Add(updated.TTL)
		}
		payload, err := encodeBackupLease(&updated)
		if err != nil {
			return nil, err
		}
		if err := writeObjectCAS(ctx, store, storeKey(store, backupLeasePath(leaseID)), payload, matchToken, true); err == nil {
			return &updated, nil
		} else if !isGCMarkCASConflict(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("release backup lease after %d CAS retries: %w", backupLeaseCASMaxRetries, ErrBackupLeaseConflict)
}

func readBackupLeaseForUpdate(
	ctx context.Context,
	store *blobstore.Store,
	leaseID string,
	now time.Time,
) (*backupLeaseRecord, string, error) {
	record, matchToken, exists, err := readBackupLeaseAnyState(ctx, store, leaseID)
	if err != nil {
		return nil, "", err
	}
	if !exists {
		return nil, "", ErrBackupLeaseNotFound
	}
	switch record.Status {
	case backupLeaseStatusReleased:
		return nil, "", ErrBackupLeaseReleased
	case backupLeaseStatusActive:
		if !record.active(now) {
			return nil, "", ErrBackupLeaseExpired
		}
	}
	return record, matchToken, nil
}

func readBackupLeaseAnyState(
	ctx context.Context,
	store *blobstore.Store,
	leaseID string,
) (*backupLeaseRecord, string, bool, error) {
	if !validateBackupLeaseID(leaseID) {
		return nil, "", false, fmt.Errorf("%w: id=%q", ErrBackupTokenInvalid, leaseID)
	}
	data, matchToken, exists, err := readObjectWithCAS(ctx, store, storeKey(store, backupLeasePath(leaseID)))
	if err != nil {
		return nil, "", false, err
	}
	if !exists {
		return nil, "", false, nil
	}
	record, err := decodeBackupLease(store, storeKey(store, backupLeasePath(leaseID)), data)
	if err != nil {
		return nil, "", false, err
	}
	return record, matchToken, true, nil
}

func normalizeBackupLeaseTTL(ttl time.Duration) (time.Duration, error) {
	if ttl == 0 {
		return defaultBackupLeaseTTL, nil
	}
	if ttl < 0 {
		return 0, fmt.Errorf("%w: negative backup lease ttl=%s", ErrBackupTokenInvalid, ttl)
	}
	if ttl < minBackupLeaseTTL {
		return 0, fmt.Errorf("%w: backup lease ttl=%s below minimum=%s", ErrBackupTokenInvalid, ttl, minBackupLeaseTTL)
	}
	if ttl > maxBackupLeaseTTL {
		return 0, fmt.Errorf("%w: backup lease ttl=%s above maximum=%s", ErrBackupTokenInvalid, ttl, maxBackupLeaseTTL)
	}
	return ttl, nil
}

// backupLeaseGuard loads the active-lease set once per physical reclamation
// pass and answers membership questions against the immutable, checksummed
// view manifests. All of its state is derived from durable objects, so a
// reclaimer restart reconstructs identical protection.
type backupLeaseGuard struct {
	store *blobstore.Store
	now   func() time.Time

	mu       sync.Mutex
	snapshot *backupGuardSnapshot
}

func newBackupLeaseGuard(store *blobstore.Store, now func() time.Time) *backupLeaseGuard {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &backupLeaseGuard{store: store, now: now}
}

// beginPass discards the cached lease set. The next guarded delete in the new
// pass reloads it; a pass that performs no deletes performs no guard I/O.
func (g *backupLeaseGuard) beginPass() {
	g.mu.Lock()
	g.snapshot = nil
	g.mu.Unlock()
}

func (g *backupLeaseGuard) ensureLoaded(ctx context.Context) (*backupGuardSnapshot, error) {
	g.mu.Lock()
	snap := g.snapshot
	g.mu.Unlock()
	if snap != nil {
		return snap, nil
	}
	loaded, err := g.load(ctx)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.snapshot == nil {
		g.snapshot = loaded
	}
	loaded = g.snapshot
	g.mu.Unlock()
	return loaded, nil
}

func (g *backupLeaseGuard) load(ctx context.Context) (*backupGuardSnapshot, error) {
	now := g.now().UTC()
	snap := &backupGuardSnapshot{
		loadedAt:   now,
		roots:      make(map[string]*backupViewRoot),
		chunkCache: make(map[string]*backupObjectChunk),
	}
	iter := g.store.NewListIterator(blobstore.ListOptions{Prefix: backupLeasesPrefix + "/"})
	scanned := 0
	for {
		object, err := iter.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("list backup leases: %w", err)
		}
		if object.IsDir {
			continue
		}
		scanned++
		if scanned > backupGuardLeaseScanLimit {
			return nil, fmt.Errorf("backup lease count exceeds guard scan limit=%d; refuse physical deletion", backupGuardLeaseScanLimit)
		}
		if path.Ext(object.Key) != ".json" {
			continue
		}
		payload, _, err := g.store.Read(ctx, object.Key)
		if err != nil {
			if errors.Is(err, blobstore.ErrNotFound) {
				continue
			}
			return nil, fmt.Errorf("read backup lease %q: %w", object.Key, err)
		}
		record, err := decodeBackupLease(g.store, object.Key, payload)
		if err != nil {
			return nil, fmt.Errorf("validate backup lease %q: %w", object.Key, err)
		}
		if !record.active(now) {
			continue
		}
		root, ok := snap.roots[record.ViewID]
		if !ok {
			root, err = loadBackupViewRoot(ctx, g.store, record.ViewID)
			if err != nil {
				return nil, fmt.Errorf("load pinned backup view %q for lease %q: %w",
					record.ViewID, record.LeaseID, err)
			}
			snap.roots[record.ViewID] = root
		}
		if root.Checksum != record.RootChecksum {
			return nil, fmt.Errorf("backup lease %q root checksum mismatch view=%q", record.LeaseID, record.ViewID)
		}
		snap.activeLeases = append(snap.activeLeases, record)
	}
	return snap, nil
}

type backupGuardSnapshot struct {
	loadedAt time.Time
	// roots deduplicates view manifests shared by multiple active leases.
	roots map[string]*backupViewRoot
	// activeLeases is retained for diagnostics and lease counting.
	activeLeases []*backupLeaseRecord
	// chunkCache memoizes downloaded chunk objects for this pass.
	chunkCache map[string]*backupObjectChunk
	chunkMu    sync.Mutex
}

// protectedSet partitions candidate data-object keys into allowed and
// lease-protected sets. Non-data keys (GC records, backup bookkeeping) are
// always allowed.
func (g *backupLeaseGuard) familyForKey(key string) string {
	return backupFamilyForKey(g.store.Prefix(), key)
}

func (snap *backupGuardSnapshot) partition(ctx context.Context, guard *backupLeaseGuard, keys []string) (allowed, protected []string, err error) {
	for _, key := range keys {
		family := guard.familyForKey(key)
		if family == "" {
			allowed = append(allowed, key)
			continue
		}
		pinned, checkErr := snap.isProtected(ctx, guard, family, key)
		if checkErr != nil {
			return nil, nil, checkErr
		}
		if pinned {
			protected = append(protected, key)
		} else {
			allowed = append(allowed, key)
		}
	}
	return allowed, protected, nil
}

func (snap *backupGuardSnapshot) isProtected(
	ctx context.Context,
	guard *backupLeaseGuard,
	family, key string,
) (bool, error) {
	viewIDs := make([]string, 0, len(snap.roots))
	for viewID := range snap.roots {
		viewIDs = append(viewIDs, viewID)
	}
	sort.Strings(viewIDs)
	for _, viewID := range viewIDs {
		root := snap.roots[viewID]
		index := backupFamilyIndexFor(root, family)
		if index == nil {
			continue
		}
		chunkRef := backupChunkForKey(index, key)
		if chunkRef == nil {
			continue
		}
		chunk, err := snap.loadChunk(ctx, guard, chunkRef.Path)
		if err != nil {
			return false, err
		}
		for i := range chunk.Entries {
			if chunk.Entries[i].Path == key {
				return true, nil
			}
		}
	}
	return false, nil
}

func (snap *backupGuardSnapshot) loadChunk(ctx context.Context, guard *backupLeaseGuard, chunkPath string) (*backupObjectChunk, error) {
	snap.chunkMu.Lock()
	if chunk, ok := snap.chunkCache[chunkPath]; ok {
		snap.chunkMu.Unlock()
		return chunk, nil
	}
	snap.chunkMu.Unlock()

	chunk, err := loadBackupChunk(ctx, guard.store, chunkPath)
	if err != nil {
		return nil, err
	}
	snap.chunkMu.Lock()
	if existing, ok := snap.chunkCache[chunkPath]; ok {
		chunk = existing
	} else {
		snap.chunkCache[chunkPath] = chunk
	}
	snap.chunkMu.Unlock()
	return chunk, nil
}

func backupFamilyIndexFor(root *backupViewRoot, family string) *backupFamilyIndex {
	for i := range root.Families {
		if root.Families[i].Family == family {
			return &root.Families[i]
		}
	}
	return nil
}

// backupChunkForKey returns the chunk whose sorted [First, Last] range
// contains key, or nil when no chunk in the family can contain it.
func backupChunkForKey(index *backupFamilyIndex, key string) *backupChunkRef {
	chunks := index.Chunks
	i := sort.Search(len(chunks), func(i int) bool {
		return chunks[i].First >= key
	})
	if i < len(chunks) && chunks[i].First == key {
		return &chunks[i]
	}
	if i == 0 {
		return nil
	}
	candidate := &chunks[i-1]
	if key <= candidate.Last {
		return candidate
	}
	return nil
}

// leaseGuardedDeleter is the single physical-deletion chokepoint used by
// maintenance. Every reclamation family deletes through it, so active backup
// leases are enforced uniformly for SSTs, change batches, snapshots, and
// manifest pages, including orphan-audit paths and plans that were durably
// scheduled before the lease existed.
type leaseGuardedDeleter struct {
	base  objectDeleter
	guard *backupLeaseGuard
}

func newLeaseGuardedDeleter(base objectDeleter, guard *backupLeaseGuard) *leaseGuardedDeleter {
	return &leaseGuardedDeleter{base: base, guard: guard}
}

func (d *leaseGuardedDeleter) Delete(ctx context.Context, key string) error {
	family := d.guard.familyForKey(key)
	if family == "" {
		return d.base.Delete(ctx, key)
	}
	snap, err := d.guard.ensureLoaded(ctx)
	if err != nil {
		return err
	}
	protected, err := snap.isProtected(ctx, d.guard, family, key)
	if err != nil {
		return err
	}
	if protected {
		return &backupPinnedError{protected: []string{key}}
	}
	return d.base.Delete(ctx, key)
}

func (d *leaseGuardedDeleter) BatchDelete(ctx context.Context, keys []string) error {
	hasData := false
	for _, key := range keys {
		if d.guard.familyForKey(key) != "" {
			hasData = true
			break
		}
	}
	if !hasData {
		return d.base.BatchDelete(ctx, keys)
	}
	snap, err := d.guard.ensureLoaded(ctx)
	if err != nil {
		return err
	}
	allowed, protected, err := snap.partition(ctx, d.guard, keys)
	if err != nil {
		return err
	}
	var deleteErr error
	if len(allowed) > 0 {
		deleteErr = d.base.BatchDelete(ctx, allowed)
	}
	if len(protected) > 0 {
		sort.Strings(protected)
		return errors.Join(deleteErr, &backupPinnedError{protected: protected})
	}
	return deleteErr
}

// opportunisticallySweepBackupLeases removes released tombstones and expired
// active records once they have stayed inactive past their TTL. It is bounded
// and best-effort: a failed sweep never blocks backup or reclamation.
func opportunisticallySweepBackupLeases(
	ctx context.Context,
	store *blobstore.Store,
	now time.Time,
	limit int,
) (swept int, err error) {
	if limit <= 0 {
		return 0, nil
	}
	iter := store.NewListIterator(blobstore.ListOptions{Prefix: backupLeasesPrefix + "/"})
	for swept < limit {
		object, nextErr := iter.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			return swept, nil
		}
		if nextErr != nil {
			return swept, nextErr
		}
		if object.IsDir || !strings.HasSuffix(object.Key, ".json") {
			continue
		}
		payload, _, readErr := store.Read(ctx, object.Key)
		if readErr != nil {
			if errors.Is(readErr, blobstore.ErrNotFound) {
				continue
			}
			return swept, readErr
		}
		record, decodeErr := decodeBackupLease(store, object.Key, payload)
		if decodeErr != nil {
			continue
		}
		sweepable := record.Status == backupLeaseStatusReleased || !record.active(now)
		if !sweepable || now.Before(record.ExpiresAt.Add(record.TTL)) {
			continue
		}
		if deleteErr := store.Delete(ctx, object.Key); deleteErr != nil {
			return swept, deleteErr
		}
		swept++
	}
	return swept, nil
}
