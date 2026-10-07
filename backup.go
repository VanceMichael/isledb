package isledb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"github.com/segmentio/ksuid"
)

// BackupObjectKind identifies one family of objects in an online backup view.
type BackupObjectKind string

const (
	// BackupObjectCurrent is the pinned manifest/CURRENT generation. Its bytes
	// must be restored at LogicalPath, not at the internal pinned-copy path.
	BackupObjectCurrent BackupObjectKind = "current"
	// BackupObjectManifestSnapshot is a manifest checkpoint object.
	BackupObjectManifestSnapshot BackupObjectKind = "manifest_snapshot"
	// BackupObjectManifestPage is one immutable manifest page.
	BackupObjectManifestPage BackupObjectKind = "manifest_page"
	// BackupObjectSST is one live SST data file.
	BackupObjectSST BackupObjectKind = "sstable"
	// BackupObjectChangeBatch is one change-feed batch required by the
	// generation.
	BackupObjectChangeBatch BackupObjectKind = "change_batch"
)

// BackupObject is one row of a stable backup manifest. Every row carries the
// exact object key and size and/or checksum information sufficient to verify
// a copied object independently.
type BackupObject struct {
	// Kind identifies the object family.
	Kind BackupObjectKind
	// Path is the exact object key to copy.
	Path string
	// LogicalPath is set only for the pinned CURRENT row: restore the copied
	// bytes at this key (manifest/CURRENT). Empty for every other family,
	// where LogicalPath equals Path.
	LogicalPath string
	// Size is the stored-object length in bytes. It is populated for SST and
	// change-batch objects.
	Size int64
	// EncodedBytes is the exact encoded length of a manifest object.
	EncodedBytes uint64
	// Checksum is "sha256:" plus 64 lowercase hex digits when the generation
	// recorded an object checksum.
	Checksum string
}

// BackupOptions configures one online backup session.
type BackupOptions struct {
	// Owner labels the backup agent in the durable lease record. It is
	// diagnostic only; possession of the returned token grants renew/release.
	// Empty selects a generated agent label.
	Owner string

	// IdempotencyKey optionally makes BeginBackup retry-safe across unknown
	// commit results and process restarts. Repeating BeginBackup with the
	// same (Owner, IdempotencyKey) pair always adopts the first durable lease
	// instead of capturing a newer CURRENT generation. Callers that need
	// crash-safe convergence before persisting the returned token should set
	// it. Empty generates a unique lease per call.
	IdempotencyKey string

	// LeaseTTL is the initial lease lifetime. Zero selects 24h; values are
	// clamped to [1m, 168h]. Renew extends it; physical reclamation of every
	// object in the view is blocked while the lease is active.
	LeaseTTL time.Duration
}

// Backup is a handle on one pinned, independently copyable database
// generation. The handle is safe for concurrent ListObjects and Renew calls;
// serialize Release with other calls.
type Backup struct {
	store         *blobstore.Store
	manifestStore *manifest.Store
	now           func() time.Time

	mu    sync.Mutex
	lease *backupLeaseRecord
	root  *backupViewRoot
}

// BackupToken is the caller-persisted recovery handle. It survives process
// restarts and always addresses the same lease and view: resuming with it can
// neither capture a newer generation nor drift to a different manifest.
type BackupToken struct {
	LeaseID      string `json:"lease_id"`
	ViewID       string `json:"view_id"`
	RootChecksum string `json:"root_checksum"`
}

// MarshalText serializes the token as URL-safe base64 JSON.
func (t BackupToken) MarshalText() ([]byte, error) {
	if t.LeaseID == "" || t.ViewID == "" || t.RootChecksum == "" {
		return nil, fmt.Errorf("%w: incomplete backup token", ErrBackupTokenInvalid)
	}
	raw, err := json.Marshal(struct {
		LeaseID      string `json:"lease_id"`
		ViewID       string `json:"view_id"`
		RootChecksum string `json:"root_checksum"`
	}{LeaseID: t.LeaseID, ViewID: t.ViewID, RootChecksum: t.RootChecksum})
	if err != nil {
		return nil, err
	}
	enc := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(enc, raw)
	return enc, nil
}

// UnmarshalText parses a token produced by MarshalText.
func (t *BackupToken) UnmarshalText(text []byte) error {
	raw := make([]byte, base64.RawURLEncoding.DecodedLen(len(text)))
	n, err := base64.RawURLEncoding.Decode(raw, text)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBackupTokenInvalid, err)
	}
	var wire struct {
		LeaseID      string `json:"lease_id"`
		ViewID       string `json:"view_id"`
		RootChecksum string `json:"root_checksum"`
	}
	if err := json.Unmarshal(raw[:n], &wire); err != nil {
		return fmt.Errorf("%w: %v", ErrBackupTokenInvalid, err)
	}
	if !validateBackupLeaseID(wire.LeaseID) || len(wire.ViewID) != 64 || wire.RootChecksum == "" {
		return fmt.Errorf("%w: malformed fields", ErrBackupTokenInvalid)
	}
	t.LeaseID = wire.LeaseID
	t.ViewID = wire.ViewID
	t.RootChecksum = wire.RootChecksum
	return nil
}

// String returns the portable token text.
func (t BackupToken) String() string {
	text, err := t.MarshalText()
	if err != nil {
		return ""
	}
	return string(text)
}

// DecodeBackupToken parses token text produced by BackupToken.String.
func DecodeBackupToken(text string) (BackupToken, error) {
	var token BackupToken
	if err := token.UnmarshalText([]byte(strings.TrimSpace(text))); err != nil {
		return BackupToken{}, err
	}
	return token, nil
}

// BackupInfo describes the pinned generation and its lease.
type BackupInfo struct {
	// LeaseID identifies the durable lease.
	LeaseID string
	// ViewID identifies the pinned CURRENT generation (sha256 of its bytes).
	ViewID string
	// EntryCount is the total number of objects in the stable manifest,
	// including the pinned CURRENT row.
	EntryCount int
	// TotalBytes is the informational sum of recorded object sizes.
	TotalBytes int64
	// ExpiresAt is the current lease deadline.
	ExpiresAt time.Time
	// Released reports whether the lease was explicitly released.
	Released bool
}

func (b *Backup) Info() BackupInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	info := BackupInfo{
		LeaseID:    b.lease.LeaseID,
		ViewID:     b.lease.ViewID,
		ExpiresAt:  b.lease.ExpiresAt,
		EntryCount: b.root.EntryCount,
		TotalBytes: b.root.TotalBytes,
		Released:   b.lease.Status == backupLeaseStatusReleased,
	}
	return info
}

// Token returns the caller-persisted recovery token.
func (b *Backup) Token() (BackupToken, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	token := BackupToken{
		LeaseID:      b.lease.LeaseID,
		ViewID:       b.lease.ViewID,
		RootChecksum: b.lease.RootChecksum,
	}
	if _, err := token.MarshalText(); err != nil {
		return BackupToken{}, err
	}
	return token, nil
}

// Renew extends the lease. Zero ttl selects the default TTL. Renewing after
// another concurrent renewal adopts the later deadline; renewing a released
// or expired lease returns ErrBackupLeaseReleased or ErrBackupLeaseExpired.
func (b *Backup) Renew(ctx context.Context, ttl time.Duration) (time.Time, error) {
	b.mu.Lock()
	leaseID := b.lease.LeaseID
	b.mu.Unlock()
	record, err := renewBackupLease(ctx, b.store, leaseID, ttl, b.now().UTC())
	if err != nil {
		return time.Time{}, err
	}
	b.mu.Lock()
	b.lease = record
	expires := record.ExpiresAt
	b.mu.Unlock()
	return expires, nil
}

// Release explicitly ends the lease. It is idempotent and never revokes
// already-copied objects; after release, ordinary reclamation resumes for
// objects that remain unreachable.
func (b *Backup) Release(ctx context.Context) error {
	b.mu.Lock()
	leaseID := b.lease.LeaseID
	b.mu.Unlock()
	record, err := releaseBackupLease(ctx, b.store, leaseID, b.now().UTC())
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.lease = record
	b.mu.Unlock()
	return nil
}

// ExpiredAt reports whether the lease is no longer active at now according to
// durable state. It performs no I/O; call OpenBackup after a restart to read
// the authoritative state.
func (b *Backup) ExpiredAt(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.lease.active(now.UTC())
}

// BackupListCursor is an opaque, stable pagination position in one view.
type BackupListCursor struct {
	currentEmitted bool
	familyIndex    int
	chunk          int
	offset         int
	done           bool
	viewID         string
}

type backupListCursorWire struct {
	CurrentEmitted bool   `json:"c,omitempty"`
	FamilyIndex    int    `json:"f,omitempty"`
	Chunk          int    `json:"i,omitempty"`
	Offset         int    `json:"o,omitempty"`
	Done           bool   `json:"d,omitempty"`
	ViewID         string `json:"v,omitempty"`
}

// MarshalText serializes the cursor.
func (c BackupListCursor) MarshalText() ([]byte, error) {
	raw, err := json.Marshal(backupListCursorWire{
		CurrentEmitted: c.currentEmitted,
		FamilyIndex:    c.familyIndex,
		Chunk:          c.chunk,
		Offset:         c.offset,
		Done:           c.done,
		ViewID:         c.viewID,
	})
	if err != nil {
		return nil, err
	}
	enc := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	base64.RawURLEncoding.Encode(enc, raw)
	return enc, nil
}

// UnmarshalText parses a cursor produced by MarshalText.
func (c *BackupListCursor) UnmarshalText(text []byte) error {
	raw := make([]byte, base64.RawURLEncoding.DecodedLen(len(text)))
	n, err := base64.RawURLEncoding.Decode(raw, text)
	if err != nil {
		return fmt.Errorf("invalid backup list cursor: %w", err)
	}
	var wire backupListCursorWire
	if err := json.Unmarshal(raw[:n], &wire); err != nil {
		return fmt.Errorf("invalid backup list cursor: %w", err)
	}
	if wire.FamilyIndex < 0 || wire.Chunk < 0 || wire.Offset < 0 {
		return errors.New("invalid backup list cursor: negative position")
	}
	c.currentEmitted = wire.CurrentEmitted
	c.familyIndex = wire.FamilyIndex
	c.chunk = wire.Chunk
	c.offset = wire.Offset
	c.done = wire.Done
	c.viewID = wire.ViewID
	return nil
}

// DecodeBackupListCursor parses cursor text.
func DecodeBackupListCursor(text string) (BackupListCursor, error) {
	var cursor BackupListCursor
	if err := cursor.UnmarshalText([]byte(strings.TrimSpace(text))); err != nil {
		return BackupListCursor{}, err
	}
	return cursor, nil
}

// ListObjects returns one deterministic page of the stable manifest. Pass nil
// for the first page and the returned cursor for the next; a nil cursor means
// the listing is complete. The sequence never changes for the view: writes,
// checkpoints, compaction, retention, and caller restarts neither add, drop,
// nor reorder rows.
func (b *Backup) ListObjects(
	ctx context.Context,
	pageSize int,
	cursor *BackupListCursor,
) ([]BackupObject, *BackupListCursor, error) {
	if pageSize <= 0 {
		pageSize = 1000
	}
	if pageSize > 1000 {
		pageSize = 1000
	}
	b.mu.Lock()
	root := b.root
	viewID := b.lease.ViewID
	b.mu.Unlock()

	pos := BackupListCursor{viewID: viewID}
	if cursor != nil {
		if cursor.viewID != "" && cursor.viewID != viewID {
			return nil, nil, fmt.Errorf("%w: cursor view=%q backup view=%q",
				ErrBackupTokenInvalid, cursor.viewID, viewID)
		}
		pos = *cursor
		pos.viewID = viewID
	}
	if pos.done {
		return nil, nil, nil
	}

	objects := make([]BackupObject, 0, pageSize)
	for len(objects) < pageSize {
		if !pos.currentEmitted {
			objects = append(objects, publicBackupObject(root.Current))
			pos.currentEmitted = true
			continue
		}
		if pos.familyIndex >= len(root.Families) {
			pos.done = true
			break
		}
		family := &root.Families[pos.familyIndex]
		if pos.chunk >= len(family.Chunks) {
			pos.familyIndex++
			pos.chunk = 0
			pos.offset = 0
			continue
		}
		ref := family.Chunks[pos.chunk]
		chunk, err := loadBackupChunk(ctx, b.store, ref.Path)
		if err != nil {
			return nil, nil, err
		}
		if chunk.Count != ref.Count || chunk.ViewID != viewID || chunk.Family != family.Family || chunk.Index != pos.chunk {
			return nil, nil, fmt.Errorf("backup chunk identity drift path=%q", ref.Path)
		}
		for pos.offset < chunk.Count && len(objects) < pageSize {
			objects = append(objects, publicBackupObject(chunk.Entries[pos.offset]))
			pos.offset++
		}
		if pos.offset >= chunk.Count {
			pos.chunk++
			pos.offset = 0
		}
	}

	var next *BackupListCursor
	if !pos.done {
		pos.viewID = viewID
		posCopy := pos
		next = &posCopy
	}
	return objects, next, nil
}

func publicBackupObject(entry backupObjectEntry) BackupObject {
	kind := BackupObjectKind(entry.Kind)
	return BackupObject{
		Kind:         kind,
		Path:         entry.Path,
		LogicalPath:  entry.LogicalPath,
		Size:         entry.Size,
		EncodedBytes: entry.EncodedBytes,
		Checksum:     entry.Checksum,
	}
}

func (db *DB) backupNow() func() time.Time {
	if db.backupClock != nil {
		return db.backupClock
	}
	return func() time.Time { return time.Now().UTC() }
}

// BeginBackup captures one committed database generation online, without
// pausing the writer, reader, or maintenance. CURRENT is read exactly once;
// the returned view enumerates that generation's CURRENT, reachable manifest
// snapshot and pages, every live SST, and the required change batches, and
// pins them with a durable object-store lease for the backup lifetime.
func (db *DB) BeginBackup(ctx context.Context, opts BackupOptions) (*Backup, error) {
	if err := checkContext(ctx); err != nil {
		return nil, ctx.Err()
	}
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}
	now := db.backupNow()().UTC()
	ttl, err := normalizeBackupLeaseTTL(opts.LeaseTTL)
	if err != nil {
		return nil, err
	}
	owner := strings.TrimSpace(opts.Owner)
	if owner == "" {
		owner = "backup-agent"
	}
	if len(owner) > 256 {
		return nil, fmt.Errorf("%w: owner too long", ErrBackupTokenInvalid)
	}

	leaseID, err := backupLeaseIdentifier(owner, opts.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	// A caller-supplied idempotency key converges before reading CURRENT:
	// retrying Begin after an unknown commit, or from a restarted process, must
	// adopt the first durable lease and can never capture a newer generation.
	if opts.IdempotencyKey != "" {
		if existing, loadErr := loadBackupLease(ctx, db.store, leaseID); loadErr == nil {
			return db.backupHandle(ctx, existing)
		} else if !errors.Is(loadErr, ErrBackupLeaseNotFound) {
			return nil, loadErr
		}
	}

	// The single authoritative generation read for this backup. Every later
	// derivation uses these bytes only.
	currentBytes, _, err := db.manifestStore.Storage().ReadCurrent(ctx)
	if err != nil {
		if errors.Is(err, manifest.ErrNotFound) {
			return nil, fmt.Errorf("begin backup: database is not initialized")
		}
		return nil, fmt.Errorf("read CURRENT for backup: %w", err)
	}

	viewID, root, err := captureBackupView(ctx, db.store, db.manifestStore, currentBytes, now)
	if err != nil {
		return nil, err
	}

	record := &backupLeaseRecord{
		Version:      backupLeaseVersion,
		Kind:         backupLeaseKind,
		LeaseID:      leaseID,
		Owner:        owner,
		ViewID:       viewID,
		RootPath:     storeKey(db.store, backupRootPath(viewID)),
		RootChecksum: root.Checksum,
		CreatedAt:    now,
		ExpiresAt:    now.Add(ttl),
		TTL:          ttl,
		Status:       backupLeaseStatusActive,
	}
	stored, err := createBackupLease(ctx, db.store, record)
	if err != nil {
		return nil, err
	}

	// Best-effort bounded cleanup of tombstones/expired records. Failures are
	// operational noise, never a backup failure.
	_, _ = opportunisticallySweepBackupLeases(ctx, db.store, now, 100)

	return db.backupHandle(ctx, stored)
}

// OpenBackup resumes a previously started backup from a persisted token,
// including in a new process. It re-reads durable lease state and continues
// with exactly the same pinned view and manifest.
func (db *DB) OpenBackup(ctx context.Context, token BackupToken) (*Backup, error) {
	if err := checkContext(ctx); err != nil {
		return nil, ctx.Err()
	}
	if db.closed.Load() {
		return nil, errors.New("db closed")
	}
	if _, err := token.MarshalText(); err != nil {
		return nil, err
	}
	record, err := loadBackupLease(ctx, db.store, token.LeaseID)
	if err != nil {
		return nil, err
	}
	if record.ViewID != token.ViewID {
		return nil, fmt.Errorf("%w: lease view=%q token view=%q",
			ErrBackupTokenInvalid, record.ViewID, token.ViewID)
	}
	if record.Status == backupLeaseStatusReleased {
		return nil, ErrBackupLeaseReleased
	}
	if !record.active(db.backupNow()().UTC()) {
		return nil, ErrBackupLeaseExpired
	}
	handle, err := db.backupHandle(ctx, record)
	if err != nil {
		return nil, err
	}
	if handle.lease.RootChecksum != token.RootChecksum {
		return nil, fmt.Errorf("%w: root checksum mismatch", ErrBackupTokenInvalid)
	}
	return handle, nil
}

func (db *DB) backupHandle(ctx context.Context, record *backupLeaseRecord) (*Backup, error) {
	root, err := loadBackupViewRoot(ctx, db.store, record.ViewID)
	if err != nil {
		return nil, err
	}
	if root.Checksum != record.RootChecksum {
		return nil, fmt.Errorf("%w: lease %q root checksum mismatch", ErrBackupTokenInvalid, record.LeaseID)
	}
	return &Backup{
		store:         db.store,
		manifestStore: db.manifestStore,
		now:           db.backupNow(),
		lease:         record,
		root:          root,
	}, nil
}

func backupLeaseIdentifier(owner, idempotencyKey string) (string, error) {
	key := strings.TrimSpace(idempotencyKey)
	if key == "" {
		return ksuid.New().String(), nil
	}
	if len(key) > 256 {
		return "", fmt.Errorf("%w: idempotency key too long", ErrBackupTokenInvalid)
	}
	sum := sha256.Sum256([]byte("isledb-backup-lease-v1\x00" + owner + "\x00" + key))
	return hex.EncodeToString(sum[:]), nil
}
