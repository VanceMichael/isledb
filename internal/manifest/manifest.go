package manifest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/ksuid"
)

type Manifest struct {
	Version   int    `json:"version"`
	NextEpoch uint64 `json:"next_epoch"`
	LogSeq    uint64 `json:"log_seq"`

	WriterFence    *FenceToken `json:"writer_fence,omitempty"`
	CompactorFence *FenceToken `json:"compactor_fence,omitempty"`

	L0SSTs []SSTMeta `json:"l0_ssts,omitempty"`
	Levels []Level   `json:"levels,omitempty"`
}

type FenceToken struct {
	Epoch     uint64    `json:"epoch"`
	Owner     string    `json:"owner"`
	ClaimedAt time.Time `json:"claimed_at"`
}

// Level is one non-overlapping, key-sorted level. Number starts at 1; L0 is
// represented separately because its SSTs may overlap.
type Level struct {
	Number uint32    `json:"number"`
	SSTs   []SSTMeta `json:"ssts,omitempty"`
}

// BloomFormatExactV1 identifies the exact-size Bloom filter sidecar. Readers
// use only filters in this format; any other format, including the JSON
// sidecars written by earlier versions, is skipped without being fetched.
const BloomFormatExactV1 = "exact-v1"

type BloomMeta struct {
	Format     string `json:"format,omitempty"`
	BitsPerKey int    `json:"bits_per_key"`
	K          int    `json:"k"`
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
	Checksum   string `json:"checksum,omitempty"`
}

type SSTMeta struct {
	ID        string    `json:"id"`
	Epoch     uint64    `json:"epoch"`
	SeqLo     uint64    `json:"seq_lo"`
	SeqHi     uint64    `json:"seq_hi"`
	MinKey    []byte    `json:"min_key"`
	MaxKey    []byte    `json:"max_key"`
	Size      int64     `json:"size"`
	Checksum  string    `json:"checksum"`
	Bloom     BloomMeta `json:"bloom"`
	CreatedAt time.Time `json:"created_at"`

	// MetaOffset is where the SST's trailing metadata begins: the filter,
	// index, properties, metaindex and footer all live in [MetaOffset, Size).
	// Readers use it to fetch that region in one request. Zero means unknown.
	MetaOffset int64 `json:"meta_offset,omitempty"`

	// Level records the logical placement committed with this metadata. L0 is
	// zero; compacted levels start at one.
	Level uint32 `json:"level"`
}

// ChangeFeedPayload identifies which PUT payload is retained in committed
// change batches. The feed itself remains optional; an empty value means it is
// disabled in CURRENT.
type ChangeFeedPayload string

const (
	ChangeFeedPayloadKeysOnly   ChangeFeedPayload = "keys_only"
	ChangeFeedPayloadFullValues ChangeFeedPayload = "full_values"
)

func (p ChangeFeedPayload) Valid() bool {
	return p == ChangeFeedPayloadKeysOnly || p == ChangeFeedPayloadFullValues
}

// ChangeBatchMeta describes one committed, block-indexed, seq-ordered mutation
// batch emitted alongside a memtable flush. The object is visible only after
// the manifest entry that references it is committed.
type ChangeBatchMeta struct {
	ID            string            `json:"id"`
	Path          string            `json:"path"`
	Epoch         uint64            `json:"epoch"`
	SeqLo         uint64            `json:"seq_lo"`
	SeqHi         uint64            `json:"seq_hi"`
	Count         uint32            `json:"count"`
	BlockCount    uint32            `json:"block_count"`
	Size          int64             `json:"size"`
	RawSize       int64             `json:"raw_size"`
	Checksum      string            `json:"checksum"`
	IndexChecksum string            `json:"index_checksum"`
	CreatedAt     time.Time         `json:"created_at"`
	Version       int               `json:"version,omitempty"`
	Compression   string            `json:"compression,omitempty"`
	Payload       ChangeFeedPayload `json:"payload"`
}

// WriterCommit is one logical memtable publication. ID remains unchanged when
// SST upload or manifest publication is retried.
type WriterCommit struct {
	ID          string           `json:"id"`
	SSTable     SSTMeta          `json:"sstable"`
	ChangeBatch *ChangeBatchMeta `json:"change_batch,omitempty"`
}

// WriterCommitMarker is the bounded idempotency receipt retained in CURRENT.
// Maintenance updates preserve it even when the committed SST is compacted.
type WriterCommitMarker struct {
	CommitID    string      `json:"commit_id"`
	Fingerprint string      `json:"fingerprint"`
	EntryID     ksuid.KSUID `json:"entry_id"`
	ManifestSeq uint64      `json:"manifest_seq"`
	WriterEpoch uint64      `json:"writer_epoch"`
	SeqLo       uint64      `json:"seq_lo"`
	SeqHi       uint64      `json:"seq_hi"`
	CommittedAt time.Time   `json:"committed_at"`
}

type Current struct {
	LayoutVersion int        `json:"layout_version,omitempty"`
	Format        string     `json:"format,omitempty"`
	Snapshot      *ObjectRef `json:"snapshot,omitempty"`
	LogSeqStart   uint64     `json:"log_seq_start,omitempty"`
	NextSeq       uint64     `json:"next_seq"`
	NextEpoch     uint64     `json:"next_epoch"`

	ChangeFeedEnabled    bool              `json:"change_feed_enabled,omitempty"`
	ChangeFeedPayload    ChangeFeedPayload `json:"change_feed_payload,omitempty"`
	ChangeFeedLogStart   uint64            `json:"change_feed_log_start,omitempty"`
	StateReplayPages     uint64            `json:"state_replay_pages,omitempty"`
	StateReplayBytes     uint64            `json:"state_replay_bytes,omitempty"`
	ManifestPageMaxLevel uint8             `json:"manifest_page_max_level,omitempty"`
	MaxPinnedViewAge     time.Duration     `json:"max_pinned_view_age_nanos"`

	ActiveEntries []ManifestLogEntry `json:"active_entries,omitempty"`
	IndexFrontier []PageRef          `json:"index_frontier,omitempty"`

	WriterFence          *FenceToken               `json:"writer_fence,omitempty"`
	CompactorFence       *FenceToken               `json:"compactor_fence,omitempty"`
	LastWriterCommit     *WriterCommitMarker       `json:"last_writer_commit,omitempty"`
	MaintenanceReceipt   *MaintenanceReceipt       `json:"maintenance_receipt,omitempty"`
	MaintenanceScheduler MaintenanceSchedulerState `json:"maintenance_scheduler,omitempty"`

	// Lifecycle is empty for every active database. LifecycleDestroyed marks
	// the irreversible terminal state committed before any destruction sweep.
	// A terminal CURRENT is itself the retained tombstone: it is never deleted,
	// so a later Open cannot mistake the prefix for a new database.
	Lifecycle   LifecycleState     `json:"lifecycle,omitempty"`
	Destruction *DestructionRecord `json:"destruction,omitempty"`
}

// LifecycleState is the database lifecycle phase recorded in CURRENT.
type LifecycleState string

const (
	// LifecycleActive is the zero value for every normal database.
	LifecycleActive LifecycleState = ""
	// LifecycleDestroyed is irreversible. All opens, fence claims, and
	// publications fail with ErrDBDestroyed once CURRENT carries this value.
	LifecycleDestroyed LifecycleState = "destroyed"
)

// Valid reports whether the lifecycle value is a recognized state.
func (l LifecycleState) Valid() bool {
	switch l {
	case LifecycleActive, LifecycleDestroyed:
		return true
	default:
		return false
	}
}

// DestructionRecord is the terminal marker and durable sweep progress carried
// by a destroyed CURRENT. The terminal commit is the only operation that
// creates it; sweep progress updates only advance its cursor and Swept flag.
type DestructionRecord struct {
	// DestroyedAt is when the terminal CURRENT was committed.
	DestroyedAt time.Time `json:"destroyed_at"`
	// PinnedViewAge is copied from CURRENT.MaxPinnedViewAge at the terminal
	// commit. Physical data deletion waits for DestroyedAt + PinnedViewAge so
	// already-loaded pinned views can finish within their existing boundary.
	PinnedViewAge time.Duration `json:"pinned_view_age_nanos"`
	// SweepCursor is the full object key of the last key whose deletion batch
	// completed. Recovery restarts the listing from the prefix root but skips
	// every key at or below this lexicographic lower bound. It is only a
	// conservative resume marker: object deletion is idempotent.
	SweepCursor string `json:"sweep_cursor,omitempty"`
	// Swept is true only after a complete prefix listing found no remaining
	// object other than the terminal CURRENT.
	Swept bool `json:"swept,omitempty"`
	// SweptAt records when Swept first became true.
	SweptAt time.Time `json:"swept_at,omitempty"`
}

// NotBefore is the earliest time physical data objects may be deleted.
func (r *DestructionRecord) NotBefore() time.Time {
	if r == nil {
		return time.Time{}
	}
	return r.DestroyedAt.Add(r.PinnedViewAge)
}

// Destroyed reports whether CURRENT is in the irreversible terminal state.
func (c *Current) Destroyed() bool {
	return c != nil && c.Lifecycle == LifecycleDestroyed
}

// MaintenanceSchedulerState is advisory control-plane state. It affects work
// ordering but never logical KV contents.
type MaintenanceSchedulerState struct {
	LastPrimary                    MaintenanceCommandKind `json:"last_primary,omitempty"`
	CompactionUnitsSinceCheckpoint uint32                 `json:"compaction_units_since_checkpoint,omitempty"`
	L0UnitsSinceLower              uint32                 `json:"l0_units_since_lower,omitempty"`
	NextLowerLevel                 uint32                 `json:"next_lower_level,omitempty"`
}

type ObjectRef struct {
	Path         string    `json:"path"`
	EncodedBytes uint64    `json:"encoded_bytes"`
	Checksum     string    `json:"checksum"`
	CreatedAt    time.Time `json:"created_at"`
}

type PageRef struct {
	ObjectRef
	Level uint8  `json:"level"`
	SeqLo uint64 `json:"seq_lo"`
	SeqHi uint64 `json:"seq_hi"`
	Count uint32 `json:"count"`
}

type CommitPage struct {
	LayoutVersion int                `json:"layout_version"`
	PageType      string             `json:"page_type"`
	Level         uint8              `json:"level"`
	SeqLo         uint64             `json:"seq_lo"`
	SeqHi         uint64             `json:"seq_hi"`
	Count         uint32             `json:"count"`
	Entries       []ManifestLogEntry `json:"entries,omitempty"`
	Children      []PageRef          `json:"children,omitempty"`
	CreatedAt     time.Time          `json:"created_at"`
}

func EncodeSnapshot(m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil manifest snapshot", ErrInvalidManifest)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return encodeManifestObject(raw, manifestObjectKindSnapshot, maxManifestSnapshotRawBytes)
}

func DecodeSnapshot(data []byte) (*Manifest, error) {
	raw, err := decodeManifestObject(data, manifestObjectKindSnapshot, maxManifestSnapshotRawBytes)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func EncodeCurrent(c *Current) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: nil CURRENT", ErrInvalidManifest)
	}
	if c.MaxPinnedViewAge < 0 {
		return nil, fmt.Errorf("%w: max_pinned_view_age=%s", ErrInvalidManifest, c.MaxPinnedViewAge)
	}
	// New in-memory CURRENT values have no persisted format yet. Stamp a shallow
	// encoding copy; production write paths are already normalized and do not
	// pay for this branch. Decoding remains strict and rejects an on-disk v1.
	encoded := c
	if c.LayoutVersion == 0 || c.Format == "" {
		clone := *c
		if clone.LayoutVersion == 0 {
			clone.LayoutVersion = LayoutVersion
		}
		if clone.Format == "" {
			clone.Format = CurrentFormat
		}
		encoded = &clone
	}
	if err := validateCurrentFormat(encoded); err != nil {
		return nil, err
	}
	if err := validateLifecycle(encoded); err != nil {
		return nil, err
	}
	return json.Marshal(encoded)
}

func DecodeCurrent(data []byte) (*Current, error) {
	var c Current
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.MaxPinnedViewAge < 0 {
		return nil, fmt.Errorf("%w: max_pinned_view_age=%s", ErrInvalidManifest, c.MaxPinnedViewAge)
	}
	if err := validateCurrentFormat(&c); err != nil {
		return nil, err
	}
	if err := validateLifecycle(&c); err != nil {
		return nil, err
	}
	if err := validateCurrentRefs(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// validateLifecycle enforces the terminal-state invariant: a destroyed
// CURRENT must carry a complete destruction record with a positive safety
// window, and an active CURRENT must not carry one.
func validateLifecycle(c *Current) error {
	if !c.Lifecycle.Valid() {
		return fmt.Errorf("%w: unknown lifecycle=%q", ErrInvalidManifest, c.Lifecycle)
	}
	switch c.Lifecycle {
	case LifecycleActive:
		if c.Destruction != nil {
			return fmt.Errorf("%w: active CURRENT carries a destruction record", ErrInvalidManifest)
		}
	case LifecycleDestroyed:
		r := c.Destruction
		if r == nil {
			return fmt.Errorf("%w: destroyed CURRENT is missing its destruction record", ErrInvalidManifest)
		}
		if r.DestroyedAt.IsZero() {
			return fmt.Errorf("%w: destroyed CURRENT has zero destroyed_at", ErrInvalidManifest)
		}
		if r.PinnedViewAge <= 0 {
			return fmt.Errorf("%w: destroyed CURRENT has non-positive pinned_view_age=%s",
				ErrInvalidManifest, r.PinnedViewAge)
		}
		if r.Swept && r.SweptAt.IsZero() {
			return fmt.Errorf("%w: swept CURRENT has zero swept_at", ErrInvalidManifest)
		}
	}
	return nil
}

func validateCurrentFormat(c *Current) error {
	if c == nil || c.LayoutVersion != LayoutVersion || c.Format != CurrentFormat {
		if c == nil {
			return fmt.Errorf("%w: nil CURRENT", ErrInvalidManifest)
		}
		return fmt.Errorf("%w: CURRENT layout=%d format=%q", ErrInvalidManifest, c.LayoutVersion, c.Format)
	}
	return nil
}

func validateCurrentRefs(c *Current) error {
	if c.Snapshot != nil {
		if err := validateManifestObjectRef(*c.Snapshot, manifestObjectKindSnapshot); err != nil {
			return err
		}
	}
	for i := range c.IndexFrontier {
		if err := validatePageRef(c.IndexFrontier[i]); err != nil {
			return err
		}
	}
	return nil
}
