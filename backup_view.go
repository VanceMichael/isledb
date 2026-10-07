package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
)

const (
	backupNamespacePrefix = "manifest/backup"
	backupViewsPrefix     = "manifest/backup/views"
	backupLeasesPrefix    = "manifest/backup/leases"

	backupViewVersion = 1
	backupViewKind    = "isledb_backup_view_v1"

	// backupManifestChunkEntries fixes the deterministic chunk boundary. A
	// fixed boundary is what lets an interrupted or concurrent view build
	// converge byte-for-byte on the same chunk objects: the manifest for a
	// pinned generation must never depend on caller-supplied page sizes.
	backupManifestChunkEntries = 1000
	backupChunkMaxEncodedBytes = 16 << 20
	backupRootMaxEncodedBytes  = 8 << 20
)

// Backup object families. The order is also the stable listing order used by
// Backup.ListObjects after the pinned CURRENT entry.
const (
	backupFamilySnapshot = "manifest_snapshot"
	backupFamilyPage     = "manifest_page"
	backupFamilySST      = "sstable"
	backupFamilyChange   = "change_batch"
)

var backupFamilyOrder = []string{
	backupFamilySnapshot,
	backupFamilyPage,
	backupFamilySST,
	backupFamilyChange,
}

// backupObjectEntry is one stable manifest row. Every row carries the exact
// object key plus size and/or checksum information sufficient to verify a
// copied object.
type backupObjectEntry struct {
	// Kind is one of the backup family constants plus "current".
	Kind string `json:"kind"`
	// Path is the exact object key the backup agent must copy.
	Path string `json:"path"`
	// LogicalPath is set only for the pinned CURRENT row: restore must place
	// the copied bytes back at manifest/CURRENT rather than under the internal
	// pinned-copy location.
	LogicalPath string `json:"logical_path,omitempty"`
	// Size is the stored-object length for SST and change-batch objects.
	Size int64 `json:"size,omitempty"`
	// EncodedBytes is the exact encoded length of a manifest object.
	EncodedBytes uint64 `json:"encoded_bytes,omitempty"`
	// Checksum is "sha256:" plus 64 lowercase hex digits when the generation
	// recorded one.
	Checksum string `json:"checksum,omitempty"`
}

type backupObjectChunk struct {
	Version  int                 `json:"version"`
	Kind     string              `json:"kind"`
	ViewID   string              `json:"view_id"`
	Family   string              `json:"family"`
	Index    int                 `json:"index"`
	First    string              `json:"first"`
	Last     string              `json:"last"`
	Count    int                 `json:"count"`
	Checksum string              `json:"checksum"`
	Entries  []backupObjectEntry `json:"entries"`
}

type backupChunkRef struct {
	Family       string `json:"family"`
	Index        int    `json:"index"`
	Path         string `json:"path"`
	First        string `json:"first"`
	Last         string `json:"last"`
	Count        int    `json:"count"`
	EncodedBytes uint64 `json:"encoded_bytes"`
	Checksum     string `json:"checksum"`
}

type backupFamilyIndex struct {
	Family string           `json:"family"`
	Chunks []backupChunkRef `json:"chunks"`
	Count  int              `json:"count"`
}

type backupViewRoot struct {
	Version    int                 `json:"version"`
	Kind       string              `json:"kind"`
	ViewID     string              `json:"view_id"`
	Current    backupObjectEntry   `json:"current"`
	CreatedAt  time.Time           `json:"created_at"`
	EntryCount int                 `json:"entry_count"`
	TotalBytes int64               `json:"total_bytes"`
	Families   []backupFamilyIndex `json:"families,omitempty"`
	Checksum   string              `json:"checksum"`
}

func backupViewDir(viewID string) string {
	return backupViewsPrefix + "/" + viewID
}

func backupPinnedCurrentPath(viewID string) string {
	return backupViewDir(viewID) + "/current"
}

func backupRootPath(viewID string) string {
	return backupViewDir(viewID) + "/root.json"
}

func backupChunkPath(viewID, family string, index int) string {
	return fmt.Sprintf("%s/chunks/%s/%05d.json", backupViewDir(viewID), family, index)
}

func backupLeasePath(leaseID string) string {
	return backupLeasesPrefix + "/" + leaseID + ".json"
}

// backupFamilyForKey returns the backup family for a physical data object, or
// "" when the key is not a reclaimable data object (GC records, the backup
// namespace itself, maintenance mailboxes). dbPrefix is the database's object
// store prefix; reclamation plans always carry fully-qualified keys.
func backupFamilyForKey(dbPrefix, key string) string {
	switch {
	case hasPathPrefix(key, backupFamilyPrefix(dbPrefix, "sstable")):
		return backupFamilySST
	case hasPathPrefix(key, backupFamilyPrefix(dbPrefix, "changes")):
		return backupFamilyChange
	case hasPathPrefix(key, backupFamilyPrefix(dbPrefix, "manifest/snapshots")):
		return backupFamilySnapshot
	case hasPathPrefix(key, backupFamilyPrefix(dbPrefix, "manifest/pages")):
		return backupFamilyPage
	default:
		return ""
	}
}

func backupFamilyPrefix(dbPrefix, relative string) string {
	if dbPrefix == "" {
		return relative + "/"
	}
	return dbPrefix + "/" + relative + "/"
}

func hasPathPrefix(key, prefix string) bool {
	if len(key) < len(prefix) {
		return false
	}
	return key[:len(prefix)] == prefix
}

// captureBackupView reads CURRENT exactly once (the caller supplies the raw
// bytes), pins that generation, and materializes its deterministic object
// manifest. All writes are create-if-not-exist with content validation, so
// retrying after a timeout or an unknown commit result, including from a
// restarted process, converges on the same objects.
func captureBackupView(
	ctx context.Context,
	store *blobstore.Store,
	manifestStore *manifest.Store,
	currentBytes []byte,
	now time.Time,
) (string, *backupViewRoot, error) {
	if len(currentBytes) == 0 {
		return "", nil, errors.New("backup view: empty CURRENT bytes")
	}
	current, err := manifest.DecodeCurrent(currentBytes)
	if err != nil {
		return "", nil, fmt.Errorf("backup view: decode pinned CURRENT: %w", err)
	}
	sum := sha256.Sum256(currentBytes)
	viewID := hex.EncodeToString(sum[:])

	pinnedPath := backupPinnedCurrentPath(viewID)
	if err := putImmutableConverge(ctx, store, pinnedPath, currentBytes, sum[:]); err != nil {
		return "", nil, err
	}

	graph, err := manifestStore.DescribeGeneration(ctx, current)
	if err != nil {
		return "", nil, err
	}

	currentEntry := backupObjectEntry{
		Kind:         "current",
		Path:         storeKey(store, pinnedPath),
		LogicalPath:  store.ManifestPath(),
		EncodedBytes: uint64(len(currentBytes)),
		Checksum:     "sha256:" + viewID,
	}

	root := &backupViewRoot{
		Version:   backupViewVersion,
		Kind:      backupViewKind,
		ViewID:    viewID,
		Current:   currentEntry,
		CreatedAt: now.UTC(),
	}
	root.EntryCount = 1 // the pinned CURRENT row

	addFamily := func(family string, entries []backupObjectEntry) error {
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Path == entries[j].Path {
				return entries[i].Kind < entries[j].Kind
			}
			return entries[i].Path < entries[j].Path
		})
		entries = dedupBackupEntries(entries)
		if len(entries) == 0 {
			return nil
		}
		index := backupFamilyIndex{Family: family, Count: len(entries)}
		for start := 0; start < len(entries); start += backupManifestChunkEntries {
			end := start + backupManifestChunkEntries
			if end > len(entries) {
				end = len(entries)
			}
			chunkEntries := entries[start:end]
			chunk := &backupObjectChunk{
				Version: backupViewVersion,
				Kind:    backupViewKind,
				ViewID:  viewID,
				Family:  family,
				Index:   len(index.Chunks),
				First:   chunkEntries[0].Path,
				Last:    chunkEntries[len(chunkEntries)-1].Path,
				Count:   len(chunkEntries),
				Entries: chunkEntries,
			}
			chunk.Checksum = backupChunkChecksum(chunk)
			payload, err := json.Marshal(chunk)
			if err != nil {
				return err
			}
			if len(payload) > backupChunkMaxEncodedBytes {
				return fmt.Errorf("backup view: chunk bytes=%d exceeds limit=%d", len(payload), backupChunkMaxEncodedBytes)
			}
			chunkPath := backupChunkPath(viewID, family, chunk.Index)
			ref := backupChunkRef{
				Family:       family,
				Index:        chunk.Index,
				Path:         storeKey(store, chunkPath),
				First:        chunk.First,
				Last:         chunk.Last,
				Count:        chunk.Count,
				EncodedBytes: uint64(len(payload)),
				Checksum:     chunk.Checksum,
			}
			if err := putChunkConverge(ctx, store, chunkPath, payload, chunk); err != nil {
				return err
			}
			index.Chunks = append(index.Chunks, ref)
		}
		root.Families = append(root.Families, index)
		root.EntryCount += len(entries)
		return nil
	}

	if graph.Snapshot != nil {
		if err := addFamily(backupFamilySnapshot, []backupObjectEntry{{
			Kind:         backupFamilySnapshot,
			Path:         graph.Snapshot.Path,
			EncodedBytes: graph.Snapshot.EncodedBytes,
			Checksum:     graph.Snapshot.Checksum,
		}}); err != nil {
			return "", nil, err
		}
	}
	pageEntries := make([]backupObjectEntry, 0, len(graph.Pages))
	var pageBytes int64
	for i := range graph.Pages {
		ref := graph.Pages[i]
		pageEntries = append(pageEntries, backupObjectEntry{
			Kind:         backupFamilyPage,
			Path:         ref.Path,
			EncodedBytes: ref.EncodedBytes,
			Checksum:     ref.Checksum,
		})
		pageBytes += int64(ref.EncodedBytes)
	}
	sstEntries := make([]backupObjectEntry, 0, len(graph.SSTs))
	for i := range graph.SSTs {
		sst := graph.SSTs[i]
		sstEntries = append(sstEntries, backupObjectEntry{
			Kind:     backupFamilySST,
			Path:     store.SSTPath(sst.ID),
			Size:     sst.Size,
			Checksum: sst.Checksum,
		})
	}
	changeEntries := make([]backupObjectEntry, 0, len(graph.ChangeBatches))
	for i := range graph.ChangeBatches {
		batch := graph.ChangeBatches[i]
		changeEntries = append(changeEntries, backupObjectEntry{
			Kind:     backupFamilyChange,
			Path:     batch.Path,
			Size:     batch.Size,
			Checksum: batch.Checksum,
		})
	}
	for _, build := range []struct {
		family  string
		entries []backupObjectEntry
	}{
		{backupFamilyPage, pageEntries},
		{backupFamilySST, sstEntries},
		{backupFamilyChange, changeEntries},
	} {
		if err := addFamily(build.family, build.entries); err != nil {
			return "", nil, err
		}
	}
	root.TotalBytes = int64(len(currentBytes)) + pageBytes
	for i := range sstEntries {
		root.TotalBytes += sstEntries[i].Size
	}
	for i := range changeEntries {
		root.TotalBytes += changeEntries[i].Size
	}
	root.Checksum = backupRootChecksum(root)

	payload, err := json.Marshal(root)
	if err != nil {
		return "", nil, err
	}
	if len(payload) > backupRootMaxEncodedBytes {
		return "", nil, fmt.Errorf("backup view: root bytes=%d exceeds limit=%d", len(payload), backupRootMaxEncodedBytes)
	}
	stored, err := putRootConverge(ctx, store, backupRootPath(viewID), payload, viewID)
	if err != nil {
		return "", nil, err
	}
	return viewID, stored, nil
}

func dedupBackupEntries(entries []backupObjectEntry) []backupObjectEntry {
	if len(entries) <= 1 {
		return entries
	}
	out := entries[:0]
	for i := range entries {
		if len(out) > 0 && out[len(out)-1].Path == entries[i].Path {
			continue
		}
		out = append(out, entries[i])
	}
	return out
}

func backupChunkChecksum(chunk *backupObjectChunk) string {
	canonical := struct {
		Kind    string              `json:"kind"`
		ViewID  string              `json:"view_id"`
		Family  string              `json:"family"`
		Index   int                 `json:"index"`
		Entries []backupObjectEntry `json:"entries"`
	}{Kind: chunk.Kind, ViewID: chunk.ViewID, Family: chunk.Family, Index: chunk.Index, Entries: chunk.Entries}
	payload, err := json.Marshal(canonical)
	if err != nil {
		panic(fmt.Sprintf("marshal backup chunk checksum: %v", err))
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func backupRootChecksum(root *backupViewRoot) string {
	canonical := struct {
		Kind       string              `json:"kind"`
		ViewID     string              `json:"view_id"`
		Current    backupObjectEntry   `json:"current"`
		EntryCount int                 `json:"entry_count"`
		TotalBytes int64               `json:"total_bytes"`
		Families   []backupFamilyIndex `json:"families"`
	}{
		Kind:       root.Kind,
		ViewID:     root.ViewID,
		Current:    root.Current,
		EntryCount: root.EntryCount,
		TotalBytes: root.TotalBytes,
		Families:   root.Families,
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		panic(fmt.Sprintf("marshal backup root checksum: %v", err))
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// putImmutableConverge writes once and adopts an identical object left by a
// prior attempt. A same-path object with different content is corruption.
func putImmutableConverge(ctx context.Context, store *blobstore.Store, key string, data []byte, wantSHA []byte) error {
	fullKey := storeKey(store, key)
	if _, err := store.WriteIfNotExist(ctx, fullKey, data); err == nil {
		return nil
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return err
	}
	existing, _, err := store.Read(ctx, fullKey)
	if err != nil {
		return fmt.Errorf("verify existing backup object %q: %w", fullKey, err)
	}
	sum := sha256.Sum256(existing)
	if !bytes.Equal(sum[:], wantSHA) {
		return fmt.Errorf("backup object %q content collision", fullKey)
	}
	return nil
}

func putChunkConverge(
	ctx context.Context,
	store *blobstore.Store,
	key string,
	payload []byte,
	chunk *backupObjectChunk,
) error {
	fullKey := storeKey(store, key)
	if _, err := store.WriteIfNotExist(ctx, fullKey, payload); err == nil {
		return nil
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return err
	}
	existing, err := loadBackupChunk(ctx, store, fullKey)
	if err != nil {
		return err
	}
	if existing.ViewID != chunk.ViewID || existing.Family != chunk.Family ||
		existing.Index != chunk.Index || existing.Checksum != chunk.Checksum {
		return fmt.Errorf("backup chunk %q content collision", fullKey)
	}
	return nil
}

func putRootConverge(
	ctx context.Context,
	store *blobstore.Store,
	key string,
	payload []byte,
	viewID string,
) (*backupViewRoot, error) {
	fullKey := storeKey(store, key)
	if _, err := store.WriteIfNotExist(ctx, fullKey, payload); err == nil {
		return decodeBackupRoot(store, fullKey, payload)
	} else if !errors.Is(err, blobstore.ErrPreconditionFailed) {
		return nil, err
	}
	existing, _, err := store.Read(ctx, fullKey)
	if err != nil {
		return nil, fmt.Errorf("read existing backup root %q: %w", fullKey, err)
	}
	root, err := decodeBackupRoot(store, fullKey, existing)
	if err != nil {
		return nil, err
	}
	if root.ViewID != viewID {
		return nil, fmt.Errorf("backup root %q view collision", fullKey)
	}
	return root, nil
}

func decodeBackupRoot(store *blobstore.Store, path string, payload []byte) (*backupViewRoot, error) {
	if len(payload) == 0 || len(payload) > backupRootMaxEncodedBytes {
		return nil, fmt.Errorf("invalid backup root bytes=%d path=%q", len(payload), path)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var root backupViewRoot
	if err := decoder.Decode(&root); err != nil {
		return nil, fmt.Errorf("decode backup root %q: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("backup root %q has trailing JSON", path)
		}
		return nil, fmt.Errorf("decode backup root %q trailer: %w", path, err)
	}
	if err := validateBackupRoot(store, &root, path); err != nil {
		return nil, err
	}
	return &root, nil
}

func validateBackupRoot(store *blobstore.Store, root *backupViewRoot, path string) error {
	if root.Version != backupViewVersion || root.Kind != backupViewKind {
		return fmt.Errorf("unsupported backup root version=%d kind=%q path=%q", root.Version, root.Kind, path)
	}
	if root.ViewID == "" || len(root.ViewID) != sha256.Size*2 {
		return fmt.Errorf("invalid backup view id path=%q", path)
	}
	if root.Current.Path == "" || root.Current.LogicalPath != store.ManifestPath() ||
		root.Current.EncodedBytes == 0 || root.Current.Checksum != "sha256:"+root.ViewID {
		return fmt.Errorf("invalid backup CURRENT row path=%q", path)
	}
	wantPath := storeKey(store, backupRootPath(root.ViewID))
	if path != wantPath {
		return fmt.Errorf("backup root path mismatch %q want %q", path, wantPath)
	}
	checksum := backupRootChecksum(root)
	if root.Checksum != checksum {
		return fmt.Errorf("backup root %q checksum mismatch", path)
	}
	count := 1
	seenFamilies := make(map[string]struct{}, len(root.Families))
	for fi := range root.Families {
		family := &root.Families[fi]
		if !backupKnownFamily(family.Family) {
			return fmt.Errorf("backup root %q unknown family %q", path, family.Family)
		}
		if _, ok := seenFamilies[family.Family]; ok {
			return fmt.Errorf("backup root %q duplicate family %q", path, family.Family)
		}
		seenFamilies[family.Family] = struct{}{}
		if family.Count <= 0 || len(family.Chunks) == 0 {
			return fmt.Errorf("backup root %q empty family %q", path, family.Family)
		}
		var previous string
		for ci := range family.Chunks {
			ref := &family.Chunks[ci]
			if ref.Family != family.Family || ref.Index != ci || ref.Path == "" ||
				ref.Count <= 0 || ref.EncodedBytes == 0 || ref.Checksum == "" ||
				ref.First == "" || ref.Last == "" || ref.First > ref.Last {
				return fmt.Errorf("invalid backup chunk ref path=%q family=%q index=%d", path, family.Family, ci)
			}
			if ref.Path != storeKey(store, backupChunkPath(root.ViewID, family.Family, ci)) {
				return fmt.Errorf("backup chunk path mismatch %q", ref.Path)
			}
			if ci > 0 && ref.First <= previous {
				return fmt.Errorf("backup chunk ranges out of order path=%q family=%q", path, family.Family)
			}
			previous = ref.Last
			count += ref.Count
		}
	}
	if count != root.EntryCount {
		return fmt.Errorf("backup root %q entry count=%d want=%d", path, root.EntryCount, count)
	}
	return nil
}

func backupKnownFamily(family string) bool {
	for _, known := range backupFamilyOrder {
		if family == known {
			return true
		}
	}
	return false
}

func loadBackupChunk(ctx context.Context, store *blobstore.Store, path string) (*backupObjectChunk, error) {
	data, _, err := store.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read backup chunk %q: %w", path, err)
	}
	if len(data) == 0 || len(data) > backupChunkMaxEncodedBytes {
		return nil, fmt.Errorf("invalid backup chunk bytes=%d path=%q", len(data), path)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var chunk backupObjectChunk
	if err := decoder.Decode(&chunk); err != nil {
		return nil, fmt.Errorf("decode backup chunk %q: %w", path, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("backup chunk %q has trailing JSON", path)
		}
		return nil, fmt.Errorf("decode backup chunk %q trailer: %w", path, err)
	}
	if chunk.Version != backupViewVersion || chunk.Kind != backupViewKind ||
		chunk.ViewID == "" || !backupKnownFamily(chunk.Family) || chunk.Count != len(chunk.Entries) ||
		len(chunk.Entries) == 0 {
		return nil, fmt.Errorf("invalid backup chunk header path=%q", path)
	}
	if chunk.Checksum != backupChunkChecksum(&chunk) {
		return nil, fmt.Errorf("backup chunk %q checksum mismatch", path)
	}
	for i := range chunk.Entries {
		entry := &chunk.Entries[i]
		if entry.Kind != chunk.Family || entry.Path == "" {
			return nil, fmt.Errorf("invalid backup chunk entry path=%q index=%d", path, i)
		}
		if i > 0 && entry.Path < chunk.Entries[i-1].Path {
			return nil, fmt.Errorf("backup chunk %q entries not sorted", path)
		}
		if i == 0 && entry.Path != chunk.First {
			return nil, fmt.Errorf("backup chunk %q first-key mismatch", path)
		}
		if i == len(chunk.Entries)-1 && entry.Path != chunk.Last {
			return nil, fmt.Errorf("backup chunk %q last-key mismatch", path)
		}
	}
	return &chunk, nil
}

// loadBackupViewRoot reads and validates a stored view root.
func loadBackupViewRoot(ctx context.Context, store *blobstore.Store, viewID string) (*backupViewRoot, error) {
	path := storeKey(store, backupRootPath(viewID))
	data, _, err := store.Read(ctx, path)
	if err != nil {
		return nil, err
	}
	return decodeBackupRoot(store, path, data)
}
