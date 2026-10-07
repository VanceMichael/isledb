package isledb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ankur-anand/isledb/blobstore"
	"github.com/ankur-anand/isledb/internal/manifest"
	"gocloud.dev/blob"
)

// ErrDatabaseDestroyed is the stable terminal error returned by every open,
// handle creation, refresh, and publication path of a destroyed database
// prefix. The prefix can never return to active service; callers must not
// retry these operations against the same prefix.
var ErrDatabaseDestroyed = manifest.ErrDBDestroyed

// destroySweepBatch bounds both listing pages and batch-delete requests used
// by the destruction sweep.
const destroySweepBatch = 1000

// destructionObjectStore is the narrow blob-store seam used by the sweep. It
// matches *blobstore.Store and lets tests inject listing/delete faults.
type destructionObjectStore interface {
	ManifestPath() string
	ListPage(
		ctx context.Context,
		pageToken []byte,
		pageSize int,
		opts blobstore.ListOptions,
	) (*blobstore.ListResult, []byte, error)
	BatchDelete(ctx context.Context, keys []string) error
}

// DestroyOptions configures a database destruction request. Only the bucket
// prefix is relevant: destruction never reads or changes writer, change-feed,
// or SST output options.
type DestroyOptions struct {
	// Prefix is the database root path inside the bucket or container.
	Prefix string
}

// DestroyResult reports the durable phase of one destruction call.
type DestroyResult struct {
	// Terminal is true once CURRENT has committed the irreversible destroyed
	// state. It never returns to false for the same prefix.
	Terminal bool
	// Swept is true only when the prefix contains no object other than the
	// terminal CURRENT marker.
	Swept bool
	// RetryAfter is positive while the sweep is waiting for the pinned-view
	// safety window committed in CURRENT. Repeat the same Destroy call after
	// this delay; the terminal state and all sweep progress are already
	// durable, so the repeat performs no new publication.
	RetryAfter time.Duration
}

// Destroy irreversibly destroys the database rooted at opts.Prefix in
// bucketURL.
//
// Destruction is a two-phase, resumable operation:
//
//  1. One conditional update commits CURRENT to the destroyed terminal state
//     and simultaneously invalidates every earlier writer/compactor fence.
//     Only after that commit is acknowledged can any object be deleted.
//  2. Control objects (maintenance HEAD and GC records) are removed at once;
//     SSTs, change batches, and manifest snapshots/pages are deleted in
//     bounded batches only after the pinned-view safety window recorded in
//     CURRENT elapses. The terminal CURRENT is retained forever as the
//     tombstone.
//
// The call is idempotent. A lost terminal-CAS response is recognized by
// re-reading CURRENT; interrupted listings or batch deletes resume from the
// cursor persisted in the terminal CURRENT. When the safety window has not
// elapsed, the call returns RetryAfter with a nil error rather than blocking.
//
// Destroy fails closed without writing anything when the prefix is
// uninitialized or CURRENT is missing/corrupt; use Open for those cases.
func Destroy(ctx context.Context, bucketURL string, opts DestroyOptions) (DestroyResult, error) {
	store, err := blobstore.Open(ctx, bucketURL, opts.Prefix)
	if err != nil {
		return DestroyResult{}, err
	}
	result, err := destroyPrefix(ctx, store)
	closeErr := store.Close()
	if err == nil {
		err = closeErr
	}
	return result, err
}

// DestroyBucket destroys a database over an existing Go Cloud bucket. It does
// not close bucket; its lifecycle remains owned by the caller.
func DestroyBucket(
	ctx context.Context,
	bucket *blob.Bucket,
	bucketName string,
	opts DestroyOptions,
) (DestroyResult, error) {
	if bucket == nil {
		return DestroyResult{}, fmt.Errorf("%w: nil bucket", ErrInvalidDBOptions)
	}
	if bucketName == "" {
		return DestroyResult{}, fmt.Errorf("%w: bucket name is required", ErrInvalidDBOptions)
	}
	store := blobstore.New(bucket, bucketName, opts.Prefix)
	return destroyPrefix(ctx, store)
}

func destroyPrefix(ctx context.Context, store *blobstore.Store) (DestroyResult, error) {
	manifestStore := newManifestStore(store, nil)

	current, err := currentForDestroy(ctx, store, manifestStore)
	if err != nil {
		return DestroyResult{}, err
	}

	// Phase 1: the one irreversible publication. Idempotent and
	// response-loss tolerant.
	current, err = manifestStore.CommitDestruction(ctx)
	if err != nil {
		return DestroyResult{}, fmt.Errorf("commit destroyed CURRENT: %w", err)
	}
	if current.Destruction == nil {
		return DestroyResult{}, fmt.Errorf("commit destroyed CURRENT: missing destruction record")
	}
	record := current.Destruction

	// Phase 2a: control objects are not read by any pinned view and may be
	// removed immediately after the terminal commit.
	if err := deleteObjectsUnderPrefixes(ctx, store, []string{"maintenance", "manifest/gc"}); err != nil {
		return DestroyResult{Terminal: true}, fmt.Errorf("delete control objects: %w", err)
	}

	// An already-finalized prefix only needs the orphan verification listing.
	if record.Swept {
		clean, err := verifySweptPrefix(ctx, store)
		if err != nil {
			return DestroyResult{Terminal: true}, fmt.Errorf("verify destroyed prefix: %w", err)
		}
		return DestroyResult{Terminal: true, Swept: clean}, nil
	}

	// Phase 2b: immutable data waits for every view loaded before the
	// terminal commit to pass its existing age boundary.
	notBefore := record.NotBefore()
	if now := time.Now(); now.Before(notBefore) {
		return DestroyResult{
			Terminal:   true,
			Swept:      false,
			RetryAfter: notBefore.Sub(now),
		}, nil
	}

	cursor := record.SweepCursor
	if err := sweepDataObjects(ctx, store, manifestStore, cursor); err != nil {
		return DestroyResult{Terminal: true}, fmt.Errorf("sweep database objects: %w", err)
	}
	if _, err := manifestStore.PersistDestructionProgress(ctx, cursor, true); err != nil {
		return DestroyResult{Terminal: true}, fmt.Errorf("mark destruction sweep complete: %w", err)
	}
	return DestroyResult{Terminal: true, Swept: true}, nil
}

// currentForDestroy classifies the prefix before any write. It preserves the
// fail-closed distinction Open draws between an uninitialized prefix and a
// non-empty prefix that unexpectedly lost CURRENT.
func currentForDestroy(
	ctx context.Context,
	store *blobstore.Store,
	manifestStore *manifest.Store,
) (*manifest.Current, error) {
	current, err := manifestStore.ReadCurrentData(ctx)
	if err != nil {
		return nil, err
	}
	if current != nil {
		return current, nil
	}

	occupied, err := store.HasImmutableDatabaseObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("check database prefix after missing CURRENT: %w", err)
	}
	// A concurrent first writer may create CURRENT between the read and the
	// listing. Re-read once, exactly as the open path does.
	current, err = manifestStore.ReadCurrentData(ctx)
	if err != nil {
		return nil, err
	}
	if current != nil {
		return current, nil
	}
	if occupied {
		return nil, fmt.Errorf("destroy: %w: database prefix %q contains immutable state",
			ErrManifestUnavailable, store.Prefix())
	}
	return nil, fmt.Errorf("destroy: %w: database prefix %q is not initialized",
		ErrManifestUnavailable, store.Prefix())
}

// deleteObjectsUnderPrefixes batch-deletes every object below the given
// relative prefixes. Deletion is idempotent: missing objects are treated as
// deleted.
func deleteObjectsUnderPrefixes(ctx context.Context, store destructionObjectStore, prefixes []string) error {
	for _, prefix := range prefixes {
		var pageToken []byte
		for {
			page, nextToken, err := store.ListPage(ctx, pageToken, destroySweepBatch, blobstore.ListOptions{
				Prefix: prefix,
			})
			if err != nil {
				return err
			}
			keys := objectKeys(page.Objects)
			if len(keys) > 0 {
				if err := store.BatchDelete(ctx, keys); err != nil {
					return err
				}
			}
			if len(nextToken) == 0 {
				break
			}
			pageToken = nextToken
		}
	}
	return nil
}

// sweepDataObjects enumerates the complete database prefix in lexicographic
// key order, deletes every object except the retained terminal CURRENT, and
// persists the completed batch boundary after each batch. It resumes safely
// from cursor after a partial failure or process exit: the listing restarts
// at the prefix root and skips keys at or below cursor, while every repeated
// delete remains a no-op.
func sweepDataObjects(
	ctx context.Context,
	store destructionObjectStore,
	manifestStore *manifest.Store,
	cursor string,
) error {
	currentKey := store.ManifestPath()
	var pageToken []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		page, nextToken, err := store.ListPage(ctx, pageToken, destroySweepBatch, blobstore.ListOptions{})
		if err != nil {
			return err
		}
		batch := make([]string, 0, len(page.Objects))
		for _, obj := range page.Objects {
			if obj.IsDir || obj.Key == currentKey || obj.Key <= cursor {
				continue
			}
			batch = append(batch, obj.Key)
		}
		if len(batch) > 0 {
			// Do not advance the persisted cursor past a batch that did not
			// fully succeed. Successful keys in that batch are simply deleted
			// again on retry.
			if err := store.BatchDelete(ctx, batch); err != nil {
				return err
			}
			cursor = batch[len(batch)-1]
			if _, err := manifestStore.PersistDestructionProgress(ctx, cursor, false); err != nil {
				return err
			}
		}
		if len(nextToken) == 0 {
			return nil
		}
		pageToken = nextToken
	}
}

// verifySweptPrefix performs the post-completion verification listing. An
// object can appear here only if a stale process uploaded an orphan while the
// sweep was finishing; the orphan is removed and the caller reports Swept
// false so the next repeated call verifies a clean prefix.
func verifySweptPrefix(ctx context.Context, store destructionObjectStore) (bool, error) {
	currentKey := store.ManifestPath()
	clean := true
	var pageToken []byte
	for {
		page, nextToken, err := store.ListPage(ctx, pageToken, destroySweepBatch, blobstore.ListOptions{})
		if err != nil {
			return false, err
		}
		var extras []string
		for _, obj := range page.Objects {
			if obj.IsDir || obj.Key == currentKey {
				continue
			}
			extras = append(extras, obj.Key)
		}
		if len(extras) > 0 {
			clean = false
			if err := store.BatchDelete(ctx, extras); err != nil {
				return false, err
			}
		}
		if len(nextToken) == 0 {
			return clean, nil
		}
		pageToken = nextToken
	}
}

func objectKeys(objects []blobstore.ObjectInfo) []string {
	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		if obj.IsDir {
			continue
		}
		keys = append(keys, obj.Key)
	}
	return keys
}

// guard used by handle paths to classify the terminal error without importing
// ad-hoc string matches.
func isDestroyedError(err error) bool {
	return errors.Is(err, manifest.ErrDBDestroyed)
}
