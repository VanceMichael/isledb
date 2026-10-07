package isledb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ankur-anand/isledb/internal"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"golang.org/x/sync/errgroup"
)

const (
	compactionSSTIDPrefix = "compacted-"
	compactionSSTIDSuffix = ".sst"
	compactionSSTHashLen  = sha256.Size * 2
	compactionSSTIndexLen = 4
)

func buildSSTIDWithTimestamp(epoch, seqLo, seqHi uint64, ts time.Time) string {
	return fmt.Sprintf("%d-%d-%d-%d.sst", epoch, seqLo, seqHi, ts.UnixNano())
}

type sstStreamIdentity struct {
	ID        string
	Epoch     uint64
	CreatedAt time.Time
}

func newSSTStreamIdentity(epoch, seqLo, seqHi uint64, createdAt time.Time) sstStreamIdentity {
	createdAt = createdAt.UTC()
	return sstStreamIdentity{
		ID:        buildSSTIDWithTimestamp(epoch, seqLo, seqHi, createdAt),
		Epoch:     epoch,
		CreatedAt: createdAt,
	}
}

func writerSSTEpoch(id string) (uint64, bool) {
	if !strings.HasSuffix(id, compactionSSTIDSuffix) || isCompactionSSTID(id) {
		return 0, false
	}
	parts := strings.Split(strings.TrimSuffix(id, compactionSSTIDSuffix), "-")
	if len(parts) != 4 {
		return 0, false
	}
	values := make([]uint64, len(parts))
	for i := range parts {
		value, err := strconv.ParseUint(parts[i], 10, 64)
		if err != nil {
			return 0, false
		}
		values[i] = value
	}
	if values[0] == 0 || values[1] > values[2] || values[3] == 0 {
		return 0, false
	}
	return values[0], true
}

// sstStreamSetIdentity names every output of one deterministic multi-SST
// build. OutputKey is derived by the compactor from its active fence, immutable
// inputs, and byte-affecting output policy. Retries within that ownership reuse
// the same names; a successor compactor receives another namespace.
type sstStreamSetIdentity struct {
	OutputKey string
	Epoch     uint64
	CreatedAt time.Time
}

func (identity sstStreamSetIdentity) output(index int) (sstStreamIdentity, error) {
	if identity.OutputKey == "" || identity.Epoch == 0 || identity.CreatedAt.IsZero() || index <= 0 {
		return sstStreamIdentity{}, errors.New("incomplete multi-SST stream identity")
	}
	return sstStreamIdentity{
		ID: fmt.Sprintf("%s%s-%0*d%s", compactionSSTIDPrefix, identity.OutputKey,
			compactionSSTIndexLen, index, compactionSSTIDSuffix),
		Epoch:     identity.Epoch,
		CreatedAt: identity.CreatedAt.UTC(),
	}, nil
}

// isCompactionSSTID recognizes the exact immutable output grammar. Writer
// flushes use a different grammar; newly written compaction outputs use this
// one, while metadata-only moves retain their existing IDs. Orphan reclamation
// uses the distinction, so the parser stays beside the formatter.
func isCompactionSSTID(id string) bool {
	if !strings.HasPrefix(id, compactionSSTIDPrefix) || !strings.HasSuffix(id, compactionSSTIDSuffix) {
		return false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(id, compactionSSTIDPrefix), compactionSSTIDSuffix)
	hash, index, ok := strings.Cut(body, "-")
	if !ok || len(hash) != compactionSSTHashLen || len(index) != compactionSSTIndexLen {
		return false
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return false
	}
	n, err := strconv.Atoi(index)
	return err == nil && n > 0
}

type streamSSTResult struct {
	Meta sstMetadata
}

// writeSSTStreaming builds and uploads an SST concurrently using io.Pipe.
// The producer goroutine writes SST data to a PipeWriter, while the consumer
// goroutine reads from the PipeReader and uploads to the store.
func writeSSTStreaming(
	ctx context.Context,
	it sstIterator,
	tombstones []internal.RangeTombstone,
	opts sstWriterOptions,
	identity sstStreamIdentity,
	uploadFn func(ctx context.Context, sstID string, r io.Reader) error,
) (result streamSSTResult, err error) {
	defer func() {
		err = errors.Join(err, it.Close())
	}()
	if identity.ID == "" || identity.Epoch == 0 || identity.CreatedAt.IsZero() {
		return result, errors.New("incomplete SST stream identity")
	}

	// Fragment before the pipe starts: fragmentation is deterministic and its
	// output is the only range data the producer goroutine needs.
	fragments := fragmentRangeTombstones(tombstones)

	pr, pw := io.Pipe()
	writable := newHashingWritable(pw)
	bloomKeys := newSSTBloomKeys(opts.BloomBitsPerKey)

	wo := pebbleWriterOptions(opts)

	sst := sstable.NewWriter(writable, wo)
	state := newSSTBuildState()

	type producerResult struct {
		state       *sstBuildState
		bloom       bloomMetadata
		metaOffset  int64
		rangeResult rangeAddResult
		err         error
	}
	producerDone := make(chan producerResult, 1)

	var uploadErr atomic.Value
	getUploadErr := func() error {
		if v := uploadErr.Load(); v != nil {
			return v.(error)
		}
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)

	// Read from the pipe and upload to object storage.
	g.Go(func() error {
		err := uploadFn(gctx, identity.ID, pr)
		if err != nil {
			uploadErr.Store(err)
			_ = pr.CloseWithError(err)
			return fmt.Errorf("sst upload: %w", err)
		}
		_ = pr.Close()
		return nil
	})

	g.Go(func() (err error) {
		defer func() {
			if closeErr := pw.Close(); err == nil {
				err = closeErr
			}
		}()

		for it.Next() {
			if err := gctx.Err(); err != nil {
				if ue := getUploadErr(); ue != nil {
					err = fmt.Errorf("sst upload: %w", ue)
				}
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return err
			}

			e := it.Entry()
			k := append([]byte(nil), e.Key...)
			bloomKeys.add(k)

			keyEntry := buildKeyEntry(e, k)
			encodedValue := internal.EncodeKeyEntry(keyEntry)

			if err := state.updateOrder(k, e.Seq); err != nil {
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return fmt.Errorf("sst producer: %w", err)
			}

			kind := pebble.InternalKeyKindSet
			if e.Kind == internal.OpDelete {
				kind = pebble.InternalKeyKindDelete
			}

			ikey := pebble.MakeInternalKey(k, pebble.SeqNum(e.Seq), kind)

			if err := sst.Raw().Add(ikey, encodedValue, false); err != nil {
				if ue := getUploadErr(); ue != nil && errors.Is(err, io.ErrClosedPipe) {
					err = fmt.Errorf("sst upload: %w", ue)
				}
				writable.Abort()
				_ = sst.Close()
				producerDone <- producerResult{err: err}
				pw.CloseWithError(err)
				return fmt.Errorf("sst producer: %w", err)
			}

			state.updateBounds(k, e.Seq)
		}

		if err := it.Err(); err != nil {
			writable.Abort()
			_ = sst.Close()
			producerDone <- producerResult{err: err}
			pw.CloseWithError(err)
			return fmt.Errorf("sst producer: %w", err)
		}

		// Range tombstones are independent of point ordering; add them before
		// the empty check and Close so tombstone-only SSTs are valid.
		rangeResult, rangeErr := addRangeFragments(sst, fragments)
		if rangeErr != nil {
			writable.Abort()
			_ = sst.Close()
			producerDone <- producerResult{err: rangeErr}
			pw.CloseWithError(rangeErr)
			return fmt.Errorf("sst producer: %w", rangeErr)
		}
		if !state.found && rangeResult.records == 0 {
			writable.Abort()
			_ = sst.Close()
			producerDone <- producerResult{err: errEmptyIterator}
			pw.CloseWithError(errEmptyIterator)
			return errEmptyIterator
		}

		if err := sst.Close(); err != nil {
			if ue := getUploadErr(); ue != nil && errors.Is(err, io.ErrClosedPipe) {
				err = fmt.Errorf("sst upload: %w", ue)
			}
			producerDone <- producerResult{err: err}
			pw.CloseWithError(err)
			return fmt.Errorf("sst producer: %w", err)
		}

		sstSize := writable.size
		metaOffset := sstMetaOffset(sst)
		bloomData, bloom, err := bloomKeys.build(sstSize)
		if err == nil {
			err = writeBloomSidecar(pw, bloomData)
		}
		if err != nil {
			if ue := getUploadErr(); ue != nil && errors.Is(err, io.ErrClosedPipe) {
				err = fmt.Errorf("sst upload: %w", ue)
			}
			producerDone <- producerResult{err: err}
			pw.CloseWithError(err)
			return fmt.Errorf("sst producer: %w", err)
		}

		producerDone <- producerResult{
			state:       state,
			metaOffset:  metaOffset,
			bloom:       bloom,
			rangeResult: rangeResult,
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return result, err
	}
	pResult := <-producerDone
	if pResult.err != nil {
		return result, pResult.err
	}

	hashBytes := writable.sumBytes()
	hashStr := hex.EncodeToString(hashBytes)

	seqLo, seqHi := unionSeqBounds(pResult.state, pResult.rangeResult)
	result.Meta = sstMetadata{
		ID:             identity.ID,
		Epoch:          identity.Epoch,
		SeqLo:          seqLo,
		SeqHi:          seqHi,
		MinKey:         minBytesOr(pResult.state.minKey, pResult.rangeResult.minKey),
		MaxKey:         maxBytesOr(pResult.state.maxKey, pResult.rangeResult.maxKey),
		Size:           writable.size,
		Checksum:       "sha256:" + hashStr,
		Bloom:          pResult.bloom,
		CreatedAt:      identity.CreatedAt,
		MetaOffset:     pResult.metaOffset,
		RangeDeletions: pResult.rangeResult.meta(),
	}

	return result, nil
}

// writeMultipleSSTsStreaming builds and uploads multiple SSTs using streaming.
// Each SST is streamed to the upload function as it's built, with new SSTs
// started when the current one reaches targetSize.
//
// Range tombstones are fragmented independently of the point stream and
// distributed across outputs at output key boundaries: a tombstone spanning a
// split is cut at the next output's first point key, so each fragment lands in
// the output whose key coverage it belongs to. Tail fragments beyond the last
// point are emitted by the final output and extend its manifest bounds.
func writeMultipleSSTsStreaming(
	ctx context.Context,
	it sstIterator,
	tombstones []internal.RangeTombstone,
	opts sstWriterOptions,
	identity sstStreamSetIdentity,
	targetSize int64,
	uploadFn func(ctx context.Context, sstID string, r io.Reader) error,
) (results []streamSSTResult, err error) {
	defer func() {
		err = errors.Join(err, it.Close())
	}()

	// Fragment once before any output starts.
	fragments := fragmentRangeTombstones(tombstones)

	wo := pebbleWriterOptions(opts)

	var pr *io.PipeReader
	var pw *io.PipeWriter
	var writable *hashingWritable
	var sst *sstable.Writer
	var state *sstBuildState
	bloomKeys := newSSTBloomKeys(opts.BloomBitsPerKey)
	var sstID string
	var uploadErr atomic.Value
	var uploadDone chan struct{}
	var uploadCancel context.CancelFunc
	var started bool
	var sstIndex int
	var fragCursor int
	var splitPending bool

	getUploadErr := func() error {
		if v := uploadErr.Load(); v != nil {
			return v.(error)
		}
		return nil
	}

	// assignFragments returns the fragment portions that belong to the output
	// about to be closed. boundary is the first point key of the next output;
	// final means the last output, which also receives tail fragments.
	assignFragments := func(boundary []byte, final bool) []fragmentedRange {
		var assigned []fragmentedRange
		for fragCursor < len(fragments) {
			fragment := fragments[fragCursor]
			if !final && bytes.Compare(fragment.start, boundary) >= 0 {
				break
			}
			if !final && bytes.Compare(fragment.end, boundary) > 0 {
				// The portion before boundary belongs here; the remainder stays
				// in the cursor for the next output.
				assigned = append(assigned, fragmentedRange{
					start: append([]byte(nil), fragment.start...),
					end:   append([]byte(nil), boundary...),
					seqs:  append([]uint64(nil), fragment.seqs...),
				})
				break
			}
			assigned = append(assigned, fragmentedRange{
				start: append([]byte(nil), fragment.start...),
				end:   append([]byte(nil), fragment.end...),
				seqs:  append([]uint64(nil), fragment.seqs...),
			})
			fragCursor++
		}
		return assigned
	}

	startNewSST := func() error {
		sstIndex++
		outputIdentity, err := identity.output(sstIndex)
		if err != nil {
			return err
		}
		sstID = outputIdentity.ID

		pr, pw = io.Pipe()
		writable = newHashingWritable(pw)
		sst = sstable.NewWriter(writable, wo)
		state = newSSTBuildState()
		bloomKeys.reset()
		uploadErr = atomic.Value{}
		uploadDone = make(chan struct{})
		uploadCtx, cancelUpload := context.WithCancel(ctx)
		uploadCancel = cancelUpload
		started = true

		go func(id string, reader *io.PipeReader, done chan struct{}, errVal *atomic.Value) {
			defer close(done)
			err := uploadFn(uploadCtx, id, reader)
			if err != nil {
				errVal.Store(err)
			}
			_ = reader.CloseWithError(err)
		}(sstID, pr, uploadDone, &uploadErr)
		return nil
	}

	finishCurrentSST := func(boundary []byte, final bool) error {
		if !started {
			return nil
		}

		// Add assigned tombstone portions before Close.
		assigned := assignFragments(boundary, final)
		rangeResult, rangeErr := addRangeFragments(sst, assigned)
		if rangeErr != nil {
			pw.CloseWithError(rangeErr)
			uploadCancel()
			<-uploadDone
			return rangeErr
		}

		if err := sst.Close(); err != nil {
			pw.CloseWithError(err)
			uploadCancel()
			<-uploadDone
			return err
		}

		sstSize := writable.size
		metaOffset := sstMetaOffset(sst)
		bloomData, bloom, err := bloomKeys.build(sstSize)
		if err == nil {
			err = writeBloomSidecar(pw, bloomData)
		}
		if err != nil {
			pw.CloseWithError(err)
			uploadCancel()
			<-uploadDone
			return err
		}

		closeErr := pw.Close()

		<-uploadDone
		uploadCancel()
		if ue := getUploadErr(); ue != nil {
			return fmt.Errorf("sst upload: %w", ue)
		}
		if closeErr != nil {
			return fmt.Errorf("close sst upload stream: %w", closeErr)
		}

		hashBytes := writable.sumBytes()
		hashStr := hex.EncodeToString(hashBytes)

		seqLo, seqHi := unionSeqBounds(state, rangeResult)
		result := streamSSTResult{
			Meta: sstMetadata{
				ID:             sstID,
				Epoch:          identity.Epoch,
				SeqLo:          seqLo,
				SeqHi:          seqHi,
				MinKey:         minBytesOr(state.minKey, rangeResult.minKey),
				MaxKey:         maxBytesOr(state.maxKey, rangeResult.maxKey),
				Size:           sstSize,
				Checksum:       "sha256:" + hashStr,
				Bloom:          bloom,
				CreatedAt:      identity.CreatedAt.UTC(),
				MetaOffset:     metaOffset,
				RangeDeletions: rangeResult.meta(),
			},
		}

		results = append(results, result)
		started = false
		pr, pw, writable, sst, state, uploadDone, uploadCancel = nil, nil, nil, nil, nil, nil, nil
		return nil
	}

	abortCurrentSST := func() {
		if !started {
			return
		}

		abortErr := errors.New("sst aborted")

		if writable != nil {
			writable.Abort()
		}
		if sst != nil {
			_ = sst.Close()
		}
		if pw != nil {
			pw.CloseWithError(abortErr)
		}

		if pr != nil {
			pr.CloseWithError(abortErr)
		}
		if uploadCancel != nil {
			uploadCancel()
		}

		if uploadDone != nil {
			<-uploadDone
		}
		started = false
		pr, pw, writable, sst, state, uploadDone, uploadCancel = nil, nil, nil, nil, nil, nil, nil
	}

	for it.Next() {
		if err := ctx.Err(); err != nil {
			abortCurrentSST()
			return nil, err
		}

		e := it.Entry()

		// A deferred split from the previous entry finishes that output using
		// this entry's key as the boundary before a new output is opened.
		if splitPending {
			if err := finishCurrentSST(e.Key, false); err != nil {
				return nil, err
			}
			splitPending = false
		}

		if !started {
			if err := startNewSST(); err != nil {
				return nil, err
			}
		}

		k := append([]byte(nil), e.Key...)
		bloomKeys.add(k)

		keyEntry := buildKeyEntry(e, k)
		encodedValue := internal.EncodeKeyEntry(keyEntry)

		if err := state.updateOrder(k, e.Seq); err != nil {
			abortCurrentSST()
			return nil, err
		}

		kind := pebble.InternalKeyKindSet
		if e.Kind == internal.OpDelete {
			kind = pebble.InternalKeyKindDelete
		}

		ikey := pebble.MakeInternalKey(k, pebble.SeqNum(e.Seq), kind)

		if err := sst.Raw().Add(ikey, encodedValue, false); err != nil {
			abortCurrentSST()
			return nil, fmt.Errorf("sst producer: %w", err)
		}

		state.updateBounds(k, e.Seq)

		if writable.size >= targetSize {
			// Defer closing until the next entry provides the boundary key.
			splitPending = true
		}
	}

	if err := it.Err(); err != nil {
		abortCurrentSST()
		return nil, fmt.Errorf("sst producer: %w", err)
	}

	switch {
	case splitPending:
		// Iterator ended immediately after the size threshold. Finish that
		// output as the final output.
		if err := finishCurrentSST(nil, true); err != nil {
			return nil, err
		}
	case started && (state.found || fragCursor < len(fragments)):
		if err := finishCurrentSST(nil, true); err != nil {
			return nil, err
		}
	case started:
		// IMP: Fix goroutine leak for exhausted iterator.
		abortCurrentSST()
	}

	// Tombstone-only inputs produce no points but still need an output.
	if !started && fragCursor < len(fragments) {
		if err := startNewSST(); err != nil {
			return nil, err
		}
		if err := finishCurrentSST(nil, true); err != nil {
			return nil, err
		}
	}

	if len(results) == 0 {
		return nil, errEmptyIterator
	}

	return results, nil
}
