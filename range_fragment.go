package isledb

import (
	"bytes"
	"sort"

	"github.com/ankur-anand/isledb/internal"
)

// fragmentedRange is one non-overlapping cell covered by one or more range
// tombstones. Seqs lists every covering tombstone sequence in descending
// order.
type fragmentedRange struct {
	start []byte
	end   []byte
	seqs  []uint64
}

// fragmentRangeTombstones cuts a set of possibly overlapping tombstones into
// the sorted, non-overlapping cells Pebble's sstable writer requires. Cells
// with an identical sequence set are coalesced even when separated by a
// boundary.
//
// The algorithm is event based: every tombstone start and end is a boundary.
// Events are precomputed once; a count map then tracks active tombstone
// sequences while boundaries are walked in order.
func fragmentRangeTombstones(tombstones []internal.RangeTombstone) []fragmentedRange {
	if len(tombstones) == 0 {
		return nil
	}

	boundaries := make([][]byte, 0, len(tombstones)*2)
	for _, tombstone := range tombstones {
		boundaries = append(boundaries, tombstone.Start, tombstone.End)
	}
	sort.Slice(boundaries, func(i, j int) bool {
		return bytes.Compare(boundaries[i], boundaries[j]) < 0
	})
	boundaries = dedupeBoundaries(boundaries)

	indexAt := make(map[string]int, len(boundaries))
	for i, boundary := range boundaries {
		indexAt[string(boundary)] = i
	}
	addEvents := make([][]uint64, len(boundaries))
	endEvents := make([][]uint64, len(boundaries))
	for _, tombstone := range tombstones {
		addEvents[indexAt[string(tombstone.Start)]] = append(
			addEvents[indexAt[string(tombstone.Start)]], tombstone.Seq)
		endEvents[indexAt[string(tombstone.End)]] = append(
			endEvents[indexAt[string(tombstone.End)]], tombstone.Seq)
	}

	active := make(map[uint64]int, len(tombstones))
	var out []fragmentedRange
	for i := 0; i+1 < len(boundaries); i++ {
		lo, hi := boundaries[i], boundaries[i+1]

		for _, seq := range endEvents[i] {
			if active[seq] > 0 {
				active[seq]--
				if active[seq] == 0 {
					delete(active, seq)
				}
			}
		}
		for _, seq := range addEvents[i] {
			active[seq]++
		}

		if len(active) == 0 {
			continue
		}

		seqs := sortedActiveSeqs(active)
		if len(out) > 0 && bytes.Equal(out[len(out)-1].end, lo) &&
			equalSeqSets(out[len(out)-1].seqs, seqs) {
			out[len(out)-1].end = append(out[len(out)-1].end[:0], hi...)
			continue
		}
		out = append(out, fragmentedRange{
			start: append([]byte(nil), lo...),
			end:   append([]byte(nil), hi...),
			seqs:  seqs,
		})
	}
	return out
}

func sortedActiveSeqs(active map[uint64]int) []uint64 {
	seqs := make([]uint64, 0, len(active))
	for seq, count := range active {
		for i := 0; i < count; i++ {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })
	return seqs
}

func equalSeqSets(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func dedupeBoundaries(boundaries [][]byte) [][]byte {
	if len(boundaries) < 2 {
		return boundaries
	}
	kept := boundaries[:1]
	for i := 1; i < len(boundaries); i++ {
		if bytes.Equal(kept[len(kept)-1], boundaries[i]) {
			continue
		}
		kept = append(kept, boundaries[i])
	}
	return kept
}
