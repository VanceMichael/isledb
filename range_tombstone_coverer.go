package isledb

import (
	"bytes"
	"sort"

	"github.com/ankur-anand/isledb/internal"
)

// rangeTombstoneCoverer answers whether a point at key with sequence seq is
// hidden by any range tombstone. It uses the same fragmented representation
// SST writers use, so the per-key lookup is one binary search.
type rangeTombstoneCoverer struct {
	fragments []fragmentedRange
}

func newRangeTombstoneCoverer(tombstones []internal.RangeTombstone) *rangeTombstoneCoverer {
	return &rangeTombstoneCoverer{fragments: fragmentRangeTombstones(tombstones)}
}

// covers reports that a tombstone covering key has a sequence at least as
// large as seq. Tombstones and points from the same SST are compared by exact
// sequence, so a point written after the tombstone remains visible.
func (c *rangeTombstoneCoverer) covers(key []byte, seq uint64) bool {
	if len(c.fragments) == 0 {
		return false
	}
	i := sort.Search(len(c.fragments), func(i int) bool {
		return bytes.Compare(c.fragments[i].start, key) > 0
	}) - 1
	if i < 0 {
		return false
	}
	fragment := &c.fragments[i]
	if bytes.Compare(key, fragment.end) >= 0 {
		return false
	}
	return fragment.seqs[0] >= seq
}
