package internal

import "bytes"

// RangeTombstone deletes every visible point key k with Start <= k < End and a
// sequence number not greater than Seq. Point writes with a larger sequence
// number remain visible even when their key falls inside [Start, End).
type RangeTombstone struct {
	Start []byte
	End   []byte
	Seq   uint64
}

// EncodedSize is the approximate memory cost used for memtable rotation and
// size accounting.
func (t RangeTombstone) EncodedSize() int64 {
	return int64(len(t.Start) + len(t.End) + 8)
}

// Contains reports whether the tombstone covers key under byte ordering.
func (t RangeTombstone) Contains(key []byte) bool {
	return bytes.Compare(t.Start, key) <= 0 && bytes.Compare(key, t.End) < 0
}
