// Package ec provides erasure-coding (Reed-Solomon) helpers built on top of
// the github.com/klauspost/reedsolomon library and adapted to the micros3
// domain types.
//
// A Codec splits object data into k data shards and m parity shards. Each
// node in the cluster stores exactly one shard (plus the full metadata).
// Any k out of k+m shards are sufficient to reconstruct the original data.
package ec

import (
	"errors"

	"github.com/klauspost/reedsolomon"
)

// Codec is a Reed-Solomon (k, m) erasure codec.
type Codec struct {
	k, m int
	enc  reedsolomon.Encoder
}

// NewCodec creates a Reed-Solomon codec with k data shards and m parity
// shards. k must be >= 1, m >= 0, and k+m <= 256.
func NewCodec(k, m int) (*Codec, error) {
	if k < 1 {
		return nil, errors.New("ec: k must be >= 1")
	}
	if m < 0 {
		return nil, errors.New("ec: m must be >= 0")
	}
	if k+m > 256 {
		return nil, errors.New("ec: k+m must be <= 256")
	}
	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, err
	}
	return &Codec{k: k, m: m, enc: enc}, nil
}

// K returns the number of data shards.
func (c *Codec) K() int { return c.k }

// M returns the number of parity shards.
func (c *Codec) M() int { return c.m }

// N returns the total number of shards (k+m).
func (c *Codec) N() int { return c.k + c.m }

// ShardSize returns the size of each shard given a data length. Every shard
// has the same size; the last data shard is zero-padded to the computed
// size.
func (c *Codec) ShardSize(dataLen int64) int64 {
	if c.k == 0 {
		return 0
	}
	return (dataLen + int64(c.k) - 1) / int64(c.k)
}

// Encode splits data into k+m shards of equal length. The first k shards
// contain the (zero-padded) data; the remaining m shards are parity.
// The returned slice has length k+m. Each shard is a newly allocated
// slice of size ShardSize(len(data)).
func (c *Codec) Encode(data []byte) ([][]byte, error) {
	shardSize := int(c.ShardSize(int64(len(data))))
	if shardSize == 0 {
		shardSize = 1
	}
	n := c.k + c.m
	shards := make([][]byte, n)
	for i := range shards {
		shards[i] = make([]byte, shardSize)
	}
	// Copy data into the first k shards.
	for i := 0; i < len(data); i++ {
		shards[i/shardSize][i%shardSize] = data[i]
	}
	if err := c.enc.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

// Reconstruct rebuilds the original data from a subset of shards.
//
// shards is a slice of length k+m where present shards are non-nil and
// missing shards are nil. At least k shards must be present. The function
// fills in the missing shards in-place and returns the reconstructed data
// (without trailing zero-padding) using dataLen as the original length.
func (c *Codec) Reconstruct(shards [][]byte, dataLen int64) ([]byte, error) {
	n := c.k + c.m
	if len(shards) != n {
		return nil, errors.New("ec: expected k+m shards")
	}
	// Ensure all shard slots exist (reedsolomon requires non-nil entries,
	// using nil for missing ones).
	for i := range shards {
		if shards[i] == nil {
			shards[i] = nil // keep as nil marker
		}
	}
	if err := c.enc.Reconstruct(shards); err != nil {
		return nil, err
	}
	return trimData(shards, c.k, dataLen), nil
}

// Verify reports whether the provided shards are consistent (i.e. the
// parity shards match the data shards). shards uses the same convention as
// Reconstruct: nil entries denote missing shards and are ignored.
func (c *Codec) Verify(shards [][]byte) (bool, error) {
	if len(shards) != c.k+c.m {
		return false, errors.New("ec: expected k+m shards")
	}
	return c.enc.Verify(shards)
}

func trimData(shards [][]byte, k int, dataLen int64) []byte {
	if len(shards) == 0 {
		return nil
	}
	shardSize := len(shards[0])
	if dataLen <= 0 {
		out := make([]byte, 0, k*shardSize)
		for i := 0; i < k; i++ {
			out = append(out, shards[i]...)
		}
		return out
	}
	out := make([]byte, 0, dataLen)
	for i := 0; i < k; i++ {
		remaining := dataLen - int64(i*shardSize)
		if remaining <= 0 {
			break
		}
		end := int64(shardSize)
		if end > remaining {
			end = remaining
		}
		out = append(out, shards[i][:end]...)
	}
	return out
}
