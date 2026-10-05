// Package hash maps keys to partitions with xxhash + jump consistent hash.
package hash

import "github.com/cespare/xxhash/v2"

// Partition returns the bucket in [0, numBuckets) for key. Adding buckets moves only a minimal share of keys.
func Partition(key []byte, numBuckets int32) int32 {
	return jump(xxhash.Sum64(key), numBuckets)
}

// jump implements consistent hashing from this paper: http://arxiv.org/abs/1406.2294
// code is copied from https://github.com/dgryski/go-jump/blob/master/jump.go
func jump(key uint64, numBuckets int32) int32 {
	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	// b < numBuckets (an int32), so the conversion cannot overflow.
	return int32(b) // #nosec G115
}
