package hash

import "testing"

func TestJumpStaysInRange(t *testing.T) {
	for buckets := int32(1); buckets <= 64; buckets++ {
		for key := uint64(0); key < 2000; key++ {
			p := jump(key*0x9E3779B97F4A7C15, buckets)
			if p < 0 || p >= buckets {
				t.Fatalf("jump out of range: key=%d buckets=%d got=%d", key, buckets, p)
			}
		}
	}
}

func TestPartitionIsStable(t *testing.T) {
	if Partition([]byte("aaaaa"), 999) != Partition([]byte("aaaaa"), 999) {
		t.Fatal("same key must map to the same partition")
	}
	if Partition([]byte("aaaaa"), 999) == Partition([]byte("bbbbb"), 999) {
		t.Fatal("different keys are expected to map to different partitions for these inputs")
	}
}
