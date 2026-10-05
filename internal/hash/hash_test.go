package hash

import "testing"

func TestJumpStaysInRange(t *testing.T) {
	for buckets := int32(1); buckets <= 64; buckets++ {
		for key := range uint64(2000) {
			p := jump(key*0x9E3779B97F4A7C15, buckets)
			if p < 0 || p >= buckets {
				t.Fatalf("jump out of range: key=%d buckets=%d got=%d", key, buckets, p)
			}
		}
	}
}

func TestPartitionIsStable(t *testing.T) {
	a1 := Partition([]byte("aaaaa"), 999)
	a2 := Partition([]byte("aaaaa"), 999)
	b := Partition([]byte("bbbbb"), 999)

	if a1 != a2 {
		t.Fatalf("same key must map to the same partition: %d != %d", a1, a2)
	}
	if a1 == b {
		t.Fatalf("different keys are expected to map to different partitions for these inputs: both %d", a1)
	}
}
