package consistenthash

import (
	"fmt"
	"testing"
)

func benchmarkLookup(b *testing.B, nbuckets int) {

	var buckets []string

	m := NewmpcHash(nbuckets, 3, siphash64seed, [2]uint64{1, 2}, 21)

	for i := 1; i <= nbuckets; i++ {
		m.Add(fmt.Sprintf("shard-%d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Hash(buckets[i&(nbuckets-1)])
	}
}

func BenchmarkLookup8(b *testing.B)    { benchmarkLookup(b, 8) }
func BenchmarkLookup32(b *testing.B)   { benchmarkLookup(b, 32) }
func BenchmarkLookup128(b *testing.B)  { benchmarkLookup(b, 128) }
func BenchmarkLookup512(b *testing.B)  { benchmarkLookup(b, 512) }
func BenchmarkLookup2048(b *testing.B) { benchmarkLookup(b, 2048) }
func BenchmarkLookup8192(b *testing.B) { benchmarkLookup(b, 8192) }
