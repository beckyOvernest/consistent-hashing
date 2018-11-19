package consistenthash

import (
	"bufio"
	"fmt"
	"os"
	"testing"

	"github.com/dchest/siphash"
)

func siphash64seed(b []byte, s uint64) uint64 { return siphash.Hash(s, 0, b) }

func TestCompat(t *testing.T) {

	f, err := os.Open("testdata/compat.out")
	if err != nil {
		t.Fatalf("compat: %v", err)
	}
	scanner := bufio.NewScanner(f)
	var want []string
	for scanner.Scan() {
		want = append(want, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("error during scan: %v", err)
	}

	var buckets []string
	for i := 1; i <= 6000; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	m := NewmpcHash(6000, 3, siphash64seed, [2]uint64{1, 2}, 21)

	// var buckets2 []string
	// for i := 3001; i <= 6000; i++ {
	// 	buckets2 = append(buckets2, fmt.Sprintf("shard-%d", i))
	// }

	for i := 1; i <= 6000; i++ {
		m.Add(fmt.Sprintf("shard-%d", i))
	}

	for i, b := range buckets {
		if g := m.Hash(b); g != want[i] {
			t.Errorf("Hash(%v)=%v, want %v", b, g, want[i])
		}
	}
}
