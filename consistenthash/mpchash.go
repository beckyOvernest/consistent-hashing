package consistenthash

import (
	"fmt"
	"math"
	"sort"
)

// Multi selects buckets with a multi-probe consistent hash
type Multi struct {
	buckets  []string
	replicas int
	seeds    [2]uint64
	hashf    func(b []byte, s uint64) uint64
	k        int

	bmap map[uint64]string

	// We store sorted slices of hashes by bit prefix
	bhashes     [][]uint64
	prefixmask  uint64
	prefixshift uint64
}

// New returns a new multi-probe hasher.  The hash function h is used with the two seeds to generate k different probes.
//replicas is used to define the duplicate number of files. Since we are going to
//use erasure coding for duplication, we don't need replicas. We set replicas as 1 for now in the calling method.
func NewmpcHash(bucketLen int, replicas int, h func(b []byte, s uint64) uint64, seeds [2]uint64, k int) *Multi {

	m := &Multi{
		buckets:  make([]string, bucketLen),
		replicas: replicas,
		hashf:    h,
		seeds:    seeds,
		bmap:     make(map[uint64]string, bucketLen),
		k:        k,
	}

	//copy(m.buckets, buckets)

	const desiredCollisionRate = 6
	prefixlen := bucketLen / desiredCollisionRate
	psize := ilog2(prefixlen)

	m.prefixmask = ((1 << psize) - 1) << (64 - psize)
	m.prefixshift = 64 - psize

	m.bhashes = make([][]uint64, 1<<psize)

	// for _, b := range buckets {
	// 	h := m.hashf([]byte(b), 0)
	// 	prefix := (h & m.prefixmask) >> m.prefixshift

	// 	m.bhashes[prefix] = append(m.bhashes[prefix], h)
	// 	m.bmap[h] = b
	// }

	// for _, v := range m.bhashes {
	// 	sort.Sort(uint64Slice(v))
	// }

	return m
}

// Returns true if there are no items available.
func (m *Multi) IsEmpty() bool {
	return len(m.buckets) == 0
}

func (m *Multi) Add(buckets ...string) {
	for _, b := range buckets {
		h := m.hashf([]byte(b), 0)
		prefix := (h & m.prefixmask) >> m.prefixshift

		m.bhashes[prefix] = append(m.bhashes[prefix], h)
		m.bmap[h] = b
	}

	for _, v := range m.bhashes {
		sort.Sort(uint64Slice(v))
	}
}

// Hash returns the bucket for a given key
func (m *Multi) Hash(key string) []string {
	fmt.Println("hash..............")
	bkey := []byte(key)

	minDistance := uint64(math.MaxUint64)

	selectedNodes := make([]Node, m.replicas)
	for i := 0; i < m.replicas; i++ {
		selectedNodes[i].distance = minDistance
	}
	distanceFunc := func(n1, n2 *Node) bool {
		return n1.distance < n2.distance
	}

	h1 := m.hashf(bkey, m.seeds[0])
	h2 := m.hashf(bkey, m.seeds[1])

	for i := 0; i < m.k; i++ {
		hash := h1 + uint64(i)*h2
		prefix := (hash & m.prefixmask) >> m.prefixshift

		var node uint64
	FOUND:
		for {
			uints := m.bhashes[prefix]

			for _, v := range uints {
				if hash < v {
					node = v
					break FOUND
				}
			}

			prefix++
			if prefix == uint64(len(m.bhashes)) {
				prefix = 0
				// wrapped -- take the first node hash we can find
				for uints = nil; uints == nil; prefix++ {
					uints = m.bhashes[prefix]
				}

				node = uints[0]
				break FOUND
			}
		}

		distance := node - hash
		if distance < selectedNodes[m.replicas-1].distance {
			selectedNodes[m.replicas-1].distance = distance
			selectedNodes[m.replicas-1].hash = node
			By(distanceFunc).Sort(selectedNodes)
			//			minDistance = distance
			//			minhash = node
		}
	}

	var results []string
	for i := 0; i < m.replicas; i++ {
		results = append(results, m.bmap[selectedNodes[i].hash])
	}
	return results
}

type Node struct {
	hash     uint64
	distance uint64
}

// By is the type of a "less" function that defines the ordering of its Planet arguments.
type By func(n1, n2 *Node) bool

// planetSorter joins a By function and a slice of Planets to be sorted.
type nodeSorter struct {
	nodes []Node
	by    func(n1, n2 *Node) bool // Closure used in the Less method.
}

// Sort is a method on the function type, By, that sorts the argument slice according to the function.
func (by By) Sort(nodes []Node) {
	ns := &nodeSorter{
		nodes: nodes,
		by:    by, // The Sort method's receiver is the function (closure) that defines the sort order.
	}
	sort.Sort(ns)
}

// Len is part of sort.Interface.
func (s *nodeSorter) Len() int {
	return len(s.nodes)
}

// Swap is part of sort.Interface.
func (s *nodeSorter) Swap(i, j int) {
	s.nodes[i], s.nodes[j] = s.nodes[j], s.nodes[i]
}

// Less is part of sort.Interface. It is implemented by calling the "by" closure in the sorter.
func (s *nodeSorter) Less(i, j int) bool {
	return s.by(&s.nodes[i], &s.nodes[j])
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// integer log base 2
func ilog2(v int) uint64 {
	var r uint64
	for ; v != 0; v >>= 1 {
		r++
	}
	return r
}
