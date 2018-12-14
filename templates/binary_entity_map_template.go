package gosmonaut

import (
	"math"
	"sort"
)

// binaryGenericEntityTypeEntityMap uses a binary search table for storing
// entites. It is well suited in this case since we only have to sort it once
// between the reads and writes. Also we avoid the memory overhead of storing
// the IDs twice and instead just read them from the struct. The (fake-)generic
// solution performs much better than it would using the OSMEntity interface.
type binaryGenericEntityTypeEntityMap struct {
	buckets [][]GenericEntityType
	n       uint64
}

func newBinaryGenericEntityTypeEntityMap(n int) *binaryGenericEntityTypeEntityMap {
	// Calculate the number of buckets, exponent defines max number of lookups
	nb := n / int(math.Pow(2, 20))
	if nb < 1 {
		nb = 1
	}

	// Calculate the bucket sizes. Leave a small array overhead since the
	// distribution is not completely even.
	var bs int
	if nb == 1 {
		bs = n
	} else {
		bs = int(float64(n/nb) * 1.05)
	}

	// Create the buckets
	buckets := make([][]GenericEntityType, nb)
	for i := 0; i < nb; i++ {
		buckets[i] = make([]GenericEntityType, 0, bs)
	}
	return &binaryGenericEntityTypeEntityMap{
		buckets: buckets,
		n:       uint64(nb),
	}
}

func (m *binaryGenericEntityTypeEntityMap) hash(id int64) uint64 {
	return uint64(id) % m.n
}

// Must not be called after calling prepare()
func (m *binaryGenericEntityTypeEntityMap) add(e GenericEntityType) {
	h := m.hash(e.ID)
	m.buckets[h] = append(m.buckets[h], e)
}

// Must be called between the last write and the first read
func (m *binaryGenericEntityTypeEntityMap) prepare() {
	// Sort buckets
	for _, b := range m.buckets {
		sort.Slice(b, func(i, j int) bool {
			return b[i].ID < b[j].ID
		})
	}
}

// Must not be called before calling prepare()
func (m *binaryGenericEntityTypeEntityMap) get(id int64) (GenericEntityType, bool) {
	h := m.hash(id)
	bucket := m.buckets[h]

	// Binary search (we can't use sort.Search as we use int64)
	lo := 0
	hi := len(bucket) - 1
	for lo <= hi {
		mid := (lo + hi) / 2
		midID := bucket[mid].ID

		if midID < id {
			lo = mid + 1
		} else if midID > id {
			hi = mid - 1
		} else {
			return bucket[mid], true
		}
	}
	return GenericEntityType{}, false
}
