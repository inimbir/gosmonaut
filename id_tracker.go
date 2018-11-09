package gosmonaut

/* ID Tracker */
type idTracker interface {
	set(int64)
	get(int64) bool
	len() int
}

/* Bitset ID Tracker */
type bitsetIDTracker struct {
	neg, pos []bitset // One for negative and positive IDs
}

func newBitsetIDTracker() *bitsetIDTracker {
	return new(bitsetIDTracker)
}

func (c *bitsetIDTracker) index(id int64) (b *[]bitset, setID, bitID int) {
	// Select positive or negative bitset
	if id < 0 {
		b = &c.neg
		id = -id
	} else {
		b = &c.pos
	}

	setID = int(id / idsPerBitset)
	bitID = int(id % idsPerBitset)
	return
}

func (c *bitsetIDTracker) set(id int64) {
	b, setID, bitID := c.index(id)
	for setID >= len(*b) {
		*b = append(*b, newBitset())
	}
	(*b)[setID].set(bitID)
}

func (c *bitsetIDTracker) get(id int64) bool {
	b, setID, bitID := c.index(id)
	if setID >= len(*b) {
		return false
	}
	return (*b)[setID].get(bitID)
}

func (c *bitsetIDTracker) len() int {
	n := 0
	for _, sets := range [][]bitset{c.pos, c.neg} {
		for _, b := range sets {
			n += b.n
		}
	}
	return n
}

/* Bitset */
const (
	bytesPerBitset = 2048
	idsPerBitset   = bytesPerBitset * 8
)

// bitset uses at most bytesPerBitset bytes. The byte slice in truncated from
// both sides and grows as needed to keep it as small as possible.
type bitset struct {
	buf       []byte
	n, offset int
}

func newBitset() bitset {
	return bitset{
		offset: -1,
	}
}

func (b *bitset) index(x int) (byteID, bufID int, bitID uint) {
	byteID = x / 8
	bufID = byteID - b.offset
	bitID = uint(x % 8)
	return
}

func (b *bitset) set(x int) {
	// Check if ID exists
	if b.get(x) {
		return
	}
	b.n++

	byteID, bufID, bitID := b.index(x)
	if b.offset == -1 {
		// First insert: use a single byte
		b.grow(false, 1)
		b.offset = byteID
		bufID = 0
	} else if bufID < 0 {
		// Prepend to slice
		d := b.offset - byteID
		b.grow(true, len(b.buf)+d)
		b.offset -= d
		bufID = 0
	} else if bufID >= len(b.buf) {
		// Append to slice
		b.grow(false, bufID+1)
	}

	// Set bit
	b.buf[bufID] |= 1 << bitID
}

func (b *bitset) grow(prepend bool, n int) {
	offset := n - len(b.buf)

	// Grow slice
	for n > len(b.buf) {
		b.buf = append(b.buf, 0)
	}

	if prepend {
		// Shift values to the end
		copy(b.buf[offset:], b.buf)
		for i := 0; i < offset; i++ {
			b.buf[i] = 0
		}
	}
}

func (b *bitset) get(x int) bool {
	_, bufID, bitID := b.index(x)
	if bufID < 0 {
		return false
	} else if bufID >= len(b.buf) {
		return false
	}
	return b.buf[bufID]&(1<<bitID) != 0
}
