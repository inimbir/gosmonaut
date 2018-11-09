package gosmonaut

import (
	"testing"
)

func TestIDTracker(t *testing.T) {
	// Test number series
	for m := int64(1); m <= 10; m++ {
		var c idTracker = newBitsetIDTracker()
		n := 0
		r := int64(5000 / m * m)
		for i := r; i >= -r; i -= m {
			c.set(i)
			c.set(i)
			n++
			if c.len() != n {
				t.Error("Tracker length is wrong")
			}
		}

		for i := r; i >= -r; i -= m {
			contains := i%m == 0
			if c.get(i) != contains {
				t.Error("Tracker failed for ID", i)
			}
		}
	}

	c := newBitsetIDTracker()
	testIDTracker(t, c, 0, 1)
	testIDTracker(t, c, 1, 2)
	testIDTracker(t, c, -5123, 3)
	testIDTracker(t, c, 5955111, 4)
	testIDTracker(t, c, -92233720, 5)
	testIDTracker(t, c, 92233720, 6)
}

func testIDTracker(t *testing.T, c idTracker, n int64, l int) {
	if c.get(n) {
		t.Error("Tracker already contained number")
	}
	c.set(n)
	if !c.get(n) {
		t.Error("Tracker does not contain number")
	}

	// Check length
	if l != c.len() {
		t.Error("Tracker length is wrong")
	}
}
