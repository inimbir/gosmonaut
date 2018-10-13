package gosmonaut

import (
	"encoding/json"
)

// Make tags map from stringtable and two parallel arrays of IDs.
func extractTags(stringTable []string, keyIDs, valueIDs []uint32) OSMTags {
	tags := NewOSMTags(len(keyIDs))
	for index, keyID := range keyIDs {
		key := stringTable[keyID]
		val := stringTable[valueIDs[index]]
		tags.Set(key, val)
	}
	return tags
}

type tagUnpacker struct {
	stringTable []string
	keysVals    []int32
	index       int
}

// Make tags map from stringtable and array of IDs (used in DenseNodes encoding).
func (tu *tagUnpacker) next() OSMTags {
	var tags OSMTags
	for tu.index < len(tu.keysVals) {
		keyID := tu.keysVals[tu.index]
		tu.index++
		if keyID == 0 {
			break
		}

		valID := tu.keysVals[tu.index]
		tu.index++

		key := tu.stringTable[keyID]
		val := tu.stringTable[valID]
		tags.Set(key, val)
	}
	return tags
}

/* OSM Tags */

// OSMTags represents a key-value mapping for OSM tags.
type OSMTags []string // Alternating array of keys/values

// NewOSMTags creates new OSMTags and can be used if the number of tags is
// known.
func NewOSMTags(n int) OSMTags {
	if n < 0 {
		n = 0
	}
	return make([]string, 0, n*2)
}

// NewOSMTagsFromMap creates new OSMTags that contains the tags from the given
// map.
func NewOSMTagsFromMap(m map[string]string) OSMTags {
	t := NewOSMTags(len(m))
	for k, v := range m {
		t = append(t, k, v)
	}
	return t
}

// Set adds or updates the value for the given key.
func (t *OSMTags) Set(key, val string) {
	if i, ok := t.index(key); ok {
		t.set(i+1, val)
	} else {
		*t = append(*t, key, val)
	}
}

// Get returns the value for the given key or false if the key does not exist.
func (t *OSMTags) Get(key string) (string, bool) {
	if i, ok := t.index(key); ok {
		return t.get(i + 1), true
	}
	return "", false
}

// Has returns true if the given key exists.
func (t *OSMTags) Has(key string) bool {
	_, ok := t.index(key)
	return ok
}

// HasValue return true if the given key exists and its value is val.
func (t *OSMTags) HasValue(key, val string) bool {
	if i, ok := t.index(key); ok {
		return t.get(i+1) == val
	}
	return false
}

// Map returns the map representation of the tags.
func (t *OSMTags) Map() map[string]string {
	m := make(map[string]string, t.Len())
	for i := 0; i < len(*t); i += 2 {
		m[t.get(i)] = t.get(i + 1)
	}
	return m
}

// MarshalJSON prints the JSON representation of the tags.
func (t OSMTags) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Map())
}

// Len returns the number of tags.
func (t *OSMTags) Len() int {
	return len(*t) / 2
}

func (t *OSMTags) get(i int) string {
	return []string(*t)[i]
}

func (t *OSMTags) set(i int, s string) {
	[]string(*t)[i] = s
}

func (t *OSMTags) index(key string) (int, bool) {
	for i := 0; i < len(*t); i += 2 {
		if t.get(i) == key {
			return i, true
		}
	}
	return -1, false
}
