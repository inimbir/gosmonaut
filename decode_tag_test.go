package gosmonaut

import (
	"reflect"
	"testing"
)

func TestTags(t *testing.T) {
	tags := NewOSMTags(-1)
	testTagsWithMap(t, tags, map[string]string{})

	var tags2 OSMTags
	tags2.Set("A", "5")
	tags2.Set("a", "5")
	testTagsWithMap(t, tags2, map[string]string{
		"a": "5",
		"A": "5",
	})

	tags3 := NewOSMTags(100)
	tags3.Set("created_by", "test")
	tags3.Set("addr:housenumber", "170-232")
	tags3.Set("addr:interpolation", "all")
	tags3.Set("addr:interpolation", "all")
	tags3.Set("created_by", "Potlatch 0.5d")
	testTagsWithMap(t, tags3, map[string]string{
		"addr:housenumber":   "170-232",
		"addr:interpolation": "all",
		"created_by":         "Potlatch 0.5d",
	})
}

func testTagsWithMap(t *testing.T, tags OSMTags, m map[string]string) {
	// Check length
	if tags.Len() != len(m) {
		t.Error("Wrong length")
	}

	// Test map
	if !reflect.DeepEqual(tags.Map(), m) {
		t.Error("Returned wrong map")
	}

	for k, v := range m {
		// Test get
		if val, ok := tags.Get(k); ok {
			if val != v {
				t.Error("Got wrong value")
			}
		} else {
			t.Error("Did not get value")
		}

		// Test has
		if !tags.Has(k) {
			t.Error("Does not contain key")
		}

		// Test has value
		if !tags.HasValue(k, v) {
			t.Error("Does not contain key/value")
		}
	}

	// False positives
	s := []string{
		"",
		"test",
		"ADDR:housenumber",
		"321",
	}
	for _, k := range s {
		if _, ok := m[k]; ok {
			continue
		}

		// Test get
		if _, ok := tags.Get(k); ok {
			t.Error("Got value for non-existing key")
		}

		// Test has
		if tags.Has(k) {
			t.Error("Contains non-existing key")
		}
	}

	// Rebuild from map
	tagsNew := NewOSMTagsFromMap(tags.Map())
	if !reflect.DeepEqual(tagsNew.Map(), m) {
		t.Error("Rebuild tags from map are wrong")
	}
}
