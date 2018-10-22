package gosmonaut

import (
	"reflect"
	"testing"
)

func TestOsmTypes(t *testing.T) {
	// Construct some relation that contains all OSM types
	relation := Relation{-10000,
		NewOSMTagsFromMap(map[string]string{
			"type": "multipolygon",
			"abc":  "xyz",
		}),
		[]Member{
			Member{"outer",
				Way{123,
					NewOSMTagsFromMap(map[string]string{
						"point": "123.345",
					}),
					[]Node{
						Node{1, 2, 3,
							NewOSMTagsFromMap(map[string]string{
								"abc":        "xyz",
								"test:hello": "",
							}),
						},
						Node{-100, -1.234567812345678, 0, NewOSMTags(0)},
						Node{-99, 0, 0, NewOSMTagsFromMap(nil)},
						Node{0, 0.5, 1.234567812345678, nil},
					},
				},
			},
			Member{"inner",
				Node{5, 2, 3, NewOSMTagsFromMap(map[string]string{})},
			},
		},
	}
	expected := `{
  "type": "relation",
  "id": -10000,
  "tags": {
    "abc": "xyz",
    "type": "multipolygon"
  },
  "members": [
    {
      "role": "outer",
      "entity": {
        "type": "way",
        "id": 123,
        "tags": {
          "point": "123.345"
        },
        "nodes": [
          {
            "type": "node",
            "id": 1,
            "lat": 2.0000000,
            "lon": 3.0000000,
            "tags": {
              "abc": "xyz",
              "test:hello": ""
            }
          },
          {
            "type": "node",
            "id": -100,
            "lat": -1.2345678,
            "lon": 0.0000000
          },
          {
            "type": "node",
            "id": -99,
            "lat": 0.0000000,
            "lon": 0.0000000
          },
          {
            "type": "node",
            "id": 0,
            "lat": 0.5000000,
            "lon": 1.2345678
          }
        ]
      }
    },
    {
      "role": "inner",
      "entity": {
        "type": "node",
        "id": 5,
        "lat": 2.0000000,
        "lon": 3.0000000
      }
    }
  ]
}
`
	if relation.String() != expected {
		t.Error("Relation doesn't match expected JSON output")
	}
}

/* OSM Type Set */
func TestOsmTypeSet(t *testing.T) {
	var set OSMTypeSet
	testOsmTypeSet(t, set, false, false, false)

	set.Set(NodeType, true)
	testOsmTypeSet(t, set, true, false, false)

	set.Set(WayType, true)
	set.Set(RelationType, true)
	testOsmTypeSet(t, set, true, true, true)

	set.Set(WayType, false)
	set.Set(NodeType, false)
	testOsmTypeSet(t, set, false, false, true)

	set = NewOSMTypeSet(true, false, true)
	testOsmTypeSet(t, set, true, false, true)
}

func testOsmTypeSet(t *testing.T, set OSMTypeSet, nodeEnabled, wayEnabled, relationEnabled bool) {
	if set.Get(NodeType) != nodeEnabled {
		t.Error("Node enabled is wrong")
	}
	if set.Get(WayType) != wayEnabled {
		t.Error("Way enabled is wrong")
	}
	if set.Get(RelationType) != relationEnabled {
		t.Error("Relation enabled is wrong")
	}
}

/* OSM Tags */
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
