package gosmonaut

import (
	"testing"
)

func TestOsmTypes(t *testing.T) {
	// Construct some relation that contains all OSM types
	relation := Relation{-10000,
		map[string]string{
			"type": "multipolygon",
			"abc":  "xyz",
		},
		[]Member{
			Member{"outer",
				Way{123,
					map[string]string{
						"point": "123.345",
					},
					[]Node{
						Node{1, 2, 3,
							map[string]string{
								"abc":        "xyz",
								"test:hello": "",
							},
						},
						Node{-100, -1.234567812345678, 0, map[string]string{}},
						Node{0, 0.5, 1.234567812345678, nil},
					},
				},
			},
			Member{"inner",
				Node{5, 2, 3, map[string]string{}},
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
