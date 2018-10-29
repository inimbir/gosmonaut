package gosmonaut

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

const (
	// Test files are stored at https://gist.github.com/AlekSi/d4369aa13cf1fc5ddfac3e91b67b2f7b
	// 8604f36a7357adfbd6b5292c2ea4972d9d0bfd3d is the latest commit.
	GistURL = "https://gist.githubusercontent.com/AlekSi/d4369aa13cf1fc5ddfac3e91b67b2f7b/raw/8604f36a7357adfbd6b5292c2ea4972d9d0bfd3d/"

	// Originally downloaded from http://download.geofabrik.de/europe/great-britain/england/greater-london.html
	London = "greater-london-140324.osm.pbf"

	// Same file as above, but without 'DenseNodes'. This has been generated using the below command (using osmium-tool http://osmcode.org/osmium-tool/)
	// "osmium cat -o  greater-london-140324-nondense.osm.pbf greater-london-140324.osm.pbf -f osm.pbf,pbf_dense_nodes=false"
	LondonNonDense = "greater-london-140324-nondense.osm.pbf"
)

func TestGoDecoder(t *testing.T) {
	testGosmonautWithFile(t, London, GoDecoder)
}

func TestGoDecoderNonDense(t *testing.T) {
	testGosmonautWithFile(t, LondonNonDense, GoDecoder)
}

func TestFastDecoder(t *testing.T) {
	testGosmonautWithFile(t, London, FastDecoder)
}

func TestFastDecoderNonDense(t *testing.T) {
	testGosmonautWithFile(t, LondonNonDense, FastDecoder)
}

// Test results have been verified by Osmonaut v1.1
func testGosmonautWithFile(t *testing.T, filename string, decoder DecoderType) {
	if testing.Short() {
		t.Skip("Skipping decoding tests in short mode")
	}

	downloadTestOSMFile(filename, t)

	// Test addresses
	testGosmonaut(t, filename,
		NewOSMTypeSet(true, true, true),
		func(t OSMType, tags OSMTags) bool {
			return tags.Has("addr:housenumber")
		},
		decoder,
		428593, 63528, 97,
		loadTestdata(t, "addr_node.json"),
		loadTestdata(t, "addr_way.json"),
		loadTestdata(t, "addr_relation.json"),
	)

	// Test restrictions
	testGosmonaut(t, filename,
		NewOSMTypeSet(false, false, true),
		func(t OSMType, tags OSMTags) bool {
			return tags.HasValue("type", "restriction")
		},
		decoder,
		18143, 3181, 1517,
		"",
		"",
		loadTestdata(t, "restriction.json"),
	)

	testGosmonaut(t, filename,
		NewOSMTypeSet(false, false, false),
		func(t OSMType, tags OSMTags) bool {
			return true
		},
		decoder,
		0, 0, 0,
		"", "", "",
	)

	testGosmonaut(t, filename,
		NewOSMTypeSet(true, true, true),
		func(t OSMType, tags OSMTags) bool {
			return false
		},
		decoder,
		0, 0, 0,
		"", "", "",
	)
}

func testGosmonaut(
	t *testing.T,
	filename string,
	types OSMTypeSet,
	f func(OSMType, OSMTags) bool,
	decoder DecoderType,
	nc, wc, rc int, // Number of total entities per type
	ns, ws, rs string, // JSON string of the first entity per type
) {
	g := NewGosmonaut(filepath.Join("testdata", filename), types, f)
	g.Decoder = decoder
	g.Start()
	var nh, wh, rh bool
	var rnc, rwc, rrc int
	highestType := NodeType
	for {
		if i, err := g.Next(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		} else {
			// Check type order (nodes, then ways, then relations)
			if i.GetType() < highestType {
				t.Fatal("Type order is wrong")
			}
			highestType = i.GetType()

			switch i := i.(type) {
			case Node:
				// Check first entity
				if !nh {
					if i.String() != ns {
						t.Fatal("Node test failed")
					}
					nh = true
				}

				// Count entities
				rnc++
			case Way:
				// Check first entity
				if !wh {
					if i.String() != ws {
						t.Fatal("Way test failed")
					}
					wh = true
				}

				// Count entities
				rwc++
				rnc += len(i.Nodes)
			case Relation:
				// Check first entity
				if !rh {
					if i.String() != rs {
						t.Fatal("Relation test failed")
					}
					rh = true
				}

				// Count entities
				rrc++
				for _, m := range i.Members {
					switch mi := m.Entity.(type) {
					case Node:
						rnc++
					case Way:
						rwc++
						rnc += len(mi.Nodes)
					default:
						t.Fatalf("Invalid member entity type: %T", mi)
					}
				}
			default:
				t.Fatalf("Invalid entity type: %T", i)
			}
		}
	}

	// Check type counts
	if rnc != nc {
		t.Fatalf("Wrong number of nodes")
	}
	if rwc != wc {
		t.Fatalf("Wrong number of ways")
	}
	if rrc != rc {
		t.Fatalf("Wrong number of relations")
	}
}

func downloadTestOSMFile(fileName string, t *testing.T) {
	path := filepath.Join("testdata", fileName)

	_, err := os.Stat(path)
	if err == nil {
		return
	}
	if !os.IsNotExist(err) {
		t.Fatal(err)
	}

	resp, err := http.Get(GistURL + fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	if _, err = io.Copy(out, resp.Body); err != nil {
		t.Fatal(err)
	}
}

func loadTestdata(t *testing.T, filename string) string {
	b, err := ioutil.ReadFile(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
