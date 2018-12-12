package gosmonaut

import (
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
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
	// Open file
	file, err := os.Open(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	g, _ := NewGosmonaut(file, Config{
		Decoder: decoder,
	})

	// Run test twice in order to test repeated runs
	for i := 0; i < 2; i++ {
		g.Start(types, f)
		var nh, wh, rh bool
		var rnc, rwc, rrc int
		highestType := NodeType
		for {
			if e, err := g.Next(); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			} else {
				// Check type order (nodes, then ways, then relations)
				if e.GetType() < highestType {
					t.Fatal("Type order is wrong")
				}
				highestType = e.GetType()

				switch e := e.(type) {
				case Node:
					// Check first entity
					if !nh {
						if e.String() != ns {
							t.Fatal("Node test failed")
						}
						nh = true
					}

					// Count entities
					rnc++
				case Way:
					// Check first entity
					if !wh {
						if e.String() != ws {
							t.Fatal("Way test failed")
						}
						wh = true
					}

					// Count entities
					rwc++
					rnc += len(e.Nodes)
				case Relation:
					// Check first entity
					if !rh {
						if e.String() != rs {
							t.Fatal("Relation test failed")
						}
						rh = true
					}

					// Count entities
					rrc++
					for _, m := range e.Members {
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
}

func TestHeader(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping decoding tests in short mode")
	}

	downloadTestOSMFile(London, t)

	header := Header{
		BoundingBox: &BoundingBox{
			-0.511482,
			0.33543700000000004,
			51.69344,
			51.285540000000005,
		},
		RequiredFeatures: []string{
			"OsmSchema-V0.6",
			"DenseNodes",
		},
		WritingProgram:              "Osmium (http://wiki.openstreetmap.org/wiki/Osmium)",
		OsmosisReplicationTimestamp: time.Unix(1395698102, 0),
	}

	// Open file
	file, err := os.Open(filepath.Join("testdata", London))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Compare headers
	g, err := NewGosmonaut(file)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(header, g.Header()) {
		t.Fatal("Header test failed")
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
