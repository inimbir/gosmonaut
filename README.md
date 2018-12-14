# gOSMonaut

gOSMonaut is a Go library that decodes OpenStreetMap PBF files. Instead of returning the internal PBF data model, which uses reference-IDs to nested OSM entities, it always returns complete entities. E.g. a way contains all child nodes, including tags and coordinates, instead of just the node-IDs.

## Output

All entities can be serialized into the JSON format. The used JSON format is similar to [Overpass JSON](http://overpass-api.de/output_formats.html#json) but it comes with nested entities. Here is an example of how a relation with nested member ways and nodes can look like:

```json
{
  "type": "relation",
  "id": 8024698,
  "tags": {
    "addr:city": "Leonberg",
    "addr:housenumber": "5",
    "addr:postcode": "71229",
    "addr:street": "Sellallee",
    "building": "apartments",
    "type": "multipolygon"
  },
  "members": [
    {
      "role": "outer",
      "entity": {
        "type": "way",
        "id": 561602115,
        "tags": {},
        "nodes": [
          {
            "type": "node",
            "id": 5414987608,
            "lat": 48.7994117,
            "lon": 9.0173691
          },
          {
            "type": "node",
            "id": 5414987609,
            "lat": 48.7994205,
            "lon": 9.0171999,
            "tags": {
              "entrance": "main"
            }
          }
        ]
      }
    }
  ]
}
```

## Installation

```bash
$ go get github.com/qedus/osmpbf
```

## Usage

Usage is similar to `json.Decoder`.

```Go
	f, err := os.Open("greater-london-140324.osm.pbf")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	d := osmpbf.NewDecoder(f)

	// use more memory from the start, it is faster
	d.SetBufferSize(osmpbf.MaxBlobSize)

	// start decoding with several goroutines, it is faster
	err = d.Start(runtime.GOMAXPROCS(-1))
	if err != nil {
		log.Fatal(err)
	}

	var nc, wc, rc uint64
	for {
		if v, err := d.Decode(); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		} else {
			switch v := v.(type) {
			case *osmpbf.Node:
				// Process Node v.
				nc++
			case *osmpbf.Way:
				// Process Way v.
				wc++
			case *osmpbf.Relation:
				// Process Relation v.
				rc++
			default:
				log.Fatalf("unknown type %T\n", v)
			}
		}
	}

	fmt.Printf("Nodes: %d, Ways: %d, Relations: %d\n", nc, wc, rc)
```

## Documentation

http://godoc.org/github.com/qedus/osmpbf

## To Do

The parseNodes code has not been tested as I can only find PBF files with DenseNode format.

An Encoder still needs to be created to reverse the process.
