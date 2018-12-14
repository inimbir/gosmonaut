# gOSMonaut

gOSMonaut is a Go library that decodes OpenStreetMap PBF files. Instead of returning the internal PBF data model, which uses reference-IDs of nested OSM entities, it always returns complete entities. E.g. a way contains all child nodes, including tags and coordinates, instead of just the node-IDs.

## Installation

```bash
$ go get github.com/morbz/gosmonaut
```

## Usage

In order to produce complete entities all nested entites are cached in memory. To keep the used memory low there are two filters that the decoder uses to decide which entites it needs to cache. They are passed to the `Start()` method of the Gosmonaut object. Only entites that satisfy both filters will be send to the caller:

`types OSMTypeSet`: Defines which OSM element types are requested (node, way, relation)
`funcEntityNeeded func(OSMType, OSMTags) bool`: A callback function that the decoder will call during decoding. Based on the element type and tags, this function decides if the entity is needed.

### Sample

In this example the program will output all the street names of London:

```Go
package main

import (
	"io"
	"log"
	"os"

	"github.com/MorbZ/gosmonaut"
)

func main() {
	// Open the PBF file
	f, err := os.Open("greater-london-140324.osm.pbf")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Create the Gosmonaut object
	g, err := gosmonaut.NewGosmonaut(f)
	if err != nil {
		log.Fatal(err)
	}

	// Start decoding
	g.Start(
		// We only want to receive ways (nodes, ways, relations)
		gosmonaut.NewOSMTypeSet(false, true, false),

		// Only request highways with a name
		func(t gosmonaut.OSMType, tags gosmonaut.OSMTags) bool {
			return tags.Has("highway") && tags.Has("name")
		},
	)

	// Receive found entities
	for {
		if i, err := g.Next(); err == io.EOF {
			// Last entity received
			break
		} else if err != nil {
			// Error during encoding
			log.Fatal(err)
		} else {
			// Received entity, print name
			tags := i.GetTags()
			log.Println(tags.Get("name"))
		}
	}
}
```

### Configuration

When creating the Gosmonaut object, an optional configuration object can be passed.

Instead of:

```go
gosmonaut.NewGosmonaut(f)
```

the configuration can be passed by calling:

```go
gosmonaut.NewGosmonaut(
	f,
	gosmonaut.Config{
		PrintWarnings: true,
		NumProcessors: 2,
	},
)
```

See the [documentation](https://godoc.org/github.com/MorbZ/gosmonaut#Config) for possible configuration parameters.
 
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

## Documentation

[https://godoc.org/github.com/MorbZ/gosmonaut](https://godoc.org/github.com/MorbZ/gosmonaut)
