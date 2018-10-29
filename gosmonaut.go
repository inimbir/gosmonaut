package gosmonaut

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"
)

/* Gosmonaut */
type osmPair struct {
	i OSMEntity
	e error
}

// Gosmonaut is responsible for decoding an OpenStreetMap pbf file.
// For creating an instance the NewGosmonaut() function must be used.
type Gosmonaut struct {
	dec    *decoder
	stream chan osmPair

	// Defined by caller
	file             io.ReadSeeker
	types            OSMTypeSet
	funcEntityNeeded func(OSMType, OSMTags) bool

	// Entitiy caches
	nodeCache map[int64]*Node
	wayCache  map[int64]*Way

	// For debug mode
	timeStarted time.Time
	timeLast    time.Time

	// DebugMode prints warnings during decoding.
	// Also duration and memory info will be printed after every processing step
	// and the garbage collector is run. This variable should not be changed
	// after running Start().
	DebugMode bool

	// Set the number of processes that are used for decoding.
	// If not set the amount of available logical CPUs will be used.
	NumProcessors int

	// Decoder sets the PBF blob decoder.
	Decoder DecoderType
}

// NewGosmonaut creates a new Gosmonaut instance.
// Only types that are enabled in `types` will be sent to the caller.
// funcEntityNeeded will be called to determine if the caller needs a specific
// OSM entity.
func NewGosmonaut(
	file io.ReadSeeker,
	types OSMTypeSet,
	funcEntityNeeded func(OSMType, OSMTags) bool,
) *Gosmonaut {
	return &Gosmonaut{
		stream:           make(chan osmPair, entitiesPerPrimitiveBlock),
		file:             file,
		types:            types,
		funcEntityNeeded: funcEntityNeeded,
		nodeCache:        map[int64]*Node{},
		wayCache:         map[int64]*Way{},
		Decoder:          FastDecoder,
	}
}

// Start starts the decoding process (non-blocking).
// Found entities and encountered errors can be received by polling the Next()
// function.
func (g *Gosmonaut) Start() {
	go func() {
		{
			timeNow := time.Now()
			g.timeStarted = timeNow
			g.timeLast = timeNow
		}
		g.printDebugInfo("Decoding started")

		defer close(g.stream)

		// Determine number of processes
		var nProcs int
		if g.NumProcessors != 0 {
			nProcs = g.NumProcessors
		} else {
			nProcs = runtime.NumCPU()
		}

		// Create decoder
		g.dec = newDecoder(g.file, nProcs, g.Decoder)

		// Scan relation dependencies
		if g.types.Get(RelationType) {
			if err := g.scanRelationDependencies(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo(fmt.Sprintf("Scanned relation dependencies [length: %d]", len(g.wayCache)))
		}

		// Scan way dependencies
		if g.types.Get(WayType) || len(g.wayCache) != 0 {
			if err := g.scanWayDependencies(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo(fmt.Sprintf("Scanned way dependencies [length: %d]", len(g.nodeCache)))
		}

		// Scan nodes
		if g.types.Get(NodeType) || len(g.nodeCache) != 0 {
			if err := g.scanNodes(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned nodes")
		}

		// Scan ways
		if g.types.Get(WayType) || len(g.wayCache) != 0 {
			if err := g.scanWays(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned ways")
		}

		// Scan relations
		if g.types.Get(RelationType) {
			if err := g.scanRelations(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned relations")
		}

		if g.DebugMode {
			fmt.Println("Elapsed time:", time.Since(g.timeStarted))
		}
	}()
}

func (g *Gosmonaut) streamError(err error) {
	g.stream <- osmPair{nil, err}
}

func (g *Gosmonaut) streamEntity(i OSMEntity) {
	g.stream <- osmPair{i, nil}
}

// Next returns the next decoded entity (x)or an error.
// If the error is io.EOF the file has successfully been decoded.
// If the error is not EOF decoding has been stopped due to another error.
func (g *Gosmonaut) Next() (OSMEntity, error) {
	p, ok := <-g.stream
	if !ok {
		return nil, io.EOF
	}
	return p.i, p.e
}

func (g *Gosmonaut) entityNeeded(t OSMType, tags OSMTags) bool {
	if !g.types.Get(t) {
		return false
	}
	return g.funcEntityNeeded(t, tags)
}

func (g *Gosmonaut) scanRelationDependencies() error {
	return g.scan(RelationType, func(v interface{}) error {
		d, ok := v.(relationParser)
		if !ok {
			return fmt.Errorf("Got invalid relation parser (%T)", v)
		}

		for {
			_, tags, err := d.next()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			if !g.entityNeeded(RelationType, tags) {
				continue
			}

			// Add members to ID caches
			ids, err := d.ids()
			if err != nil {
				return err
			}
			types, err := d.types()
			if err != nil {
				return err
			}
			if len(ids) != len(types) {
				return errors.New("Length of relation ids and types differs")
			}
			for i, id := range ids {
				switch types[i] {
				case WayType:
					g.wayCache[id] = nil
				case NodeType:
					g.nodeCache[id] = nil
					// We don't support sub-relations yet
				}
			}
		}
		return nil
	})
}

func (g *Gosmonaut) scanWayDependencies() error {
	return g.scan(WayType, func(v interface{}) error {
		d, ok := v.(wayParser)
		if !ok {
			return fmt.Errorf("Got invalid way parser (%T)", v)
		}

		for {
			id, tags, err := d.next()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			if _, ok := g.wayCache[id]; ok || g.entityNeeded(WayType, tags) {
				// Add nodes to ID cache
				refs, err := d.refs()
				if err != nil {
					return err
				}
				for _, id := range refs {
					g.nodeCache[id] = nil
				}
			}
		}
		return nil
	})
}

func (g *Gosmonaut) scanNodes() error {
	return g.scan(NodeType, func(v interface{}) error {
		d, ok := v.(nodeParser)
		if !ok {
			return fmt.Errorf("Got invalid node parser (%T)", v)
		}

		for {
			id, lat, lon, tags, err := d.next()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			// Add to node cache
			if _, ok := g.nodeCache[id]; ok {
				g.nodeCache[id] = &Node{id, lat, lon, tags}
			}

			// Send to output stream
			if g.entityNeeded(NodeType, tags) {
				g.streamEntity(Node{id, lat, lon, tags})
			}
		}
		return nil
	})
}

func (g *Gosmonaut) scanWays() error {
	return g.scan(WayType, func(v interface{}) error {
		d, ok := v.(wayParser)
		if !ok {
			return fmt.Errorf("Got invalid way parser (%T)", v)
		}

		for {
			id, tags, err := d.next()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			// Needed by cache or stream?
			_, reqCache := g.wayCache[id]
			reqStream := g.entityNeeded(WayType, tags)
			if !reqCache && !reqStream {
				continue
			}

			// Build nodes
			refs, err := d.refs()
			if err != nil {
				return err
			}
			nodes := make([]Node, len(refs))
			for i, rid := range refs {
				if n, ok := g.nodeCache[rid]; ok && n != nil {
					nodes[i] = *n
				} else {
					return fmt.Errorf("Node #%d in not in file for way #%d", rid, id)
				}
			}

			// Add to way cache
			if reqCache {
				g.wayCache[id] = &Way{id, tags, nodes}
			}

			// Send to output stream
			if reqStream {
				g.streamEntity(Way{id, tags, nodes})
			}
		}
		return nil
	})
}

func (g *Gosmonaut) scanRelations() error {
	return g.scan(RelationType, func(v interface{}) error {
		d, ok := v.(relationParser)
		if !ok {
			return fmt.Errorf("Got invalid relation parser (%T)", v)
		}

		for {
			id, tags, err := d.next()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			// Needed by stream?
			if !g.entityNeeded(RelationType, tags) {
				continue
			}

			// Build members
			ids, err := d.ids()
			if err != nil {
				return err
			}
			types, err := d.types()
			if err != nil {
				return err
			}
			roles, err := d.roles()
			if err != nil {
				return err
			}
			if len(ids) != len(types) || len(ids) != len(roles) {
				return errors.New("Length of relation ids, roles and types differs")
			}
			members := make([]Member, 0, len(ids))
			for i, mid := range ids {
				var e OSMEntity
				switch types[i] {
				case WayType:
					if w, ok := g.wayCache[mid]; ok && w != nil {
						e = *w
					} else {
						g.printWarning(fmt.Sprintf("Way #%d in not in file for relation #%d", mid, id))
						continue
					}
				case NodeType:
					if n, ok := g.nodeCache[mid]; ok && n != nil {
						e = *n
					} else {
						g.printWarning(fmt.Sprintf("Node #%d in not in file for relation #%d", mid, id))
						continue
					}
				default:
					// We don't support sub-relations yet
					g.printWarning(fmt.Sprintf("Skipping sub-relation #%d in relation #%d (not supported)", mid, id))
					continue
				}
				members = append(members, Member{roles[i], e})
			}

			// Send to output stream
			g.streamEntity(Relation{id, tags, members})
		}
		return nil
	})
}

func (g *Gosmonaut) scan(t OSMType, receiver func(v interface{}) error) error {
	if err := g.dec.Start(t); err != nil {
		return err
	}

	// Decode file
	for {
		if parsers, err := g.dec.nextPair(); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			// Send to receiver
			for _, v := range parsers {
				if err := receiver(v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

/* Debug Mode */
func (g *Gosmonaut) printWarning(warning string) {
	if g.DebugMode {
		fmt.Println("Warning:", warning)
	}
}

func (g *Gosmonaut) printDebugInfo(state string) {
	if !g.DebugMode {
		return
	}
	elapsed := time.Since(g.timeLast).Seconds()

	// Run garbage collector
	runtime.GC()

	// Print memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB\tTotalAlloc = %v MiB\tSys = %v MiB\tNumGC = %v\n",
		bToMb(m.Alloc),
		bToMb(m.TotalAlloc),
		bToMb(m.Sys),
		m.NumGC,
	)

	// Print elapsed
	fmt.Printf("%.4fs - %v\n", elapsed, state)
	g.timeLast = time.Now()
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
