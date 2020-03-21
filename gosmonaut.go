package gosmonaut

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"
)

/* Gosmonaut */
type osmPair struct {
	i OSMEntity
	e error
}

// Config defines the configuration for Gosmonaut.
type Config struct {
	// DebugMode prints duration and memory info and runs the garbage collector
	// after every processing step if enabled.
	DebugMode bool

	// PrintWarnings prints warnings to stdout if enabled. Possible warnings
	// include missing referenced entites or unsupported features.
	PrintWarnings bool

	// Set the number of processes that are used for decoding.
	// If not set the amount of available logical CPUs will be used.
	NumProcessors int

	// Decoder sets the PBF blob decoder. Defaults to `FastDecoder`.
	Decoder DecoderType

	// Set an option to skip nodes in case they are missed in OSM file.
	// Defaults to false.
	SkipMissingNodes bool
}

// Gosmonaut is responsible for decoding an OpenStreetMap pbf file.
// For creating an instance the NewGosmonaut() function must be used.
type Gosmonaut struct {
	dec    *decoder
	stream chan osmPair
	lock   sync.Mutex

	// Store header block
	header Header

	// Defined by caller
	types            OSMTypeSet
	funcEntityNeeded func(OSMType, OSMTags) bool

	// ID trackers
	nodeIDTracker, wayIDTracker idTracker

	// Entitiy caches
	nodeCache *binaryNodeEntityMap
	wayCache  *binaryWayEntityMap

	// For debug mode
	debugMode     bool
	printWarnings bool
	timeStarted   time.Time
	timeLast      time.Time

	// Skip missing nodes while parsing ways
	skipMissingNodes bool
}

// NewGosmonaut creates a new Gosmonaut instance and parses the meta information
// (Header) of the given file. Either zero or exactly one `Config` object must
// be passed.
func NewGosmonaut(file io.ReadSeeker, config ...Config) (*Gosmonaut, error) {
	// Check config
	var conf Config
	if len(config) > 1 {
		return nil, errors.New("Only 1 Config object is allowed")
	} else if len(config) == 1 {
		conf = config[0]
	}

	// Get number of processes
	var nProcs int
	if conf.NumProcessors < 1 {
		nProcs = runtime.NumCPU()
	} else {
		nProcs = conf.NumProcessors
	}

	// Create decoder
	dec, header, err := newDecoder(file, nProcs, conf.Decoder)
	if err != nil {
		return nil, err
	}

	return &Gosmonaut{
		dec:              dec,
		header:           header,
		debugMode:        conf.DebugMode,
		printWarnings:    conf.PrintWarnings,
		skipMissingNodes: conf.SkipMissingNodes,
	}, nil
}

// Header returns the meta information of the PBF file.
func (g *Gosmonaut) Header() Header {
	return g.header
}

// Start starts the decoding process. The function call will block until the
// previous run has finished.
// Only types that are enabled in `types` will be sent to the caller.
// funcEntityNeeded will be called to determine if the caller needs a specific
// OSM entity.
// Found entities and encountered errors can be received by polling the Next()
// method.
func (g *Gosmonaut) Start(
	types OSMTypeSet,
	funcEntityNeeded func(OSMType, OSMTags) bool,
) {
	// Block until previous run finished
	g.lock.Lock()
	g.stream = make(chan osmPair, entitiesPerPrimitiveBlock)

	go func() {
		// Defer order is important
		defer g.lock.Unlock()
		defer close(g.stream)

		// Init vars
		g.funcEntityNeeded = funcEntityNeeded
		g.types = types

		g.nodeIDTracker = newBitsetIDTracker()
		g.wayIDTracker = newBitsetIDTracker()

		// Init debug vars
		{
			timeNow := time.Now()
			g.timeStarted = timeNow
			g.timeLast = timeNow
		}
		g.printDebugInfo("Decoding started")

		// Scan relation dependencies
		if g.types.Get(RelationType) {
			if err := g.scanRelationDependencies(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo(fmt.Sprintf("Scanned relation dependencies [length: %d]", g.wayIDTracker.len()))
		}

		// Scan way dependencies
		if g.types.Get(WayType) || g.wayIDTracker.len() != 0 {
			if err := g.scanWayDependencies(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo(fmt.Sprintf("Scanned way dependencies [length: %d]", g.nodeIDTracker.len()))
		}

		g.nodeCache = newBinaryNodeEntityMap(g.nodeIDTracker.len())
		g.printDebugInfo("Created node cache")

		// Scan nodes
		if g.types.Get(NodeType) || g.nodeIDTracker.len() != 0 {
			if err := g.scanNodes(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned nodes")
		}

		g.nodeIDTracker = nil
		g.printDebugInfo("Deleted node ID tracker")

		g.nodeCache.prepare()
		g.printDebugInfo("Prepared node cache")

		g.wayCache = newBinaryWayEntityMap(g.wayIDTracker.len())
		g.printDebugInfo("Created way cache")

		// Scan ways
		if g.types.Get(WayType) || g.wayIDTracker.len() != 0 {
			if err := g.scanWays(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned ways")
		}

		g.wayIDTracker = nil
		g.printDebugInfo("Deleted way ID tracker")

		g.wayCache.prepare()
		g.printDebugInfo("Prepared way cache")

		// Scan relations
		if g.types.Get(RelationType) {
			if err := g.scanRelations(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned relations")
		}

		g.wayCache = nil
		g.nodeCache = nil
		g.printDebugInfo("Deleted entity caches")

		if g.debugMode {
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
					g.wayIDTracker.set(id)
				case NodeType:
					g.nodeIDTracker.set(id)
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

			if g.wayIDTracker.get(id) || g.entityNeeded(WayType, tags) {
				// Add nodes to ID cache
				refs, err := d.refs()
				if err != nil {
					return err
				}
				for _, id := range refs {
					g.nodeIDTracker.set(id)
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
			n := Node{id, lat, lon, tags}

			// Add to node cache
			if g.nodeIDTracker.get(id) {
				g.nodeCache.add(n)
			}

			// Send to output stream
			if g.entityNeeded(NodeType, tags) {
				g.streamEntity(n)
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
			reqCache := g.wayIDTracker.get(id)
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
				if n, ok := g.nodeCache.get(rid); ok {
					nodes[i] = n
				} else {
					if g.skipMissingNodes {
						g.printWarning(fmt.Sprintf("Node #%d in not in file for way #%d", rid, id))
						continue
					}
					return fmt.Errorf("Node #%d in not in file for way #%d", rid, id)
				}
			}
			w := Way{id, tags, nodes}

			// Add to way cache
			if reqCache {
				g.wayCache.add(w)
			}

			// Send to output stream
			if reqStream {
				g.streamEntity(w)
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
					if w, ok := g.wayCache.get(mid); ok {
						e = w
					} else {
						g.printWarning(fmt.Sprintf("Way #%d in not in file for relation #%d", mid, id))
						continue
					}
				case NodeType:
					if n, ok := g.nodeCache.get(mid); ok {
						e = n
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
	if g.printWarnings {
		fmt.Println("Warning:", warning)
	}
}

func (g *Gosmonaut) printDebugInfo(state string) {
	if !g.debugMode {
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
