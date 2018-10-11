package gosmonaut

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"time"
)

/* ID Cache */
type idCache map[int64]struct{}

func newIDCache() idCache {
	return idCache{}
}

func (c idCache) add(id int64) {
	c[id] = struct{}{}
}

func (c idCache) contains(id int64) bool {
	_, ok := c[id]
	return ok
}

func (c idCache) len() int {
	return len(c)
}

/* Gosmonaut */
type osmPair struct {
	i OSMEntity
	e error
}

// Gosmonaut is responsible for decoding an OpenStreetMap pbf file.
// For creating an instance the NewGosmonaut() function must be used.
type Gosmonaut struct {
	stream           chan osmPair
	filename         string
	types            OSMTypeSet
	funcEntityNeeded func(OSMType, map[string]string) bool
	nodeIDCache      idCache
	nodeCache        map[int64]Node
	wayIDCache       idCache
	wayCache         map[int64]Way
	timeStarted      time.Time
	timeLast         time.Time

	// DebugMode prints warnings during decoding.
	// Also duration and memory info will be printed after every processing step
	// and the garbage collector is run. This variable should not be changed
	// after running Start().
	DebugMode bool

	// Set the number of processes that are used for decoding.
	// If not set the amount of available logical CPUs will be used.
	NumProcessors int
}

// NewGosmonaut creates a new Gosmonaut instance.
// Only types that are enabled in `types` will be sent to the caller.
// funcEntityNeeded will be called to determine if the caller needs a specific
// OSM entity.
func NewGosmonaut(
	filename string,
	types OSMTypeSet,
	funcEntityNeeded func(OSMType, map[string]string) bool,
) *Gosmonaut {
	return &Gosmonaut{
		stream:           make(chan osmPair, entitiesPerPrimitiveBlock),
		filename:         filename,
		types:            types,
		funcEntityNeeded: funcEntityNeeded,
		nodeIDCache:      newIDCache(),
		wayIDCache:       newIDCache(),
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

		// Scan relation dependencies
		if g.types.Get(RelationType) {
			if err := g.scanRelationDependencies(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned relation dependencies")
		}

		// Create way cache
		if g.wayIDCache.len() != 0 {
			g.wayCache = make(map[int64]Way, g.wayIDCache.len())
			g.printDebugInfo(fmt.Sprintf("Created way cache [length: %d]", g.wayIDCache.len()))
		}

		// Scan way dependencies
		if g.types.Get(WayType) || g.wayIDCache.len() != 0 {
			if err := g.scanWayDependencies(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned way dependencies")
		}

		// Create node cache
		if g.nodeIDCache.len() != 0 {
			g.nodeCache = make(map[int64]Node, g.nodeIDCache.len())
			g.printDebugInfo(fmt.Sprintf("Created node cache [length: %d]", g.nodeIDCache.len()))
		}

		// Scan nodes
		if g.types.Get(NodeType) || g.nodeIDCache.len() != 0 {
			if err := g.scanNodes(); err != nil {
				g.streamError(err)
				return
			}
			g.printDebugInfo("Scanned nodes")
		}

		// Scan ways
		if g.types.Get(WayType) || g.wayIDCache.len() != 0 {
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
		close(g.stream)
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

func (g *Gosmonaut) entityNeeded(t OSMType, tags map[string]string) bool {
	if !g.types.Get(t) {
		return false
	}
	return g.funcEntityNeeded(t, tags)
}

func (g *Gosmonaut) scanRelationDependencies() error {
	return g.scan(RelationType, func(v interface{}) error {
		r, ok := v.(rawRelation)
		if !ok {
			return fmt.Errorf("Got invalid relation from decoder (%T)", v)
		}

		if g.entityNeeded(RelationType, r.Tags) {
			// Add members to ID caches
			for _, m := range r.Members {
				switch m.Type {
				case WayType:
					g.wayIDCache.add(m.ID)
				case NodeType:
					g.nodeIDCache.add(m.ID)
				case RelationType:
					// We don't support sub-relations yet
				}
			}
		}
		return nil
	})
}

func (g *Gosmonaut) scanWayDependencies() error {
	return g.scan(WayType, func(v interface{}) error {
		w, ok := v.(rawWay)
		if !ok {
			return fmt.Errorf("Got invalid way from decoder (%T)", v)
		}

		if g.wayIDCache.contains(w.ID) || g.entityNeeded(WayType, w.Tags) {
			// Add nodes to ID cache
			for _, id := range w.NodeIDs {
				g.nodeIDCache.add(id)
			}
		}
		return nil
	})
}

func (g *Gosmonaut) scanNodes() error {
	return g.scan(NodeType, func(v interface{}) error {
		n, ok := v.(Node)
		if !ok {
			return fmt.Errorf("Got invalid node from decoder (%T)", v)
		}

		// Add to node cache
		if g.nodeIDCache.contains(n.ID) {
			g.nodeCache[n.ID] = n
		}

		// Send to output stream
		if g.entityNeeded(NodeType, n.Tags) {
			g.streamEntity(n)
		}
		return nil
	})
}

func (g *Gosmonaut) scanWays() error {
	return g.scan(WayType, func(v interface{}) error {
		raw, ok := v.(rawWay)
		if !ok {
			return fmt.Errorf("Got invalid way from decoder (%T)", v)
		}

		// Needed by cache or stream?
		if !g.wayIDCache.contains(raw.ID) && !g.entityNeeded(WayType, raw.Tags) {
			return nil
		}

		// Build nodes
		nodes := make([]Node, 0, len(raw.NodeIDs))
		for _, id := range raw.NodeIDs {
			if n, ok := g.nodeCache[id]; ok {
				nodes = append(nodes, n)
			} else {
				return fmt.Errorf("Node #%d in not in file for way #%d", id, raw.ID)
			}
		}

		// Build way
		w := Way{raw.ID, raw.Tags, nodes}

		// Add to way cache
		if g.wayIDCache.contains(w.ID) {
			g.wayCache[w.ID] = w
		}

		// Send to output stream
		if g.entityNeeded(WayType, w.Tags) {
			g.streamEntity(w)
		}
		return nil
	})
}

func (g *Gosmonaut) scanRelations() error {
	return g.scan(RelationType, func(v interface{}) error {
		raw, ok := v.(rawRelation)
		if !ok {
			return fmt.Errorf("Got invalid relation from decoder (%T)", v)
		}

		// Needed by stream?
		if !g.entityNeeded(RelationType, raw.Tags) {
			return nil
		}

		// Build members
		members := make([]Member, 0, len(raw.Members))
		for _, rawm := range raw.Members {
			var i OSMEntity
			switch rawm.Type {
			case WayType:
				if w, ok := g.wayCache[rawm.ID]; ok {
					i = w
				} else {
					g.printWarning(fmt.Sprintf("Way #%d in not in file for relation #%d", rawm.ID, raw.ID))
					continue
				}
			case NodeType:
				if n, ok := g.nodeCache[rawm.ID]; ok {
					i = n
				} else {
					g.printWarning(fmt.Sprintf("Node #%d in not in file for relation #%d", rawm.ID, raw.ID))
					continue
				}
			default:
				// We don't support sub-relations yet
				g.printWarning(fmt.Sprintf("Skipping sub-relation #%d in relation #%d (not supported)", rawm.ID, raw.ID))
				continue
			}
			m := Member{rawm.Role, i}
			members = append(members, m)
		}

		// Build relation
		r := Relation{raw.ID, raw.Tags, members}

		// Send to output stream
		g.streamEntity(r)
		return nil
	})
}

func (g *Gosmonaut) scan(t OSMType, receiver func(v interface{}) error) error {
	// Open file
	f, err := os.Open(g.filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Determine number of processes
	var nProcs int
	if g.NumProcessors != 0 {
		nProcs = g.NumProcessors
	} else {
		nProcs = runtime.NumCPU()
	}

	// Create decoder
	d := NewDecoder(f)
	d.SetBufferSize(MaxBlobSize)
	if err := d.Start(nProcs); err != nil {
		return err
	}

	// Decode file
	for {
		if v, err := d.Decode(); err == io.EOF {
			break
		} else if err != nil {
			return err
		} else {
			switch v := v.(type) {
			case Node:
				if t != NodeType {
					continue
				}
			case rawWay:
				if t != WayType {
					continue
				}
			case rawRelation:
				if t != RelationType {
					continue
				}
			default:
				return fmt.Errorf("Unknown type %T", v)
			}

			// Send to receiver
			if err := receiver(v); err != nil {
				return err
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
