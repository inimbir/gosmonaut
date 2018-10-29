package gosmonaut

import (
	"./OSMPBF"
	"errors"
	"github.com/golang/protobuf/proto"
	"io"
)

// goBlobDecoder uses the official Golang Protobuf package.
// All protobuf messages will be unmarshalled to temporary objects before
// processing.
type goBlobDecoder struct {
	q []entityParser
}

func (dec *goBlobDecoder) decode(blob *OSMPBF.Blob, t OSMType) ([]entityParser, OSMTypeSet, error) {
	data, err := getBlobData(blob)
	if err != nil {
		return nil, 0, err
	}

	primitiveBlock := new(OSMPBF.PrimitiveBlock)
	if err := proto.Unmarshal(data, primitiveBlock); err != nil {
		return nil, 0, err
	}

	// Get types
	types := dec.getTypes(primitiveBlock)
	if !types.Get(t) {
		return nil, types, nil
	}

	// Build entity parsers
	dec.q = make([]entityParser, 0, len(primitiveBlock.GetPrimitivegroup()))
	err = dec.parsePrimitiveBlock(primitiveBlock, t)
	return dec.q, types, err
}

func (dec *goBlobDecoder) getTypes(pb *OSMPBF.PrimitiveBlock) (types OSMTypeSet) {
	for _, pg := range pb.GetPrimitivegroup() {
		if len(pg.GetNodes()) > 0 || len(pg.GetDense().GetId()) > 0 {
			types.Set(NodeType, true)
		}
		if len(pg.GetWays()) > 0 {
			types.Set(WayType, true)
		}
		if len(pg.GetRelations()) > 0 {
			types.Set(RelationType, true)
		}
	}
	return
}

func (dec *goBlobDecoder) parsePrimitiveBlock(pb *OSMPBF.PrimitiveBlock, t OSMType) error {
	for _, pg := range pb.GetPrimitivegroup() {
		st := stringTable(pb.GetStringtable().GetS())
		var parser entityParser
		switch t {
		case NodeType:
			if len(pg.GetNodes()) > 0 {
				parser = newGoNodeParser(st, pb, pg.GetNodes())
			} else if len(pg.GetDense().GetId()) > 0 {
				var err error
				parser, err = newGoDenseNodesParser(st, pb, pg.GetDense())
				if err != nil {
					return err
				}
			}
		case WayType:
			parser = newGoWayParser(st, pb, pg.GetWays())
		case RelationType:
			parser = newGoRelationParser(st, pb, pg.GetRelations())
		}

		if parser != nil {
			dec.q = append(dec.q, parser)
		}
	}
	return nil
}

/* Node Parsers */
type goNodeParser struct {
	index                int
	granularity          int64
	latOffset, lonOffset int64
	st                   stringTable
	nodes                []*OSMPBF.Node
}

func newGoNodeParser(st stringTable, pb *OSMPBF.PrimitiveBlock, nodes []*OSMPBF.Node) *goNodeParser {
	return &goNodeParser{
		granularity: int64(pb.GetGranularity()),
		latOffset:   pb.GetLatOffset(),
		lonOffset:   pb.GetLonOffset(),
		st:          st,
		nodes:       nodes,
	}
}

func (d *goNodeParser) isEntityParser() {}

func (d *goNodeParser) next() (id int64, lat, lon float64, tags OSMTags, err error) {
	if d.index >= len(d.nodes) {
		err = io.EOF
		return
	}

	node := d.nodes[d.index]
	id = node.GetId()
	lat = decodeCoord(d.latOffset, d.granularity, node.GetLat())
	lon = decodeCoord(d.latOffset, d.granularity, node.GetLon())
	tags, err = extractTags(d.st, node.GetKeys(), node.GetVals())

	d.index++
	return
}

type goDenseNodesParser struct {
	index                int
	id, lat, lon         int64
	ids                  []int64
	granularity          int64
	latOffset, lonOffset int64
	lats, lons           []int64
	tu                   *tagUnpacker
}

func newGoDenseNodesParser(st stringTable, pb *OSMPBF.PrimitiveBlock, dn *OSMPBF.DenseNodes) (*goDenseNodesParser, error) {
	ids := dn.GetId()
	lats := dn.GetLat()
	lons := dn.GetLon()
	if len(ids) != len(lats) || len(ids) != len(lons) {
		return nil, errors.New("Length of dense nodes ids, lat and lons differs")
	}

	return &goDenseNodesParser{
		granularity: int64(pb.GetGranularity()),
		latOffset:   pb.GetLatOffset(),
		lonOffset:   pb.GetLonOffset(),
		ids:         ids,
		lats:        lats,
		lons:        lons,
		tu:          &tagUnpacker{st, dn.GetKeysVals(), 0},
	}, nil
}

func (d *goDenseNodesParser) isEntityParser() {}

func (d *goDenseNodesParser) next() (id int64, lat, lon float64, tags OSMTags, err error) {
	if d.index >= len(d.ids) {
		err = io.EOF
		return
	}

	d.id = d.ids[d.index] + d.id
	id = d.id

	d.lat = d.lats[d.index] + d.lat
	d.lon = d.lons[d.index] + d.lon
	lat = decodeCoord(d.latOffset, d.granularity, d.lat)
	lon = decodeCoord(d.lonOffset, d.granularity, d.lon)
	tags, err = d.tu.next()

	d.index++
	return
}

/* Way Parser */
type goWayParser struct {
	index int
	st    stringTable
	ways  []*OSMPBF.Way
	way   *OSMPBF.Way
}

func newGoWayParser(st stringTable, pb *OSMPBF.PrimitiveBlock, ways []*OSMPBF.Way) *goWayParser {
	return &goWayParser{
		st:   st,
		ways: ways,
	}
}

func (d *goWayParser) isEntityParser() {}

func (d *goWayParser) next() (id int64, tags OSMTags, err error) {
	if d.index >= len(d.ways) {
		err = io.EOF
		return
	}

	way := d.ways[d.index]
	id = way.GetId()
	tags, err = extractTags(d.st, way.GetKeys(), way.GetVals())
	d.way = way

	d.index++
	return
}

func (d *goWayParser) refs() ([]int64, error) {
	protoIDs := d.way.GetRefs()
	ids := make([]int64, len(protoIDs))
	var id int64
	for i, protoID := range protoIDs {
		id += protoID // delta encoding
		ids[i] = id
	}
	return ids, nil
}

/* Relation Parser */
type goRelationParser struct {
	index     int
	st        stringTable
	relations []*OSMPBF.Relation
	relation  *OSMPBF.Relation
}

func newGoRelationParser(st stringTable, pb *OSMPBF.PrimitiveBlock, relations []*OSMPBF.Relation) *goRelationParser {
	return &goRelationParser{
		st:        st,
		relations: relations,
	}
}

func (d *goRelationParser) isEntityParser() {}

func (d *goRelationParser) next() (id int64, tags OSMTags, err error) {
	if d.index >= len(d.relations) {
		err = io.EOF
		return
	}

	rel := d.relations[d.index]
	id = rel.GetId()
	tags, err = extractTags(d.st, rel.GetKeys(), rel.GetVals())
	d.relation = rel

	d.index++
	return
}

func (d *goRelationParser) ids() ([]int64, error) {
	protoIDs := d.relation.GetMemids()
	ids := make([]int64, len(protoIDs))
	var id int64
	for i, protoID := range protoIDs {
		id += protoID
		ids[i] = id
	}
	return ids, nil
}

func (d *goRelationParser) roles() ([]string, error) {
	protoRoles := d.relation.GetRolesSid()
	roles := make([]string, len(protoRoles))
	for i, protoRole := range protoRoles {
		if role, err := d.st.get(int(protoRole)); err == nil {
			roles[i] = role
		} else {
			return nil, err
		}
	}
	return roles, nil
}

func (d *goRelationParser) types() ([]OSMType, error) {
	protoTypes := d.relation.GetTypes()
	types := make([]OSMType, len(protoTypes))
	for i, protoT := range protoTypes {
		var t OSMType
		switch protoT {
		case OSMPBF.Relation_NODE:
			t = NodeType
		case OSMPBF.Relation_WAY:
			t = WayType
		case OSMPBF.Relation_RELATION:
			t = RelationType
		}
		types[i] = t
	}
	return types, nil
}

/* Tag Decoding */
// Make tags from stringtable and two parallel arrays of IDs.
func extractTags(st stringTable, keyIDs, valueIDs []uint32) (OSMTags, error) {
	if len(keyIDs) != len(valueIDs) {
		return nil, errors.New("Length of tag key and value arrays differs")
	}

	tags := NewOSMTags(len(keyIDs))
	for index, keyID := range keyIDs {
		key, val, err := st.extractTag(int(keyID), int(valueIDs[index]))
		if err != nil {
			return nil, err
		}
		tags.Set(key, val)
	}
	return tags, nil
}

type tagUnpacker struct {
	st       stringTable
	keysVals []int32
	index    int
}

// Make tags map from stringtable and array of IDs (used in DenseNodes encoding).
func (tu *tagUnpacker) next() (OSMTags, error) {
	if len(tu.keysVals) == 0 {
		return nil, nil
	}

	var tags OSMTags
	for {
		var key, val string

		if keyID, err := tu.read(); err != nil {
			return nil, err
		} else if keyID == 0 {
			break
		} else {
			if key, err = tu.st.get(keyID); err != nil {
				return nil, err
			}
		}

		if valID, err := tu.read(); err != nil {
			return nil, err
		} else if valID == 0 {
			return nil, errors.New("Dense nodes tag value is zero")
		} else {
			if val, err = tu.st.get(valID); err != nil {
				return nil, err
			}
		}

		tags.Set(key, val)
	}
	return tags, nil
}

func (tu *tagUnpacker) read() (int, error) {
	if tu.index >= len(tu.keysVals) {
		return 0, errors.New("Dense nodes tag index is out of bounds")
	}
	v := tu.keysVals[tu.index]
	tu.index++
	return int(v), nil
}
