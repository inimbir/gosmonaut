package gosmonaut

import (
	"./OSMPBF"
	"bytes"
	"compress/zlib"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
)

// Decoder for Blob with OSMData (PrimitiveBlock)
type dataDecoder struct {
	q []interface{}
}

func (dec *dataDecoder) Decode(blob *OSMPBF.Blob, t OSMType) ([]interface{}, OSMTypeSet, error) {
	data, err := getBlobData(blob)
	if err != nil {
		return nil, 0, err
	}

	primitiveBlock := new(OSMPBF.PrimitiveBlock)
	if err := proto.Unmarshal(data, primitiveBlock); err != nil {
		return nil, 0, err
	}

	// Count entities per type
	nc, wc, rc := dec.countTypes(primitiveBlock)
	types := NewOSMTypeSet(nc > 0, wc > 0, rc > 0)
	var n int
	switch t {
	case NodeType:
		n = nc
	case WayType:
		n = wc
	case RelationType:
		n = rc
	}

	// Build entities
	if n > 0 {
		dec.q = make([]interface{}, 0, n)
		dec.parsePrimitiveBlock(primitiveBlock, t)
	} else {
		dec.q = nil
	}
	return dec.q, types, nil
}

func (dec *dataDecoder) countTypes(pb *OSMPBF.PrimitiveBlock) (nc, wc, rc int) {
	for _, pg := range pb.GetPrimitivegroup() {
		nc += len(pg.GetNodes())
		nc += len(pg.GetDense().GetId())
		wc += len(pg.GetWays())
		rc += len(pg.GetRelations())
	}
	return
}

func (dec *dataDecoder) parsePrimitiveBlock(pb *OSMPBF.PrimitiveBlock, t OSMType) {
	for _, pg := range pb.GetPrimitivegroup() {
		dec.parsePrimitiveGroup(pb, pg, t)
	}
}

func (dec *dataDecoder) parsePrimitiveGroup(pb *OSMPBF.PrimitiveBlock, pg *OSMPBF.PrimitiveGroup, t OSMType) {
	switch t {
	case NodeType:
		dec.parseNodes(pb, pg.GetNodes())
		dec.parseDenseNodes(pb, pg.GetDense())
	case WayType:
		dec.parseWays(pb, pg.GetWays())
	case RelationType:
		dec.parseRelations(pb, pg.GetRelations())
	}
}

func (dec *dataDecoder) parseNodes(pb *OSMPBF.PrimitiveBlock, nodes []*OSMPBF.Node) {
	st := pb.GetStringtable().GetS()
	granularity := int64(pb.GetGranularity())

	latOffset := pb.GetLatOffset()
	lonOffset := pb.GetLonOffset()

	for _, node := range nodes {
		id := node.GetId()
		lat := node.GetLat()
		lon := node.GetLon()

		latitude := 1e-9 * float64((latOffset + (granularity * lat)))
		longitude := 1e-9 * float64((lonOffset + (granularity * lon)))

		tags := extractTags(st, node.GetKeys(), node.GetVals())

		dec.q = append(dec.q, &Node{id, latitude, longitude, tags})
	}

}

func (dec *dataDecoder) parseDenseNodes(pb *OSMPBF.PrimitiveBlock, dn *OSMPBF.DenseNodes) {
	st := pb.GetStringtable().GetS()
	granularity := int64(pb.GetGranularity())
	latOffset := pb.GetLatOffset()
	lonOffset := pb.GetLonOffset()
	ids := dn.GetId()
	lats := dn.GetLat()
	lons := dn.GetLon()

	tu := tagUnpacker{st, dn.GetKeysVals(), 0}
	var id, lat, lon int64
	for index := range ids {
		id = ids[index] + id
		lat = lats[index] + lat
		lon = lons[index] + lon
		latitude := 1e-9 * float64((latOffset + (granularity * lat)))
		longitude := 1e-9 * float64((lonOffset + (granularity * lon)))
		tags := tu.next()

		dec.q = append(dec.q, &Node{id, latitude, longitude, tags})
	}
}

func (dec *dataDecoder) parseWays(pb *OSMPBF.PrimitiveBlock, ways []*OSMPBF.Way) {
	st := pb.GetStringtable().GetS()

	for _, way := range ways {
		id := way.GetId()

		tags := extractTags(st, way.GetKeys(), way.GetVals())

		refs := way.GetRefs()
		var nodeID int64
		nodeIDs := make([]int64, len(refs))
		for index := range refs {
			nodeID = refs[index] + nodeID // delta encoding
			nodeIDs[index] = nodeID
		}

		dec.q = append(dec.q, rawWay{id, tags, nodeIDs})
	}
}

// Make relation members from stringtable and three parallel arrays of IDs.
func extractMembers(stringTable []string, rel *OSMPBF.Relation) []rawMember {
	memIDs := rel.GetMemids()
	types := rel.GetTypes()
	roleIDs := rel.GetRolesSid()

	var memID int64
	members := make([]rawMember, len(memIDs))
	for index := range memIDs {
		memID = memIDs[index] + memID // delta encoding

		var memType OSMType
		switch types[index] {
		case OSMPBF.Relation_NODE:
			memType = NodeType
		case OSMPBF.Relation_WAY:
			memType = WayType
		case OSMPBF.Relation_RELATION:
			memType = RelationType
		}

		role := stringTable[roleIDs[index]]

		members[index] = rawMember{memID, memType, role}
	}

	return members
}

func (dec *dataDecoder) parseRelations(pb *OSMPBF.PrimitiveBlock, relations []*OSMPBF.Relation) {
	st := pb.GetStringtable().GetS()

	for _, rel := range relations {
		id := rel.GetId()
		tags := extractTags(st, rel.GetKeys(), rel.GetVals())
		members := extractMembers(st, rel)

		dec.q = append(dec.q, rawRelation{id, tags, members})
	}
}

func getBlobData(blob *OSMPBF.Blob) ([]byte, error) {
	switch {
	case blob.Raw != nil:
		return blob.GetRaw(), nil

	case blob.ZlibData != nil:
		r, err := zlib.NewReader(bytes.NewReader(blob.GetZlibData()))
		if err != nil {
			return nil, err
		}
		buf := bytes.NewBuffer(make([]byte, 0, blob.GetRawSize()+bytes.MinRead))
		_, err = buf.ReadFrom(r)
		if err != nil {
			return nil, err
		}
		if buf.Len() != int(blob.GetRawSize()) {
			err = fmt.Errorf("raw blob data size %d but expected %d", buf.Len(), blob.GetRawSize())
			return nil, err
		}
		return buf.Bytes(), nil

	default:
		return nil, errors.New("unknown blob data")
	}
}
