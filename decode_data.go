package gosmonaut

import (
	"./OSMPBF"
	"github.com/golang/protobuf/proto"
)

// Decoder for Blob with OSMData (PrimitiveBlock)
type dataDecoder struct {
	q []interface{}
}

func (dec *dataDecoder) Decode(blob *OSMPBF.Blob, t OSMType) ([]interface{}, error) {
	dec.q = make([]interface{}, 0, 8000) // typical PrimitiveBlock contains 8k OSM entities

	data, err := getData(blob)
	if err != nil {
		return nil, err
	}

	primitiveBlock := &OSMPBF.PrimitiveBlock{}
	if err := proto.Unmarshal(data, primitiveBlock); err != nil {
		return nil, err
	}

	dec.parsePrimitiveBlock(primitiveBlock, t)
	return dec.q, nil
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

		dec.q = append(dec.q, Node{id, latitude, longitude, tags})
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

		dec.q = append(dec.q, Node{id, latitude, longitude, tags})
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
