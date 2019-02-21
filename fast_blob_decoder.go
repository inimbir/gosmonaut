package gosmonaut

import (
	"errors"
	"github.com/MorbZ/gosmonaut/OSMPBF"
	"github.com/golang/protobuf/proto"
	"io"
)

const (
	protoPBStringTable    = 1
	protoPBPrimitiveGroup = 2
	protoPBGranularity    = 17
	protoPBLatOffset      = 19
	protoPBLonOffset      = 20

	protoSTString = 1

	protoPGNode       = 1
	protoPGDenseNodes = 2
	protoPGWay        = 3
	protoPGRelation   = 4

	protoDNID       = 1
	protoDNLat      = 8
	protoDNLon      = 9
	protoDNKeysVals = 10

	protoEntityID   = 1
	protoEntityKeys = 2
	protoEntityVals = 3

	// protoNID   = 1
	// protoNKeys = 2
	// protoNVals = 3
	protoNLat = 8
	protoNLon = 9

	// protoWID   = 1
	// protoWKeys = 2
	// protoWVals = 3
	protoWRefs = 8

	// protoRID       = 1
	// protoRKeys     = 2
	// protoRVals     = 3
	protoRRolesSid = 8
	protoRMemIDs   = 9
	protoRTypes    = 10

	protoMTNode     = 0
	protoMTWay      = 1
	protoMTRelation = 2
)

// fastBlobDecoder is a custom implementation of the protobuf format. It is
// optimized for decoding of PBF files. Rather than unmarshalling it streams
// the entities and thus reduces GC overhead. The fast blob decoder lacks
// support of some protobuf features which include groups and unpacked varint
// arrays. It is supposed to fail when it encounters a feature it doesn't
// support.
type fastBlobDecoder struct {
	q                                 []entityParser
	types                             OSMTypeSet
	granularity, latOffset, lonOffset int64
}

func newFastBlobDecoder() *fastBlobDecoder {
	return &fastBlobDecoder{
		granularity: 100,
	}
}

func (dec *fastBlobDecoder) decode(blob *OSMPBF.Blob, t OSMType) ([]entityParser, OSMTypeSet, error) {
	data, err := getBlobData(blob)
	if err != nil {
		return nil, 0, err
	}

	// Decode primitive block
	var pgBufs [][]byte
	var stBuf []byte
	p := proto.NewBuffer(data)
	for {
		m, err := nextProtoMessage(p)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, err
		}

		switch m.wire {
		case proto.WireBytes:
			switch m.tag {
			case protoPBStringTable:
				stBuf = m.buf
			case protoPBPrimitiveGroup:
				pgBufs = append(pgBufs, m.buf)
			}
		case proto.WireVarint:
			switch m.tag {
			case protoPBGranularity:
				dec.granularity = int64(m.i)
			case protoPBLatOffset:
				dec.latOffset = int64(m.i)
			case protoPBLonOffset:
				dec.lonOffset = int64(m.i)
			}
		}
	}

	// Decode primitive groups
	dec.q = nil
	var st stringTable
	for _, buf := range pgBufs {
		if err := dec.decodePrimitiveGroup(buf, &st, t); err != nil {
			return nil, 0, err
		}
	}

	// Decode string table
	if len(dec.q) > 0 {
		if st, err = dec.decodeStringTable(stBuf); err != nil {
			return nil, 0, err
		}
	}

	return dec.q, dec.types, nil
}

func (dec *fastBlobDecoder) decodePrimitiveGroup(buf []byte, st *stringTable, t OSMType) error {
	p := proto.NewBuffer(buf)

	// Read first tag to evaluate the entity type
	m, err := nextProtoMessage(p)
	if err != nil {
		return err
	}
	if m.wire == proto.WireBytes {
		var parser entityParser
		switch m.tag {
		case protoPGDenseNodes:
			if t == NodeType {
				// We use the nested buffer here since there is only 1 dense
				// nodes message.
				if d, err := newFastDenseNodesParser(
					st, m.buf,
					dec.granularity, dec.latOffset, dec.lonOffset,
				); err == nil {
					parser = d
				} else {
					return err
				}
			}
			dec.types.Set(NodeType, true)
		case protoPGNode:
			if t == NodeType {
				parser = newFastNodeParser(
					st, buf,
					dec.granularity, dec.latOffset, dec.lonOffset,
				)
			}
			dec.types.Set(NodeType, true)
		case protoPGWay:
			if t == WayType {
				parser = newFastWayParser(st, buf)
			}
			dec.types.Set(WayType, true)
		case protoPGRelation:
			if t == RelationType {
				parser = newFastRelationParser(st, buf)
			}
			dec.types.Set(RelationType, true)
		}
		if parser != nil {
			dec.q = append(dec.q, parser)
		}
	}
	return nil
}

func (dec *fastBlobDecoder) decodeStringTable(buf []byte) (stringTable, error) {
	p := proto.NewBuffer(buf)
	var st []string
	for {
		m, err := nextProtoMessage(p)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if m.wire == proto.WireBytes && m.tag == protoSTString {
			s := string(m.buf)
			st = append(st, s)
		}
	}
	return stringTable(st), nil
}

/* Dense Node Parser */
type fastDenseNodesParser struct {
	st                                *stringTable
	idBuf, latBuf, lonBuf, keyValsBuf *proto.Buffer
	id, lat, lon                      int64
	granularity, latOffset, lonOffset int64
}

func newFastDenseNodesParser(
	st *stringTable,
	buf []byte,
	granularity, latOffset, lonOffset int64,
) (*fastDenseNodesParser, error) {
	d := &fastDenseNodesParser{
		st:          st,
		granularity: granularity,
		latOffset:   latOffset,
		lonOffset:   lonOffset,
	}

	// Collect packed field buffers
	var idBuf, latBuf, lonBuf, keyValsBuf []byte
	p := proto.NewBuffer(buf)
	for {
		m, err := nextProtoMessage(p)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if m.wire == proto.WireBytes {
			var dest *[]byte
			switch m.tag {
			case protoDNID:
				dest = &idBuf
			case protoDNLat:
				dest = &latBuf
			case protoDNLon:
				dest = &lonBuf
			case protoDNKeysVals:
				dest = &keyValsBuf
			}
			if dest != nil {
				*dest = m.buf
			}
		}
	}

	// Create proto buffers
	d.idBuf = proto.NewBuffer(idBuf)
	d.latBuf = proto.NewBuffer(latBuf)
	d.lonBuf = proto.NewBuffer(lonBuf)
	d.keyValsBuf = proto.NewBuffer(keyValsBuf)

	return d, nil
}

func (d *fastDenseNodesParser) isEntityParser() {}

func (d *fastDenseNodesParser) next() (id int64, lat, lon float64, tags OSMTags, err error) {
	// ID
	did, err := d.idBuf.DecodeZigzag64()
	if err != nil {
		err = io.EOF
		return
	}
	d.id = int64(did) + d.id
	id = d.id

	// Lat
	dlat, err := d.latBuf.DecodeZigzag64()
	if err != nil {
		return
	}
	d.lat += int64(dlat)
	lat = decodeCoord(d.latOffset, d.granularity, d.lat)

	// Lon
	dlon, err := d.lonBuf.DecodeZigzag64()
	if err != nil {
		return
	}
	d.lon += int64(dlon)
	lon = decodeCoord(d.lonOffset, d.granularity, d.lon)

	// Tags
	tags, err = d.nextTags()
	return
}

func (d *fastDenseNodesParser) nextTags() (OSMTags, error) {
	var tags OSMTags
	for {
		k, err := d.keyValsBuf.DecodeVarint()
		if err != nil || k == 0 {
			break
		}

		v, err := d.keyValsBuf.DecodeVarint()
		if err != nil {
			return nil, err
		}

		key, val, err := d.st.extractTag(int(k), int(v))
		if err != nil {
			return nil, err
		}
		tags.Set(key, val)
	}
	return tags, nil
}

/* Node Parser */
type fastNodeParser struct {
	st                                *stringTable
	p                                 *proto.Buffer
	granularity, latOffset, lonOffset int64
}

func newFastNodeParser(
	st *stringTable,
	buf []byte,
	granularity, latOffset, lonOffset int64,
) *fastNodeParser {
	return &fastNodeParser{
		st:          st,
		p:           proto.NewBuffer(buf),
		granularity: granularity,
		latOffset:   latOffset,
		lonOffset:   lonOffset,
	}
}

func (d *fastNodeParser) isEntityParser() {}

func (d *fastNodeParser) next() (id int64, lat, lon float64, tags OSMTags, err error) {
	var hasLat, hasLon bool
	if id, tags, err = extractEntityBuffered(*d.st, d.p, protoPGNode, func(m protoMessage) {
		if m.wire == proto.WireVarint {
			switch m.tag {
			case protoNLat:
				lat = decodeCoord(d.latOffset, d.granularity, decodeZigzac(m.i))
				hasLat = true
			case protoNLon:
				lon = decodeCoord(d.lonOffset, d.granularity, decodeZigzac(m.i))
				hasLon = true
			}
		}
	}); err != nil {
		return
	}

	if !hasLat || !hasLon {
		err = errors.New("Node does not have coordinates")
		return
	}

	id = decodeZigzac(uint64(id))
	return
}

/* Way Parser */
type fastWayParser struct {
	st     *stringTable
	p      *proto.Buffer
	refBuf []byte
}

func newFastWayParser(st *stringTable, buf []byte) *fastWayParser {
	return &fastWayParser{
		st: st,
		p:  proto.NewBuffer(buf),
	}
}

func (d *fastWayParser) isEntityParser() {}

func (d *fastWayParser) next() (int64, OSMTags, error) {
	return extractEntityBuffered(*d.st, d.p, protoPGWay, func(m protoMessage) {
		if m.wire == proto.WireBytes && m.tag == protoWRefs {
			d.refBuf = m.buf
		}
	})
}

func (d *fastWayParser) refs() ([]int64, error) {
	return extractRefsBuffered(d.refBuf), nil
}

/* Relation Parser */
type fastRelationParser struct {
	st                               *stringTable
	p                                *proto.Buffer
	rolesSidBuf, memIDsBuf, typesBuf []byte
}

func newFastRelationParser(st *stringTable, buf []byte) *fastRelationParser {
	return &fastRelationParser{
		st: st,
		p:  proto.NewBuffer(buf),
	}
}

func (d *fastRelationParser) isEntityParser() {}

func (d *fastRelationParser) next() (int64, OSMTags, error) {
	return extractEntityBuffered(*d.st, d.p, protoPGRelation, func(m protoMessage) {
		if m.wire == proto.WireBytes {
			switch m.tag {
			case protoRRolesSid:
				d.rolesSidBuf = m.buf
			case protoRMemIDs:
				d.memIDsBuf = m.buf
			case protoRTypes:
				d.typesBuf = m.buf
			}
		}
	})
}

func (d *fastRelationParser) ids() ([]int64, error) {
	return extractRefsBuffered(d.memIDsBuf), nil
}

func (d *fastRelationParser) roles() ([]string, error) {
	p := proto.NewBuffer(d.rolesSidBuf)
	roles := make([]string, 0, countVarints(d.rolesSidBuf))
	for {
		if sid, err := p.DecodeVarint(); err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return nil, err
		} else {
			if s, err := d.st.get(int(sid)); err == nil {
				roles = append(roles, s)
			} else {
				return nil, err
			}
		}
	}
	return roles, nil
}

func (d *fastRelationParser) types() ([]OSMType, error) {
	p := proto.NewBuffer(d.typesBuf)
	types := make([]OSMType, 0, countVarints(d.typesBuf))
	for {
		if memberType, err := p.DecodeVarint(); err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return nil, err
		} else {
			var t OSMType
			switch memberType {
			case protoMTNode:
				t = NodeType
			case protoMTWay:
				t = WayType
			case protoMTRelation:
				t = RelationType
			default:
				return nil, errors.New("Unknown member type")
			}
			types = append(types, t)
		}
	}
	return types, nil
}

/* Helpers */
func decodeZigzac(i uint64) int64 {
	return int64((i >> 1) ^ uint64((int64(i&1)<<63)>>63))
}

func countVarints(buf []byte) int {
	n := 0
	for _, b := range buf {
		if b&0x80 == 0 {
			n++
		}
	}
	return n
}

func extractRefsBuffered(buf []byte) []int64 {
	ids := make([]int64, 0, countVarints(buf))
	var id int64
	p := proto.NewBuffer(buf)
	for {
		delta, err := p.DecodeZigzag64()
		if err != nil {
			break
		}
		id += int64(delta)
		ids = append(ids, id)
	}
	return ids
}

func extractEntityBuffered(
	st stringTable,
	p *proto.Buffer,
	tag uint64,
	msgs func(protoMessage),
) (id int64, tags OSMTags, err error) {
	m, err := nextProtoMessage(p)
	if err != nil {
		return
	}
	if m.wire != proto.WireBytes || m.tag != tag {
		err = errors.New("Primitive group contains different types")
		return
	}

	var keyBuf, valBuf []byte
	var hasID bool
	p = proto.NewBuffer(m.buf)
	for {
		m, err := nextProtoMessage(p)
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, nil, err
		}

		switch m.wire {
		case proto.WireVarint:
			switch m.tag {
			case protoEntityID:
				hasID = true
				id = int64(m.i)
			default:
				msgs(m)
			}
		case proto.WireBytes:
			switch m.tag {
			case protoEntityKeys:
				keyBuf = m.buf
			case protoEntityVals:
				valBuf = m.buf
			default:
				msgs(m)
			}
		}
	}

	if !hasID {
		err = errors.New("Entity does not have an ID")
		return
	}

	tags, err = extractTagsBuffered(st, keyBuf, valBuf)
	return
}

func extractTagsBuffered(st stringTable, keys, vals []byte) (OSMTags, error) {
	kp := proto.NewBuffer(keys)
	vp := proto.NewBuffer(vals)
	tags := NewOSMTags(countVarints(keys))
	for {
		k, err := kp.DecodeVarint()
		if err == io.ErrUnexpectedEOF {
			break
		} else if err != nil {
			return nil, err
		}

		v, err := vp.DecodeVarint()
		if err != nil {
			return nil, err
		}

		if key, val, err := st.extractTag(int(k), int(v)); err == nil {
			tags.Set(key, val)
		} else {
			return nil, err
		}
	}
	return tags, nil
}

// Depending on the wire type either buf or i contain data
type protoMessage struct {
	tag  uint64
	wire uint8
	buf  []byte
	i    uint64
}

func nextProtoMessage(p *proto.Buffer) (m protoMessage, err error) {
	for {
		var x uint64
		if x, err = p.DecodeVarint(); err != nil {
			// We have no other way of checking EOF using proto.Buffer
			if err == io.ErrUnexpectedEOF {
				err = io.EOF
			}
			return
		}

		m.tag = x >> 3
		m.wire = uint8(x & 7)

		switch m.wire {
		// Unneeded fields
		case proto.WireFixed32:
			if _, err = p.DecodeFixed32(); err != nil {
				return
			}
		case proto.WireFixed64:
			if _, err = p.DecodeFixed64(); err != nil {
				return
			}

		// Groups are deprecated and usually not used in PBF files
		case proto.WireStartGroup:
			fallthrough
		case proto.WireEndGroup:
			err = errors.New("Protobuf groups are not supported")
			return

		// Valid fields
		case proto.WireVarint:
			m.i, err = p.DecodeVarint()
			return
		case proto.WireBytes:
			m.buf, err = p.DecodeRawBytes(false)
			return
		}
	}
}
