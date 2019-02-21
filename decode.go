package gosmonaut

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/MorbZ/gosmonaut/OSMPBF"
	"github.com/golang/protobuf/proto"
	"io"
	"sync"
	"time"
)

// DecoderType represents the decoder that is used for parsing PBF blob data.
type DecoderType int

// Order is important: First decoder is the default
const (
	// FastDecoder is a custom implementation of the protobuf format. It is
	// optimized for decoding of PBF files. Rather than unmarshalling it streams
	// the entities and thus reduces GC overhead. The fast blob decoder lacks
	// support of some protobuf features which include groups and unpacked
	// varint arrays. It is supposed to fail when it encounters a feature it
	// doesn't support.
	FastDecoder DecoderType = iota

	// GoDecoder uses the official Golang Protobuf package. All protobuf
	// messages will be unmarshalled to temporary objects before processing.
	GoDecoder
)

const (
	maxBlobHeaderSize = 64 * 1024

	initialBlobBufSize = 1 * 1024 * 1024

	// maxBlobSize is maximum supported blob size
	maxBlobSize = 32 * 1024 * 1024

	// Typical PrimitiveBlock contains 8k OSM entities
	entitiesPerPrimitiveBlock = 8000
)

var (
	parseCapabilities = map[string]bool{
		"OsmSchema-V0.6": true,
		"DenseNodes":     true,
	}
)

// Header contains the meta information of the PBF file.
type Header struct {
	BoundingBox                      *BoundingBox
	RequiredFeatures                 []string
	OptionalFeatures                 []string
	WritingProgram                   string
	Source                           string
	OsmosisReplicationTimestamp      time.Time
	OsmosisReplicationSequenceNumber int64
	OsmosisReplicationBaseURL        string
}

// BoundingBox defines a rectangular area of coordinates.
type BoundingBox struct {
	Left   float64
	Right  float64
	Top    float64
	Bottom float64
}

/* Used for decoding channels */
type decodeInput struct {
	blob *OSMPBF.Blob
	pos  filePosition
	err  error
}

type decodeOutput struct {
	parsers []entityParser
	types   OSMTypeSet
	pos     filePosition
	err     error
}

/* Blob Decoder Interfaces */
type blobDecoder interface {
	decode(*OSMPBF.Blob, OSMType) ([]entityParser, OSMTypeSet, error)
}

type entityParser interface {
	isEntityParser()
}

type nodeParser interface {
	entityParser
	next() (id int64, lat, lon float64, tags OSMTags, err error)
}

type wayParser interface {
	entityParser
	next() (id int64, tags OSMTags, err error)
	refs() ([]int64, error)
}

type relationParser interface {
	entityParser
	next() (id int64, tags OSMTags, err error)
	ids() ([]int64, error)
	roles() ([]string, error)
	types() ([]OSMType, error)
}

// A Decoder reads and decodes OpenStreetMap PBF data from an input stream
type decoder struct {
	nProcs      int
	nRun        int
	wg          sync.WaitGroup
	decoderType DecoderType

	// Blob providers
	nodeIndexer, wayIndexer, relationIndexer *blobIndexer
	finder                                   *blobFinder

	// For data decoders
	inputs      []chan<- decodeInput
	outputs     []<-chan decodeOutput
	outputIndex int
}

// newDecoder returns a new decoder that reads from f.
func newDecoder(f io.ReadSeeker, nProcs int, decoderType DecoderType) (dec *decoder, header Header, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, initialBlobBufSize))

	dec = &decoder{
		nProcs:          nProcs,
		finder:          &blobFinder{f, buf},
		nodeIndexer:     newBlobIndexer(f, buf),
		wayIndexer:      newBlobIndexer(f, buf),
		relationIndexer: newBlobIndexer(f, buf),
		decoderType:     decoderType,
	}

	blob, err := dec.finder.readHeaderBlob()
	if err != nil {
		return
	}

	header, err = decodeOSMHeader(blob)
	return
}

// Start decoding process using n goroutines.
func (dec *decoder) Start(t OSMType) error {
	// Wait for the previous run to finish
	dec.wg.Wait()

	dec.nRun++
	dec.outputIndex = 0

	// Start data decoders
	dec.inputs = make([]chan<- decodeInput, 0, dec.nProcs)
	dec.outputs = make([]<-chan decodeOutput, 0, dec.nProcs)
	dec.wg.Add(dec.nProcs)
	for i := 0; i < dec.nProcs; i++ {
		input := make(chan decodeInput)
		output := make(chan decodeOutput)
		go func() {
			defer dec.wg.Done()

			var bd blobDecoder
			switch dec.decoderType {
			case GoDecoder:
				bd = new(goBlobDecoder)
			case FastDecoder:
				bd = newFastBlobDecoder()
			}

			for i := range input {
				if i.err == nil {
					// Decode objects and send to output
					parsers, types, err := bd.decode(i.blob, t)
					output <- decodeOutput{
						parsers: parsers,
						types:   types,
						pos:     i.pos,
						err:     err,
					}
				} else {
					// Send input error as is
					output <- decodeOutput{
						err: i.err,
					}
				}
			}
			close(output)
		}()
		dec.inputs = append(dec.inputs, input)
		dec.outputs = append(dec.outputs, output)
	}

	// Select the blob provider
	var provider blobProvider
	if dec.nRun == 1 {
		provider = dec.finder
	} else {
		var indexer *blobIndexer
		switch t {
		case NodeType:
			indexer = dec.nodeIndexer
		case WayType:
			indexer = dec.wayIndexer
		case RelationType:
			indexer = dec.relationIndexer
		}
		indexer.reset()
		provider = indexer
	}

	// Start reading OSMData blobs
	go func() {
		var inputIndex int
		for {
			// Select input channel
			input := dec.inputs[inputIndex]
			inputIndex = (inputIndex + 1) % dec.nProcs

			// Read next blob
			blob, pos, err := provider.readDataBlob()

			// Send blob for decoding
			input <- decodeInput{
				blob: blob,
				pos:  pos,
				err:  err,
			}

			// On error close input channels and quit
			if err != nil {
				for _, input := range dec.inputs {
					close(input)
				}
				return
			}
		}
	}()
	return nil
}

func (dec *decoder) nextPair() ([]entityParser, error) {
	// Select output channel
	output := dec.outputs[dec.outputIndex]
	dec.outputIndex = (dec.outputIndex + 1) % dec.nProcs

	// Get output
	o, ok := <-output
	if !ok {
		return nil, io.EOF
	}

	// Index file position of entity types
	if dec.nRun == 1 {
		if o.types.Get(NodeType) {
			dec.nodeIndexer.index(o.pos)
		}
		if o.types.Get(WayType) {
			dec.wayIndexer.index(o.pos)
		}
		if o.types.Get(RelationType) {
			dec.relationIndexer.index(o.pos)
		}
	}
	return o.parsers, o.err
}

func decodeOSMHeader(blob *OSMPBF.Blob) (header Header, err error) {
	data, err := getBlobData(blob)
	if err != nil {
		return
	}

	headerBlock := new(OSMPBF.HeaderBlock)
	if err = proto.Unmarshal(data, headerBlock); err != nil {
		return
	}

	// Check we have the parse capabilities
	requiredFeatures := headerBlock.GetRequiredFeatures()
	for _, feature := range requiredFeatures {
		if !parseCapabilities[feature] {
			err = fmt.Errorf("parser does not have %s capability", feature)
			return
		}
	}

	// Read properties to header struct
	header = Header{
		RequiredFeatures:                 headerBlock.GetRequiredFeatures(),
		OptionalFeatures:                 headerBlock.GetOptionalFeatures(),
		WritingProgram:                   headerBlock.GetWritingprogram(),
		Source:                           headerBlock.GetSource(),
		OsmosisReplicationBaseURL:        headerBlock.GetOsmosisReplicationBaseUrl(),
		OsmosisReplicationSequenceNumber: headerBlock.GetOsmosisReplicationSequenceNumber(),
	}

	// convert timestamp epoch seconds to golang time structure if it exists
	if headerBlock.OsmosisReplicationTimestamp != nil {
		header.OsmosisReplicationTimestamp = time.Unix(*headerBlock.OsmosisReplicationTimestamp, 0)
	}
	// read bounding box if it exists
	if headerBlock.Bbox != nil {
		// Units are always in nanodegree and do not obey granularity rules.
		// See osmformat.proto.
		header.BoundingBox = &BoundingBox{
			Left:   1e-9 * float64(*headerBlock.Bbox.Left),
			Right:  1e-9 * float64(*headerBlock.Bbox.Right),
			Bottom: 1e-9 * float64(*headerBlock.Bbox.Bottom),
			Top:    1e-9 * float64(*headerBlock.Bbox.Top),
		}
	}
	return
}

/* Blob Provider */
type filePosition struct {
	offset, size int64
}

type blobProvider interface {
	readDataBlob() (*OSMPBF.Blob, filePosition, error)
}

func unmarshalBlob(buf *bytes.Buffer) (*OSMPBF.Blob, error) {
	blob := new(OSMPBF.Blob)
	err := proto.Unmarshal(buf.Bytes(), blob)
	return blob, err
}

/* Blob Indexer */
type blobIndexer struct {
	f     io.ReadSeeker
	buf   *bytes.Buffer
	blobs []filePosition
	i     int
}

func newBlobIndexer(f io.ReadSeeker, buf *bytes.Buffer) *blobIndexer {
	return &blobIndexer{
		f:   f,
		buf: buf,
	}
}

func (b *blobIndexer) index(pos filePosition) {
	b.blobs = append(b.blobs, pos)
}

func (b *blobIndexer) reset() {
	b.i = 0
}

func (b *blobIndexer) readDataBlob() (blob *OSMPBF.Blob, pos filePosition, err error) {
	if b.i >= len(b.blobs) {
		err = io.EOF
		return
	}

	// Read next file position
	pos = b.blobs[b.i]
	b.i++

	// Read blob
	b.f.Seek(pos.offset, io.SeekStart)
	b.buf.Reset()
	if _, err = io.CopyN(b.buf, b.f, pos.size); err != nil {
		return
	}

	// Unmarshal blob
	blob, err = unmarshalBlob(b.buf)
	return
}

/* Blob Finder */
type blobFinder struct {
	f   io.ReadSeeker
	buf *bytes.Buffer
}

func (d *blobFinder) readHeaderBlob() (*OSMPBF.Blob, error) {
	d.f.Seek(0, io.SeekStart)
	blob, _, err := d.readFileBlock("OSMHeader")
	return blob, err
}

func (d *blobFinder) readDataBlob() (*OSMPBF.Blob, filePosition, error) {
	return d.readFileBlock("OSMData")
}

func (d *blobFinder) readFileBlock(t string) (blob *OSMPBF.Blob, pos filePosition, err error) {
	blobHeaderSize, err := d.readBlobHeaderSize()
	if err != nil {
		return
	}

	blobHeader, err := d.readBlobHeader(blobHeaderSize)
	if err != nil {
		return
	}

	if blobHeader.GetType() != t {
		err = fmt.Errorf("unexpected fileblock of type %s", blobHeader.GetType())
		return
	}

	offset, err := d.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}

	pos = filePosition{
		offset,
		int64(blobHeader.GetDatasize()),
	}

	blob, err = d.readBlob(blobHeader)
	return
}

func (d *blobFinder) readBlobHeaderSize() (uint32, error) {
	d.buf.Reset()
	if _, err := io.CopyN(d.buf, d.f, 4); err != nil {
		return 0, err
	}

	size := binary.BigEndian.Uint32(d.buf.Bytes())
	if size >= maxBlobHeaderSize {
		return 0, errors.New("BlobHeader size >= 64Kb")
	}
	return size, nil
}

func (d *blobFinder) readBlobHeader(size uint32) (*OSMPBF.BlobHeader, error) {
	d.buf.Reset()
	if _, err := io.CopyN(d.buf, d.f, int64(size)); err != nil {
		return nil, err
	}

	blobHeader := new(OSMPBF.BlobHeader)
	if err := proto.Unmarshal(d.buf.Bytes(), blobHeader); err != nil {
		return nil, err
	}

	if blobHeader.GetDatasize() >= maxBlobSize {
		return nil, errors.New("Blob size >= 32Mb")
	}
	return blobHeader, nil
}

func (d *blobFinder) readBlob(blobHeader *OSMPBF.BlobHeader) (*OSMPBF.Blob, error) {
	d.buf.Reset()
	size := int64(blobHeader.GetDatasize())
	if _, err := io.CopyN(d.buf, d.f, size); err != nil {
		return nil, err
	}
	return unmarshalBlob(d.buf)
}

/* String Table */
type stringTable []string

func (st stringTable) get(i int) (string, error) {
	if i >= len(st) {
		return "", errors.New("String table index out of bounds")
	}
	return st[i], nil
}

func (st stringTable) extractTag(keyID, valueID int) (key, val string, err error) {
	if key, err = st.get(keyID); err != nil {
		return
	}
	if val, err = st.get(valueID); err != nil {
		return
	}
	return
}

/* Helpers */
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

func decodeCoord(offset, granularity, coord int64) float64 {
	return 1e-9 * float64((offset + (granularity * coord)))
}
