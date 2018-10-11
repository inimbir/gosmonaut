package gosmonaut

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// OSMType represents the type of an OSM entity
type OSMType uint8

// OSM Types: node, way, relation
const (
	NodeType OSMType = 1 << iota
	WayType
	RelationType
)

// OSMEntity is the common interface of all OSM entities
type OSMEntity interface {
	GetType() OSMType
	GetTags() map[string]string
	fmt.Stringer
}

/* Node */

// Node represents an OSM node element
type Node struct {
	ID   int64
	Lat  float64
	Lon  float64
	Tags map[string]string
}

// GetType always returns NodeType
func (n Node) GetType() OSMType {
	return NodeType
}

// GetTags returns the tags
func (n Node) GetTags() map[string]string {
	return n.Tags
}

func (n Node) String() string {
	return prettyPrintEntity(n)
}

// MarshalJSON prints the JSON representation of the node
func (n Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string            `json:"type"`
		ID   int64             `json:"id"`
		Lat  coordFloat        `json:"lat"`
		Lon  coordFloat        `json:"lon"`
		Tags map[string]string `json:"tags,omitempty"`
	}{"node", n.ID, coordFloat(n.Lat), coordFloat(n.Lon), n.Tags})
}

/* Way */

// Way represents an OSM way element
type Way struct {
	ID    int64
	Tags  map[string]string
	Nodes []Node
}

// GetType always returns WayType
func (w Way) GetType() OSMType {
	return WayType
}

// GetTags returns the tags
func (w Way) GetTags() map[string]string {
	return w.Tags
}

func (w Way) String() string {
	return prettyPrintEntity(w)
}

// MarshalJSON prints the JSON representation of the way
func (w Way) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string            `json:"type"`
		ID    int64             `json:"id"`
		Tags  map[string]string `json:"tags"`
		Nodes []Node            `json:"nodes"`
	}{"way", w.ID, w.Tags, w.Nodes})
}

/* Relation */

// Relation represents an OSM relation element
type Relation struct {
	ID      int64
	Tags    map[string]string
	Members []Member
}

// GetType always returns RelationType
func (r Relation) GetType() OSMType {
	return RelationType
}

// GetTags returns the tags
func (r Relation) GetTags() map[string]string {
	return r.Tags
}

func (r Relation) String() string {
	return prettyPrintEntity(r)
}

// MarshalJSON prints the JSON representation of the relation
func (r Relation) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string            `json:"type"`
		ID      int64             `json:"id"`
		Tags    map[string]string `json:"tags"`
		Members []Member          `json:"members"`
	}{"relation", r.ID, r.Tags, r.Members})
}

// Member represents a member of a relation
type Member struct {
	Role   string    `json:"role"`
	Entity OSMEntity `json:"entity"`
}

/* OSMTypeSet */

// OSMTypeSet is used to enable/disable OSM types
type OSMTypeSet uint8

// NewOSMTypeSet returns a new OSMTypeSet with the given types enabled/disabled
func NewOSMTypeSet(nodes, ways, relations bool) OSMTypeSet {
	var s OSMTypeSet
	s.Set(NodeType, nodes)
	s.Set(WayType, ways)
	s.Set(RelationType, relations)
	return s
}

// Set enables/disables the given type
func (s *OSMTypeSet) Set(t OSMType, enabled bool) {
	if enabled {
		*s = OSMTypeSet(uint8(*s) | uint8(t))
	} else {
		*s = OSMTypeSet(uint8(*s) & (^uint8(t)))
	}
}

// Get returns true if the given type is enabled
func (s *OSMTypeSet) Get(t OSMType) bool {
	return uint8(*s)&uint8(t) != 0
}

/* Helpers */

// Print an OSM entity as JSON with indention
func prettyPrintEntity(m json.Marshaler) string {
	b := new(bytes.Buffer)
	enc := json.NewEncoder(b)
	enc.SetIndent("", "  ")
	enc.Encode(m)
	return b.String()
}

// Restrict the JSON decimals to 7 which is the OSM default
type coordFloat float64

func (mf coordFloat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%.7f", mf)), nil
}
