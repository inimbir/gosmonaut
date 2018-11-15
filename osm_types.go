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
	GetID() int64
	GetType() OSMType
	GetTags() OSMTags
	fmt.Stringer
}

/* Node */

// Node represents an OSM node element
type Node struct {
	ID       int64
	Lat, Lon float64
	Tags     OSMTags
}

// GetID returns the ID
func (n Node) GetID() int64 {
	return n.ID
}

// GetType always returns NodeType
func (n Node) GetType() OSMType {
	return NodeType
}

// GetTags returns the tags
func (n Node) GetTags() OSMTags {
	return n.Tags
}

func (n Node) String() string {
	return prettyPrintEntity(n)
}

// MarshalJSON prints the JSON representation of the node
func (n Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string     `json:"type"`
		ID   int64      `json:"id"`
		Lat  coordFloat `json:"lat"`
		Lon  coordFloat `json:"lon"`
		Tags OSMTags    `json:"tags,omitempty"`
	}{"node", n.ID, coordFloat(n.Lat), coordFloat(n.Lon), n.Tags})
}

/* Way */

// Way represents an OSM way element
type Way struct {
	ID    int64
	Tags  OSMTags
	Nodes []Node
}

// GetID returns the ID
func (w Way) GetID() int64 {
	return w.ID
}

// GetType always returns WayType
func (w Way) GetType() OSMType {
	return WayType
}

// GetTags returns the tags
func (w Way) GetTags() OSMTags {
	return w.Tags
}

func (w Way) String() string {
	return prettyPrintEntity(w)
}

// MarshalJSON prints the JSON representation of the way
func (w Way) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string  `json:"type"`
		ID    int64   `json:"id"`
		Tags  OSMTags `json:"tags"`
		Nodes []Node  `json:"nodes"`
	}{"way", w.ID, w.Tags, w.Nodes})
}

/* Relation */

// Relation represents an OSM relation element
type Relation struct {
	ID      int64
	Tags    OSMTags
	Members []Member
}

// GetID returns the ID
func (r Relation) GetID() int64 {
	return r.ID
}

// GetType always returns RelationType
func (r Relation) GetType() OSMType {
	return RelationType
}

// GetTags returns the tags
func (r Relation) GetTags() OSMTags {
	return r.Tags
}

func (r Relation) String() string {
	return prettyPrintEntity(r)
}

// MarshalJSON prints the JSON representation of the relation
func (r Relation) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string   `json:"type"`
		ID      int64    `json:"id"`
		Tags    OSMTags  `json:"tags"`
		Members []Member `json:"members"`
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

/* OSM Tags */

// OSMTags represents a key-value mapping for OSM tags.
type OSMTags []string // Alternating array of keys/values

// NewOSMTags creates new OSMTags and can be used if the number of tags is
// known.
func NewOSMTags(n int) OSMTags {
	if n < 0 {
		n = 0
	}
	return make([]string, 0, n*2)
}

// NewOSMTagsFromMap creates new OSMTags that contains the tags from the given
// map.
func NewOSMTagsFromMap(m map[string]string) OSMTags {
	t := NewOSMTags(len(m))
	for k, v := range m {
		t = append(t, k, v)
	}
	return t
}

// Set adds or updates the value for the given key.
func (t *OSMTags) Set(key, val string) {
	if i, ok := t.index(key); ok {
		t.set(i+1, val)
	} else {
		*t = append(*t, key, val)
	}
}

// Get returns the value for the given key or false if the key does not exist.
func (t *OSMTags) Get(key string) (string, bool) {
	if i, ok := t.index(key); ok {
		return t.get(i + 1), true
	}
	return "", false
}

// Has returns true if the given key exists.
func (t *OSMTags) Has(key string) bool {
	_, ok := t.index(key)
	return ok
}

// HasValue return true if the given key exists and its value is val.
func (t *OSMTags) HasValue(key, val string) bool {
	if i, ok := t.index(key); ok {
		return t.get(i+1) == val
	}
	return false
}

// Map returns the map representation of the tags.
func (t *OSMTags) Map() map[string]string {
	m := make(map[string]string, t.Len())
	for i := 0; i < len(*t); i += 2 {
		m[t.get(i)] = t.get(i + 1)
	}
	return m
}

func (t OSMTags) String() string {
	return prettyPrintEntity(t)
}

// MarshalJSON prints the JSON representation of the tags.
func (t OSMTags) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Map())
}

// Len returns the number of tags.
func (t *OSMTags) Len() int {
	return len(*t) / 2
}

func (t *OSMTags) get(i int) string {
	return []string(*t)[i]
}

func (t *OSMTags) set(i int, s string) {
	[]string(*t)[i] = s
}

func (t *OSMTags) index(key string) (int, bool) {
	for i := 0; i < len(*t); i += 2 {
		if t.get(i) == key {
			return i, true
		}
	}
	return -1, false
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
