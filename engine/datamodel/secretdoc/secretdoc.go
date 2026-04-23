// Package secretdoc wraps a Document to present Kubernetes Secret .data.*
// values as plaintext to expression evaluation while keeping the raw base64
// as the wire, hash and log representation.
//
// Rationale: DBSP pipelines read Secret values via many JSONPath forms
// (dotted data.x, bracket data['x']/data["x"], rooted $.data.x, fully
// bracketed $["data"]["x"]). A previous adaptor-based fix matched paths via
// string prefix and silently returned base64 for every form other than
// dotted. Decoding at ingress is also unsafe — it leaks plaintext into
// debug log dumps (zset String/MarshalJSON) and risks double-encoding on
// write-back to the API server.
//
// This wrapper normalises path tokens through the JSONPath parser and
// applies the transform at a single choke point, so every form yields
// plaintext on GetField and every write re-encodes to base64 on SetField.
// Hash/String/MarshalJSON delegate to the base so the raw base64 remains
// the identity and on-wire form.
package secretdoc

import (
	"encoding/base64"

	"github.com/ohler55/ojg/jp"

	"github.com/l7mp/dbsp/engine/datamodel"
)

// Document wraps a datamodel.Document with Secret .data.* codec semantics.
type Document struct {
	base datamodel.Document
}

var _ datamodel.Document = (*Document)(nil)

// New returns a wrapper around base.
func New(base datamodel.Document) *Document { return &Document{base: base} }

func (d *Document) Hash() string                { return d.base.Hash() }
func (d *Document) PrimaryKey() (string, error) { return d.base.PrimaryKey() }
func (d *Document) String() string              { return d.base.String() }
func (d *Document) Fields() map[string]any      { return d.base.Fields() }
func (d *Document) MarshalJSON() ([]byte, error) {
	return d.base.MarshalJSON()
}

func (d *Document) UnmarshalJSON(data []byte) error {
	return d.base.UnmarshalJSON(data)
}

func (d *Document) Copy() datamodel.Document {
	return &Document{base: d.base.Copy()}
}

func (d *Document) New() datamodel.Document {
	return &Document{base: d.base.New()}
}

func (d *Document) Merge(other datamodel.Document) datamodel.Document {
	if o, ok := other.(*Document); ok {
		return &Document{base: d.base.Merge(o.base)}
	}
	return &Document{base: d.base.Merge(other)}
}

// GetField delegates to the base, then decodes base64 when the key names a
// leaf under top-level `data`. Non-string values and undecodable strings
// are returned as-is.
func (d *Document) GetField(key string) (any, error) {
	v, err := d.base.GetField(key)
	if err != nil {
		return nil, err
	}
	if !targetsSecretDataLeaf(key) {
		return v, nil
	}
	s, ok := v.(string)
	if !ok {
		return v, nil
	}
	raw, decErr := base64.StdEncoding.DecodeString(s)
	if decErr != nil {
		return v, nil
	}
	return string(raw), nil
}

// SetField re-encodes string values to base64 when the key names a leaf
// under top-level `data`, so a plaintext write survives round-tripping
// back to the Kubernetes API.
func (d *Document) SetField(key string, value any) error {
	if targetsSecretDataLeaf(key) {
		if s, ok := value.(string); ok {
			value = base64.StdEncoding.EncodeToString([]byte(s))
		}
	}
	return d.base.SetField(key, value)
}

// Unwrap returns the wrapped base document. Primarily for tests.
func (d *Document) Unwrap() datamodel.Document { return d.base }

// targetsSecretDataLeaf reports whether a key addresses a named child of
// the top-level `data` field. Handles dotted, bracket ('x'/"x"), and
// rooted ($, $[]) JSONPath forms. Returns false for wildcards, filters,
// array indices and for the `data` map itself (its whole value is handed
// back unchanged so callers can iterate).
func targetsSecretDataLeaf(key string) bool {
	if key == "" {
		return false
	}
	exp, err := jp.ParseString(jsonPathCandidate(key))
	if err != nil {
		return false
	}
	tokens := make([]string, 0, len(exp))
	for _, frag := range exp {
		switch f := frag.(type) {
		case jp.Root:
			continue
		case jp.Child:
			tokens = append(tokens, string(f))
		default:
			// Bracket / Nth / Filter / Descent / Wildcard etc. — not a
			// plain static member path, so we cannot decide safely.
			return false
		}
	}
	return len(tokens) >= 2 && tokens[0] == "data"
}

// jsonPathCandidate prepends a root so bare dotted or bracket forms
// (data.x, data['x']) parse via jp.ParseString.
func jsonPathCandidate(key string) string {
	if len(key) > 0 && key[0] == '$' {
		return key
	}
	if len(key) > 0 && key[0] == '[' {
		return "$" + key
	}
	return "$." + key
}
