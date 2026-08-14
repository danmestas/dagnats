// openapi/types.go
//
// Minimal OpenAPI 3.1 struct subset that this package emits. We do not
// adopt kin-openapi or a similar library because (a) the surface we
// emit is tiny and stable, (b) we want zero new transitive deps in
// the engine binary, and (c) the structural validity check is a few
// json.Marshal round-trips, not a full validator. Fields use
// `json:",omitempty"` aggressively so the rendered spec only contains
// the keys we actually populate — the noise reduction matters for the
// Scalar UI, which renders every empty section it can find.
package openapi

import (
	"encoding/json"
)

// Spec is the top-level OpenAPI 3.1 document.
type Spec struct {
	OpenAPI    string                 `json:"openapi"`
	Info       Info                   `json:"info"`
	Servers    []Server               `json:"servers,omitempty"`
	Paths      map[string]PathItem    `json:"paths"`
	Components *Components            `json:"components,omitempty"`
	Extensions map[string]interface{} `json:"-"`
}

// MarshalJSON merges Extensions (x-* keys) into the top-level object
// so consumers receive a flat OpenAPI document with our extensions
// alongside the standard fields. Without this, x-* extensions on Spec
// would be lost on encode.
func (s Spec) MarshalJSON() ([]byte, error) {
	type alias Spec
	base, err := json.Marshal((alias)(s))
	if err != nil {
		return nil, err
	}
	return mergeExtensions(base, s.Extensions)
}

// Info is the OpenAPI 3.1 info block.
type Info struct {
	Title       string `json:"title"`
	Version     string `json:"version"`
	Description string `json:"description,omitempty"`
}

// Server is one entry under "servers".
type Server struct {
	URL         string `json:"url"`
	Description string `json:"description,omitempty"`
}

// PathItem groups operations by HTTP method on a single path.
type PathItem struct {
	Get        *Operation             `json:"get,omitempty"`
	Post       *Operation             `json:"post,omitempty"`
	Put        *Operation             `json:"put,omitempty"`
	Patch      *Operation             `json:"patch,omitempty"`
	Delete     *Operation             `json:"delete,omitempty"`
	Parameters []Parameter            `json:"parameters,omitempty"`
	Extensions map[string]interface{} `json:"-"`
}

// MarshalJSON merges Extensions into the path item. Mirrors Spec.
func (p PathItem) MarshalJSON() ([]byte, error) {
	type alias PathItem
	base, err := json.Marshal((alias)(p))
	if err != nil {
		return nil, err
	}
	return mergeExtensions(base, p.Extensions)
}

// Operation describes one HTTP verb on a path.
type Operation struct {
	OperationID string                 `json:"operationId,omitempty"`
	Summary     string                 `json:"summary,omitempty"`
	Description string                 `json:"description,omitempty"`
	Tags        []string               `json:"tags,omitempty"`
	Parameters  []Parameter            `json:"parameters,omitempty"`
	RequestBody *RequestBody           `json:"requestBody,omitempty"`
	Responses   map[string]Response    `json:"responses"`
	Security    []map[string][]string  `json:"security,omitempty"`
	Extensions  map[string]interface{} `json:"-"`
}

// MarshalJSON merges Extensions into the operation. Mirrors Spec.
func (o Operation) MarshalJSON() ([]byte, error) {
	type alias Operation
	base, err := json.Marshal((alias)(o))
	if err != nil {
		return nil, err
	}
	return mergeExtensions(base, o.Extensions)
}

// Parameter describes a header / query / path / cookie parameter.
type Parameter struct {
	Name        string          `json:"name"`
	In          string          `json:"in"`
	Required    bool            `json:"required,omitempty"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// RequestBody describes the payload accepted by an operation.
type RequestBody struct {
	Required    bool                 `json:"required,omitempty"`
	Description string               `json:"description,omitempty"`
	Content     map[string]MediaType `json:"content"`
}

// MediaType pairs a content-type with a schema.
type MediaType struct {
	Schema json.RawMessage `json:"schema,omitempty"`
}

// Response describes one HTTP response for an operation.
type Response struct {
	Description string               `json:"description"`
	Headers     map[string]Header    `json:"headers,omitempty"`
	Content     map[string]MediaType `json:"content,omitempty"`
}

// Header describes a single response header.
type Header struct {
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// Components is the OpenAPI 3.1 components block.
type Components struct {
	Schemas         map[string]json.RawMessage `json:"schemas,omitempty"`
	SecuritySchemes map[string]SecurityScheme  `json:"securitySchemes,omitempty"`
}

// SecurityScheme describes one auth flow. Field set is the union of
// the OpenAPI 3.1 securityScheme variants — caller fills only the
// fields valid for their Type.
type SecurityScheme struct {
	Type         string `json:"type"`
	Description  string `json:"description,omitempty"`
	Name         string `json:"name,omitempty"`
	In           string `json:"in,omitempty"`
	Scheme       string `json:"scheme,omitempty"`
	BearerFormat string `json:"bearerFormat,omitempty"`
}

// mergeExtensions overlays the x-* keys from ext onto the JSON-encoded
// object base. Returns base unchanged when ext is empty so the caller
// pays nothing for the common no-extension case.
func mergeExtensions(
	base []byte, ext map[string]interface{},
) ([]byte, error) {
	if len(ext) == 0 {
		return base, nil
	}
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(base, &asMap); err != nil {
		return nil, err
	}
	for k, v := range ext {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		asMap[k] = raw
	}
	return json.Marshal(asMap)
}
