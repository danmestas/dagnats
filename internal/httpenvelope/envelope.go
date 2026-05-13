package httpenvelope

import (
	"net/http"
)

// Envelope is the request shape passed through TriggerEnvelope.Data
// for webhook + HTTP triggers. Method, Path, Query, Headers map 1:1
// from net/http; Body is the bounded request body bytes.
//
// Why a struct (vs http.Request): TriggerEnvelope.Data must be JSON
// serializable and survive a NATS hop. net/http.Request is neither.
// Why headers as map[string]string (not http.Header / []string):
// the trigger contract — like the webhook envelope today — only
// surfaces single-valued headers. Multi-value forces the workflow
// author to unwrap a list for every common case (Authorization,
// Content-Type) where a single value is the norm. If a multi-value
// header use case shows up, add Envelope.RawHeaders later without
// breaking the v1 shape.
type Envelope struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   map[string]string `json:"query,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
}

// BuildEnvelope reads the bounded request body and lifts request
// metadata into an Envelope. Panics on nil request (programmer
// error). max is forwarded to BoundedBody — same bounds, same
// semantics, single source of truth.
func BuildEnvelope(req *http.Request, max int64) (Envelope, error) {
	if req == nil {
		panic("BuildEnvelope: request must not be nil")
	}
	if req.URL == nil {
		panic("BuildEnvelope: request URL must not be nil")
	}

	body, err := BoundedBody(req.Body, max)
	if err != nil {
		return Envelope{}, err
	}

	var query map[string]string
	if rawQ := req.URL.Query(); len(rawQ) > 0 {
		query = make(map[string]string, len(rawQ))
		for k, vs := range rawQ {
			if len(vs) > 0 {
				query[k] = vs[0]
			}
		}
	}

	var headers map[string]string
	if len(req.Header) > 0 {
		headers = make(map[string]string, len(req.Header))
		for k, vs := range req.Header {
			if len(vs) > 0 {
				headers[k] = vs[0]
			}
		}
	}

	return Envelope{
		Method:  req.Method,
		Path:    req.URL.Path,
		Query:   query,
		Headers: headers,
		Body:    body,
	}, nil
}
