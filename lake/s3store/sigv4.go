package s3store

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// AWS Signature Version 4 over headers, the subset S3-compatible stores need: one region, the
// "s3" service, a precomputed payload hash. Written here rather than imported because it is
// eighty lines and the only part of the AWS API the lake uses.

const (
	amzDateFormat = "20060102T150405Z"
	dateFormat    = "20060102"
	algorithm     = "AWS4-HMAC-SHA256"
	emptyPayload  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type credentials struct {
	accessKey string
	secretKey string
	region    string
}

// sign adds Authorization, x-amz-date and x-amz-content-sha256 to req. payloadHash is the hex
// SHA-256 of the body (emptyPayload for none). Host must already be set by the request.
func sign(req *http.Request, creds credentials, payloadHash string, now time.Time) {
	if req.URL.Host == "" || payloadHash == "" {
		panic("s3store: sign needs a host and a payload hash")
	}
	amzDate := now.UTC().Format(amzDateFormat)
	date := now.UTC().Format(dateFormat)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if req.Header.Get("Host") == "" {
		req.Header.Set("Host", req.URL.Host)
	}

	signedHeaders, canonicalHeaders := canonicalizeHeaders(req.Header)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQuery(req.URL),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := date + "/" + creds.region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		algorithm, amzDate, scope, hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	key := hmacSHA256([]byte("AWS4"+creds.secretKey), []byte(date))
	key = hmacSHA256(key, []byte(creds.region))
	key = hmacSHA256(key, []byte("s3"))
	key = hmacSHA256(key, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))

	req.Header.Set("Authorization", algorithm+" Credential="+creds.accessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
	if len(signature) != 64 {
		panic("s3store: signature length")
	}
}

// canonicalizeHeaders returns the signed header list and the canonical header block. Every
// header present is signed; S3 requires at least host and x-amz-date.
func canonicalizeHeaders(h http.Header) (string, string) {
	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, strings.ToLower(name))
	}
	sort.Strings(names)
	var block strings.Builder
	for _, name := range names {
		values := h.Values(name)
		for i, v := range values {
			values[i] = strings.Join(strings.Fields(v), " ")
		}
		block.WriteString(name)
		block.WriteString(":")
		block.WriteString(strings.Join(values, ","))
		block.WriteString("\n")
	}
	return strings.Join(names, ";"), block.String()
}

// canonicalURI is the path with every segment URI-encoded once, as S3 expects (S3 does not
// double-encode the way other services do).
func canonicalURI(u *url.URL) string {
	if u.Path == "" {
		return "/"
	}
	segments := strings.Split(u.Path, "/")
	for i, s := range segments {
		segments[i] = uriEncode(s)
	}
	return strings.Join(segments, "/")
}

func canonicalQuery(u *url.URL) string {
	values := u.Query()
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := values[k]
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

// uriEncode is RFC 3986 percent-encoding of everything but unreserved characters, as SigV4
// requires; url.PathEscape leaves too much unencoded.
func uriEncode(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexDigits[c>>4])
			b.WriteByte(hexDigits[c&15])
		}
	}
	return b.String()
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}
