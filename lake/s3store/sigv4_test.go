// Methodology: the signer is checked against the worked GET Object example in Amazon's SigV4
// documentation (fixed date, example keys, known signature), so a canonicalisation slip fails
// here rather than as a 403 from a live store. The second test pins the percent-encoding rules.
package s3store

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestSignMatchesAWSExample(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-9")
	creds := credentials{
		accessKey: "AKIAIOSFODNN7EXAMPLE",
		secretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		region:    "us-east-1",
	}
	when := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)
	sign(req, creds, emptyPayload, when)

	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Fatalf("Authorization\n got %s\nwant %s", got, want)
	}
	if req.Header.Get("x-amz-date") != "20130524T000000Z" {
		t.Fatalf("x-amz-date %q", req.Header.Get("x-amz-date"))
	}
}

func TestURIEncode(t *testing.T) {
	cases := map[string]string{
		"abc-_.~":      "abc-_.~",
		"a b":          "a%20b",
		"a/b":          "a%2Fb",
		"chunks/ab/cd": "chunks%2Fab%2Fcd",
		"ü":            "%C3%BC",
		"*":            "%2A",
	}
	for in, want := range cases {
		if got := uriEncode(in); got != want {
			t.Fatalf("uriEncode(%q) = %q, want %q", in, got, want)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, "https://h/pre fix/a~b", nil)
	if got := canonicalURI(req.URL); got != "/pre%20fix/a~b" {
		t.Fatalf("canonicalURI %q", got)
	}
	req, _ = http.NewRequest(http.MethodGet, "https://h/?list-type=2&prefix=a/b&max-keys=10", nil)
	if got := canonicalQuery(req.URL); got != "list-type=2&max-keys=10&prefix=a%2Fb" {
		t.Fatalf("canonicalQuery %q", got)
	}
	if !strings.HasPrefix(emptyPayload, "e3b0c442") {
		t.Fatal("empty payload hash constant")
	}
}
