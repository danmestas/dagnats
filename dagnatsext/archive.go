package dagnatsext

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// The archive range protocol. An archiver (lode) that holds a stream's
// purged messages answers "lode.stream.<domain>.<stream>.range" with the
// messages of a sequence range, so a reader can go below the stream's
// FirstSeq. ArchiveReader is the seam a reader uses; archivenats.NewReader
// speaks the protocol over NATS request/reply.

// ArchiveSubjectPrefix is the root of every archive range subject. No
// program may publish under it (program.PermissionsGranted denies it), so
// only dagnats itself asks an archive for records.
const ArchiveSubjectPrefix = "lode.stream."

// ArchiveMaxBytes is the raw page size a reader asks for, the most an
// archive serves per reply.
const ArchiveMaxBytes = 512 << 10

// ArchiveDefaultDomain is the domain token of a server that names no
// JetStream domain: "lode.stream..<stream>.range" is not a valid subject.
// A server whose jetstream_domain is literally "default" shares it.
const ArchiveDefaultDomain = "default"

// ArchiveRangeSubject is the one place the range subject is assembled:
// "lode.stream.<domain>.<stream>.range", with ArchiveDefaultDomain for an
// empty domain. Both the reader and an archiver use it.
func ArchiveRangeSubject(domain, stream string) string {
	if stream == "" || strings.ContainsAny(stream, ".*> ") {
		panic("dagnatsext.ArchiveRangeSubject: stream must be one literal token")
	}
	if strings.ContainsAny(domain, ".*> ") {
		panic("dagnatsext.ArchiveRangeSubject: domain must be one literal token")
	}
	if domain == "" {
		domain = ArchiveDefaultDomain
	}
	return ArchiveSubjectPrefix + domain + "." + stream + ".range"
}

// ArchiveRange selects the archived messages to read.
type ArchiveRange struct {
	// Epoch is the stream's StreamInfo.Created.UnixNano(): a stream
	// deleted and recreated under the same name is another epoch.
	Epoch int64
	// Subject filters to one subject; empty reads every subject.
	Subject string
	// FromSeq is the first sequence to read, inclusive.
	FromSeq uint64
	// ToSeq is exclusive: the stream's FirstSeq, so the archive serves
	// only what JetStream no longer holds.
	ToSeq uint64
	// Limit stops the read after this many messages; 0 reads the range.
	Limit int
}

// ArchivedMessage is one complete message, reassembled when the archive
// split it across replies. Time has microsecond precision: never compare
// it for equality with a live record's.
type ArchivedMessage struct {
	Seq     uint64              `json:"seq"`
	Subject string              `json:"subject"`
	Time    time.Time           `json:"time"`
	Headers map[string][]string `json:"headers,omitempty"`
	Data    []byte              `json:"data"`
}

// ArchiveGap is a stream-wide loss the archive reports, sequences From
// through To inclusive. With a subject filter any gap in range may have
// held a message of that subject.
type ArchiveGap struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

// ArchivedRange is a read's result: the messages in seq order and every
// gap the archive reported in the range.
type ArchivedRange struct {
	Messages []ArchivedMessage
	Gaps     []ArchiveGap
}

// ArchiveReader reads an archived range of one stream. Implementations
// hide paging, split-message reassembly and busy retries: a caller gets
// complete messages in seq order, or an error.
type ArchiveReader interface {
	ReadRange(ctx context.Context, r ArchiveRange) (ArchivedRange, error)
}

// Archive error codes, as the archive's error reply names them.
const (
	ArchiveCodeUnknownEpoch = "unknown_epoch"
	ArchiveCodeNotArchived  = "not_archived"
	ArchiveCodeBusy         = "busy"
	// ArchiveCodeNotIndexed: the archive's subject index is still being
	// built. Not retried like busy: the read fails.
	ArchiveCodeNotIndexed = "not_indexed"
)

// Sentinels an *ArchiveError matches with errors.Is.
var (
	// ErrArchiveUnknownEpoch: the archive holds no such stream epoch.
	ErrArchiveUnknownEpoch = errors.New("archive: unknown stream epoch")
	// ErrArchiveNotArchived: the range reaches past what the archive
	// holds, so continuity with JetStream cannot be confirmed.
	ErrArchiveNotArchived = errors.New("archive: range not archived")
	// ErrArchiveBusy: the archive stayed busy through every retry.
	ErrArchiveBusy = errors.New("archive: busy")
	// ErrArchiveNotIndexed: the archive cannot filter by subject yet.
	ErrArchiveNotIndexed = errors.New("archive: subject index not built")
	// ErrArchiveUnavailable: no archive answers the range subject.
	ErrArchiveUnavailable = errors.New("archive: no archive responder")
	// ErrArchiveProtocol: a reply broke the range protocol.
	ErrArchiveProtocol = errors.New("archive: protocol violation")
)

// ArchiveError is an error reply from the archive. Any code fails the
// read; only busy is retried first.
type ArchiveError struct {
	Code    string
	Message string
	// Floor is what a not_archived reply says the archive holds up to.
	Floor uint64
}

func (e *ArchiveError) Error() string {
	if e.Code == ArchiveCodeNotArchived {
		return fmt.Sprintf("archive: %s: %s (archived floor %d)", e.Code, e.Message, e.Floor)
	}
	return fmt.Sprintf("archive: %s: %s", e.Code, e.Message)
}

// Is maps the code onto its sentinel.
func (e *ArchiveError) Is(target error) bool {
	switch e.Code {
	case ArchiveCodeUnknownEpoch:
		return target == ErrArchiveUnknownEpoch
	case ArchiveCodeNotArchived:
		return target == ErrArchiveNotArchived
	case ArchiveCodeBusy:
		return target == ErrArchiveBusy
	case ArchiveCodeNotIndexed:
		return target == ErrArchiveNotIndexed
	}
	return false
}

// ArchiveRangeRequest is the wire request. Epoch is an int64 JSON
// integer: nanosecond epochs exceed 2^53, so it is never a float.
type ArchiveRangeRequest struct {
	Epoch    int64  `json:"epoch"`
	Subject  string `json:"subject,omitempty"`
	FromSeq  uint64 `json:"from_seq"`
	ToSeq    uint64 `json:"to_seq"`
	Offset   int    `json:"offset"`
	MaxBytes int    `json:"max_bytes"`
}

// ArchiveRangeReply is the wire reply: a page, or an error with a code.
// A message the page ends inside has More set; NextSeq names it and
// NextOffset the byte to continue from.
type ArchiveRangeReply struct {
	Messages   []ArchivedMessage `json:"messages"`
	NextSeq    uint64            `json:"next_seq"`
	NextOffset int               `json:"next_offset"`
	More       bool              `json:"more"`
	Gaps       []ArchiveGap      `json:"gaps,omitempty"`
	Done       bool              `json:"done"`
	Error      string            `json:"error,omitempty"`
	Code       string            `json:"code,omitempty"`
	Floor      uint64            `json:"floor,omitempty"`
}

// ArchiveTime formats a message time the way the protocol carries it:
// RFC3339 in UTC with microseconds.
func ArchiveTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000Z07:00")
}
