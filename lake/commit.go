package lake

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ListedCommitsMax bounds one ListCommitted call; a table past it needs a narrower listing.
const ListedCommitsMax = 1 << 20

// Commit is the record written after a part. A reader trusts a part only through its commit.
// The writer names a part from Digest, a hash of the batch's content, so a true retry lands on
// the same name. Two committed parts of one stream covering overlapping sequence ranges hold
// different content (a stream reset or purge, or a bug); ListCommitted reports them as Overlaps.
type Commit struct {
	Part          string    `json:"part"` // file name, part-<digest prefix>.parquet
	Digest        string    `json:"digest"`
	Rows          int       `json:"rows"`
	Bytes         int       `json:"bytes"`
	Stream        string    `json:"stream"`
	SeqMin        int64     `json:"seq_min"`
	SeqMax        int64     `json:"seq_max"`
	SchemaVersion int       `json:"schema_version"`
	WrittenAt     time.Time `json:"written_at"`

	// Dt, Key and Path are derived from where the record was found, not stored in it.
	Dt   string `json:"-"`
	Key  string `json:"-"` // part key, slash-separated, relative to the lake root
	Path string `json:"-"` // part file path under a local root; empty from a writer
}

// Rejected is a commit record a reader will not trust, with the reason.
type Rejected struct {
	Record string // path of the commit record
	Reason string
}

// Overlap is a pair of commits of one stream whose sequence ranges intersect: the signature of
// a duplicate, since each stream message lands in exactly one part.
type Overlap struct {
	First, Second Commit
}

// Listing is what a reader may trust, and what it should look at.
type Listing struct {
	Commits  []Commit // ordered by (dt, part)
	Rejected []Rejected
	Overlaps []Overlap
}

func tableDir(root, program, table string) string {
	return filepath.Join(root, program, table)
}

// PartitionKey is the slash-separated key prefix of one event date of a table, relative to the
// lake root: every store, local or object, lays parts out under it.
func PartitionKey(program, table, dt string) string {
	return path.Join(program, table, "dt="+dt)
}

// PartitionDir is the directory of one event date of a table.
func PartitionDir(root, program, table, dt string) string {
	return filepath.Join(root, filepath.FromSlash(PartitionKey(program, table, dt)))
}

// ListCommitted returns the committed parts of a table. Parts without a commit record are
// ignored: they are the residue of a crash between part and commit. One unreadable or untrusted
// record does not hide the others; it is listed in Rejected (a commit whose part is missing is
// rejected too, since writers remove commit records before the parts they cover; the remover is
// L5). Overlaps reports commits of one stream with intersecting sequence ranges.
func ListCommitted(root, program, table string) (Listing, error) {
	if root == "" || !ValidName(program) || !ValidName(table) {
		return Listing{}, fmt.Errorf("lake: list %q/%q under %q: bad name", program, table, root)
	}
	dir := tableDir(root, program, table)
	partitions, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return Listing{}, nil
	}
	if err != nil {
		return Listing{}, fmt.Errorf("lake: list %s: %w", dir, err)
	}
	var listing Listing
	for _, partition := range partitions {
		dt, found := strings.CutPrefix(partition.Name(), "dt=")
		if !found || !partition.IsDir() {
			continue
		}
		if err := readPartition(&listing, root, PartitionKey(program, table, dt), dt); err != nil {
			return Listing{}, err
		}
	}
	sort.Slice(listing.Commits, func(i, j int) bool {
		a, b := listing.Commits[i], listing.Commits[j]
		if a.Dt != b.Dt {
			return a.Dt < b.Dt
		}
		return a.Part < b.Part
	})
	listing.Overlaps = findOverlaps(listing.Commits)
	return listing, nil
}

func readPartition(listing *Listing, root, prefix, dt string) error {
	dir := filepath.Join(root, filepath.FromSlash(prefix))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("lake: list %s: %w", dir, err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "part-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if len(listing.Commits)+len(listing.Rejected) >= ListedCommitsMax {
			return fmt.Errorf("lake: %s: more than %d commits", dir, ListedCommitsMax)
		}
		record := filepath.Join(dir, entry.Name())
		commit, err := ReadCommit(record)
		if err == nil {
			commit.Dt, commit.Key = dt, path.Join(prefix, commit.Part)
			commit.Path = filepath.Join(dir, commit.Part)
			_, err = os.Stat(commit.Path)
			if err != nil {
				err = fmt.Errorf("names a missing part: %w", err)
			}
		}
		if err != nil {
			listing.Rejected = append(listing.Rejected, Rejected{Record: record, Reason: err.Error()})
			continue
		}
		listing.Commits = append(listing.Commits, commit)
	}
	return nil
}

// findOverlaps sorts a copy by (stream, seq_min) and compares each range to the furthest one
// seen so far in its stream.
func findOverlaps(commits []Commit) []Overlap {
	sorted := append([]Commit(nil), commits...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Stream != sorted[j].Stream {
			return sorted[i].Stream < sorted[j].Stream
		}
		return sorted[i].SeqMin < sorted[j].SeqMin
	})
	var overlaps []Overlap
	var furthest Commit
	for i, commit := range sorted {
		if i > 0 && commit.Stream == furthest.Stream && commit.SeqMin <= furthest.SeqMax {
			overlaps = append(overlaps, Overlap{First: furthest, Second: commit})
		}
		if i == 0 || commit.Stream != furthest.Stream || commit.SeqMax > furthest.SeqMax {
			furthest = commit
		}
	}
	return overlaps
}

// ReadCommit decodes one commit record and checks that it names the part beside it.
func ReadCommit(path string) (Commit, error) {
	if !strings.HasSuffix(path, ".json") {
		return Commit{}, fmt.Errorf("lake: %q is not a commit record", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Commit{}, fmt.Errorf("lake: read commit: %w", err)
	}
	return DecodeCommit(path, data)
}

// DecodeCommit decodes the bytes of the commit record at name (a path or key; only its base
// matters) and checks that it names the part beside it.
func DecodeCommit(name string, data []byte) (Commit, error) {
	if !strings.HasSuffix(name, ".json") {
		return Commit{}, fmt.Errorf("lake: %q is not a commit record", name)
	}
	var commit Commit
	if err := json.Unmarshal(data, &commit); err != nil {
		return Commit{}, fmt.Errorf("lake: decode commit %s: %w", name, err)
	}
	want := strings.TrimSuffix(path.Base(filepath.ToSlash(name)), ".json") + ".parquet"
	if commit.Part != want {
		return Commit{}, fmt.Errorf("lake: commit %s names part %q, want %q", name, commit.Part, want)
	}
	if commit.SeqMin > commit.SeqMax || commit.Rows < 0 {
		return Commit{}, fmt.Errorf("lake: commit %s has an impossible range or row count", name)
	}
	return commit, nil
}
