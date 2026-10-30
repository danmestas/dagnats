package lake

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ListedCommitsMax bounds one ListCommitted call; a table past it needs a narrower listing.
const ListedCommitsMax = 1 << 20

// Commit is the record written after a part. A reader trusts a part only through its commit.
type Commit struct {
	Part          string    `json:"part"` // file name, part-<sha256-16>.parquet
	Rows          int       `json:"rows"`
	Bytes         int       `json:"bytes"`
	Stream        string    `json:"stream"`
	SeqMin        int64     `json:"seq_min"`
	SeqMax        int64     `json:"seq_max"`
	SchemaVersion int       `json:"schema_version"`
	WrittenAt     time.Time `json:"written_at"`

	// Dt and Path are derived from where the record was found, not stored in it.
	Dt   string `json:"-"`
	Path string `json:"-"` // part file path under the root
}

func tableDir(root, program, table string) string {
	return filepath.Join(root, program, table)
}

// PartitionDir is the directory of one event date of a table.
func PartitionDir(root, program, table, dt string) string {
	return filepath.Join(tableDir(root, program, table), "dt="+dt)
}

// ListCommitted returns every committed part of a table, ordered by (dt, part). Parts without a
// commit record are ignored: they are the residue of a crash between part and commit. A commit
// whose part is missing is corruption and an error, since commit records are removed before the
// parts they cover.
func ListCommitted(root, program, table string) ([]Commit, error) {
	if root == "" || !ValidName(program) || !ValidName(table) {
		return nil, fmt.Errorf("lake: list %q/%q under %q: bad name", program, table, root)
	}
	dir := tableDir(root, program, table)
	partitions, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lake: list %s: %w", dir, err)
	}
	var commits []Commit
	for _, partition := range partitions {
		dt, found := strings.CutPrefix(partition.Name(), "dt=")
		if !found || !partition.IsDir() {
			continue
		}
		partCommits, err := readPartition(root, program, table, dt, len(commits))
		if err != nil {
			return nil, err
		}
		commits = append(commits, partCommits...)
	}
	sort.Slice(commits, func(i, j int) bool {
		if commits[i].Dt != commits[j].Dt {
			return commits[i].Dt < commits[j].Dt
		}
		return commits[i].Part < commits[j].Part
	})
	return commits, nil
}

func readPartition(root, program, table, dt string, listed int) ([]Commit, error) {
	dir := PartitionDir(root, program, table, dt)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("lake: list %s: %w", dir, err)
	}
	var commits []Commit
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "part-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if listed+len(commits) >= ListedCommitsMax {
			return nil, fmt.Errorf("lake: %s: more than %d commits", dir, ListedCommitsMax)
		}
		commit, err := readCommit(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		commit.Dt = dt
		commit.Path = filepath.Join(dir, commit.Part)
		if _, err := os.Stat(commit.Path); err != nil {
			return nil, fmt.Errorf("lake: commit %s names a missing part: %w", entry.Name(), err)
		}
		commits = append(commits, commit)
	}
	return commits, nil
}

func readCommit(path string) (Commit, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Commit{}, fmt.Errorf("lake: read commit: %w", err)
	}
	var commit Commit
	if err := json.Unmarshal(data, &commit); err != nil {
		return Commit{}, fmt.Errorf("lake: decode commit %s: %w", path, err)
	}
	want := strings.TrimSuffix(filepath.Base(path), ".json") + ".parquet"
	if commit.Part != want {
		return Commit{}, fmt.Errorf("lake: commit %s names part %q, want %q", path, commit.Part, want)
	}
	return commit, nil
}
