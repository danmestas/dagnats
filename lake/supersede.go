package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"
	"time"
)

// SupersededSuffix ends a supersede record's key: part-<stem>.json.superseded beside the part's
// commit record part-<stem>.json.
const SupersededSuffix = ".json.superseded"

// RecordStore is what Supersede writes through. Every writer.Store satisfies it: Put is
// create-only and fails matching fs.ErrExist via errors.Is; Get fails matching fs.ErrNotExist.
type RecordStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// Supersede marks a committed part as no longer live, so ListCommitted moves it from Commits to
// Superseded. It writes a create-only record beside the commit: the commit as listed, with
// SupersededBy set to by (the stem, part-<stem>.parquet, of the part that now holds its rows;
// empty when retention dropped them) and WrittenAt set to at, the time the part stopped being
// live. commit is one ListCommitted returned, since its Key says where to write.
//
// A second Supersede of the same part to the same successor returns the record already stored.
// One naming a different successor, or holding another digest, is an error: a part's rows
// cannot be in two places.
func Supersede(
	ctx context.Context, store RecordStore, commit Commit, by string, at time.Time,
) (Commit, error) {
	if store == nil || at.IsZero() {
		panic("lake: Supersede needs a store and a time")
	}
	stem, err := partStem(commit)
	if err != nil {
		return Commit{}, err
	}
	if by == stem || strings.ContainsAny(by, "/.\\") || len(by) > 64 {
		return Commit{}, fmt.Errorf("lake: supersede %s by %q: not another part's stem", commit.Part, by)
	}
	marked := commit
	marked.SupersededBy, marked.WrittenAt = by, at.UTC()
	record, err := json.Marshal(marked)
	if err != nil {
		return Commit{}, fmt.Errorf("lake: encode supersede record: %w", err)
	}
	key := path.Join(path.Dir(commit.Key), "part-"+stem+SupersededSuffix)
	err = store.Put(ctx, key, record)
	if err == nil {
		return marked, nil
	}
	if !errors.Is(err, fs.ErrExist) {
		return Commit{}, fmt.Errorf("lake: put %s: %w", key, err)
	}
	return existingSupersede(ctx, store, key, marked)
}

// partStem checks that commit names a part at its key and returns the part's stem.
func partStem(commit Commit) (string, error) {
	stem, found := strings.CutPrefix(strings.TrimSuffix(commit.Part, ".parquet"), "part-")
	if !found || stem == "" || commit.Key == "" || path.Base(commit.Key) != commit.Part ||
		path.Clean(commit.Key) != commit.Key || commit.Digest == "" {
		return "", fmt.Errorf("lake: supersede %q at key %q: not a listed commit", commit.Part, commit.Key)
	}
	return stem, nil
}

func existingSupersede(ctx context.Context, store RecordStore, key string, ours Commit) (
	Commit, error,
) {
	data, err := store.Get(ctx, key)
	if err != nil {
		return Commit{}, fmt.Errorf("lake: get %s: %w", key, err)
	}
	existing, err := DecodeCommit(key, data)
	if err != nil {
		return Commit{}, err
	}
	if existing.Digest != ours.Digest || existing.SupersededBy != ours.SupersededBy {
		return Commit{}, fmt.Errorf("lake: %s is superseded by %q (digest %s); refusing %q "+
			"(digest %s)", ours.Part, existing.SupersededBy, existing.Digest, ours.SupersededBy,
			ours.Digest)
	}
	existing.Dt, existing.Key, existing.Path = ours.Dt, ours.Key, ours.Path
	return existing, nil
}
