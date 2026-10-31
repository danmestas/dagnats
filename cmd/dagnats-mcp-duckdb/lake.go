package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// lakeFilesMax bounds one glob of the lake dir, and so one refresh of its views.
const lakeFilesMax = 1 << 20

// lakeWarnedMax bounds the skipped view names remembered so each is reported once.
const lakeWarnedMax = 1 << 10

// lakeNameRE is lake.ValidName's rule for a program or table name. It is copied, not imported:
// this module does not depend on the lake module for one expression.
var lakeNameRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// WithLakeDir adds one view per table of the lake at dir (a local path or an s3:// prefix),
// named <program>_<table>. A view reads the table's live parts: each part with a commit record
// part-<stem>.json and no supersede record part-<stem>.json.superseded. A part without a record
// (a writer's crash residue) is not read. Unset, the server has no lake views.
func WithLakeDir(dir string) Option {
	return func(o *options) { o.lakeDir = dir }
}

// lakePart is one part's position: program, table, dt directory and stem.
type lakePart struct{ program, table, dt, stem string }

// refreshLakeViews makes the lake views match the lake now. It runs before every query, so a
// compaction or retention that supersedes parts shows in the next query, and a view's part list
// is only replaced when it changed. A table with no live part has no view: its columns are only
// known from a part, and an empty view of the wrong shape would mislead.
func (db *DB) refreshLakeViews(ctx context.Context) error {
	if db.lakeDir == "" || db.lakeViews == nil {
		panic("refreshLakeViews: no lake dir")
	}
	db.lakeMu.Lock()
	defer db.lakeMu.Unlock()
	tables, err := db.discoverLake(ctx)
	if err != nil {
		return err
	}
	for name, files := range tables {
		signature := strings.Join(files, "\n")
		if db.lakeViews[name] == signature {
			continue
		}
		quoted := make([]string, len(files))
		for i, file := range files {
			quoted[i] = sqlString(file)
		}
		query := fmt.Sprintf(`CREATE OR REPLACE VIEW "%s" AS SELECT * FROM read_parquet([%s], `+
			`union_by_name=true, hive_partitioning=true)`, name, strings.Join(quoted, ", "))
		if _, err := db.conn.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("create lake view %s: %w", name, err)
		}
		db.lakeViews[name] = signature
	}
	for name := range db.lakeViews {
		if _, live := tables[name]; live {
			continue
		}
		if _, err := db.conn.ExecContext(ctx, `DROP VIEW IF EXISTS "`+name+`"`); err != nil {
			return fmt.Errorf("drop lake view %s: %w", name, err)
		}
		delete(db.lakeViews, name)
	}
	return nil
}

// discoverLake lists the lake's live parts by name alone, three globs and no object reads, so a
// local dir and an S3 prefix cost the same LISTs. It returns view name -> sorted part files.
func (db *DB) discoverLake(ctx context.Context) (map[string][]string, error) {
	sets := make([]map[lakePart]string, 3)
	for i, suffix := range []string{".json", ".json.superseded", ".parquet"} {
		found, err := db.globParts(ctx, suffix)
		if err != nil {
			return nil, err
		}
		sets[i] = found
	}
	records, superseded, parts := sets[0], sets[1], sets[2]
	byTable := map[[2]string][]string{}
	for part, file := range parts {
		_, committed := records[part]
		_, hidden := superseded[part]
		if committed && !hidden {
			key := [2]string{part.program, part.table}
			byTable[key] = append(byTable[key], file)
		}
	}
	keys := make([][2]string, 0, len(byTable))
	for key := range byTable {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i][0] < keys[j][0] || keys[i][0] == keys[j][0] && keys[i][1] < keys[j][1]
	})
	tables := map[string][]string{}
	for _, key := range keys {
		name, ok := db.lakeViewName(key[0], key[1], tables)
		if !ok {
			continue
		}
		files := byTable[key]
		sort.Strings(files)
		tables[name] = files
	}
	return tables, nil
}

// globParts lists <lake>/*/*/dt=*/part-*<suffix>, keyed by part position.
func (db *DB) globParts(ctx context.Context, suffix string) (map[lakePart]string, error) {
	if !strings.HasPrefix(suffix, ".") {
		panic("globParts: suffix must start with a dot")
	}
	pattern := db.lakeDir + "/*/*/dt=*/part-*" + suffix
	rows, err := db.conn.QueryContext(ctx, "SELECT file FROM glob("+sqlString(pattern)+")")
	if err != nil {
		return nil, fmt.Errorf("list lake %s: %w", suffix, err)
	}
	defer rows.Close()
	found := map[lakePart]string{}
	for rows.Next() {
		var file string
		if err := rows.Scan(&file); err != nil {
			return nil, fmt.Errorf("list lake %s: %w", suffix, err)
		}
		if len(found) >= lakeFilesMax {
			return nil, fmt.Errorf("lake has more than %d part-*%s files", lakeFilesMax, suffix)
		}
		segments := strings.Split(strings.TrimPrefix(file, db.lakeDir+"/"), "/")
		if len(segments) != 4 || !strings.HasSuffix(segments[3], suffix) {
			continue
		}
		stem := strings.TrimSuffix(strings.TrimPrefix(segments[3], "part-"), suffix)
		found[lakePart{segments[0], segments[1], segments[2], stem}] = file
	}
	return found, rows.Err()
}

// lakeViewName names a table's view, or refuses it: a program or table outside the lake's name
// rules, a name a signal view holds, or one an earlier table (in program, table order) took.
func (db *DB) lakeViewName(program, table string, taken map[string][]string) (string, bool) {
	name := program + "_" + table
	reason := ""
	switch {
	case !lakeNameRE.MatchString(program) || !lakeNameRE.MatchString(table):
		reason = "is not a valid lake program/table name"
	case slices.Contains(signals, name):
		reason = "collides with a telemetry view"
	case taken[name] != nil:
		reason = "collides with another lake table's view"
	}
	if reason == "" {
		return name, true
	}
	key := program + "/" + table
	if !db.lakeWarned[key] && len(db.lakeWarned) < lakeWarnedMax {
		db.lakeWarned[key] = true
		fmt.Fprintf(os.Stderr, "lake table %s: no view, %q %s\n", key, name, reason)
	}
	return "", false
}
