// build_info.go owns the always-on build/identity footer (R9, #320).
//
// The footer is a one-line strip rendered below <main> on every
// console page. Content: `dagnats vX.Y.Z (abc1234) • nats://host
// (embedded) • N/M streams`.
//
// Design notes:
//   - Build identity (cfg.Build) is plumbed in via console.Config
//     already, so the footer reuses that string verbatim (#312).
//   - Endpoint + embedded marker + provisioned-stream count come
//     from DataSource.ConfigSnapshot — the same call /config uses.
//     We do not add a new DataSource method (audit-locked).
//   - The footer is intentionally identity-only. Real-time
//     connection state belongs to the header connection-pill;
//     duplicating ONLINE/OFFLINE here was the audit miss the
//     issue rewrite removed.
//   - One ConfigSnapshot round-trip per page render is the price
//     for keeping the footer authoritative. The snapshot is
//     cheap (already cached at the adapter level) and a missing
//     data source degrades to a sparse footer rather than 500ing.
package console

import (
	"context"
	"fmt"
)

// BuildInfo is the data the build-info footer template binds.
//
// DagnatsBuild is cfg.Build verbatim — the server constructs the
// "vX.Y.Z (abc1234)" string at startup and the console treats it as
// opaque so the format can evolve without coupling.
//
// NATSHost is the connected NATS URL. Empty when the data source
// hasn't been wired (the footer omits the endpoint segment in that
// case).
//
// NATSEmbedded toggles the "(embedded)" suffix on the host segment;
// false means the deployment is connected to an external NATS server.
//
// StreamsProvisioned / StreamsKnown drive the "N/M streams"
// fragment: known is the static list of streams natsutil provisions,
// provisioned is the subset confirmed reachable via JetStream.
type BuildInfo struct {
	DagnatsBuild       string
	NATSHost           string
	NATSEmbedded       bool
	StreamsProvisioned int
	StreamsKnown       int
}

// buildBuildInfo assembles the footer payload from cfg + a
// best-effort ConfigSnapshot. Degrades to a partial payload when
// the data source is nil or unreachable — the footer must never
// 500 a page; a missing piece becomes an empty segment.
//
// The function takes ctx so the snapshot call respects the request
// deadline. A nil ctx panics — programmer error.
func buildBuildInfo(ctx context.Context, cfg Config) BuildInfo {
	if ctx == nil {
		panic("buildBuildInfo: ctx is nil")
	}
	info := BuildInfo{DagnatsBuild: cfg.Build}
	if cfg.Data == nil {
		return info
	}
	snap, err := cfg.Data.ConfigSnapshot(ctx)
	if err != nil {
		return info
	}
	info.NATSHost = snap.NATSURL
	info.NATSEmbedded = snap.NATSEmbedded
	info.StreamsKnown = len(snap.Streams)
	for _, s := range snap.Streams {
		if s.Provisioned {
			info.StreamsProvisioned++
		}
	}
	return info
}

// HostLabel renders the NATS host with the embedded marker, e.g.
// `nats://127.0.0.1:4222 (embedded)`. Returns the bare URL when the
// connection is external. Returns empty string when the URL is
// unknown — the template hides the segment in that case.
func (b BuildInfo) HostLabel() string {
	if b.NATSHost == "" {
		return ""
	}
	if b.NATSEmbedded {
		return fmt.Sprintf("%s (embedded)", b.NATSHost)
	}
	return b.NATSHost
}

// StreamsLabel renders the N/M streams fragment. Returns empty when
// no streams are known (data source unreachable) so the template
// omits the segment rather than rendering "0/0 streams" — that
// would lie about a reachable-but-empty state.
func (b BuildInfo) StreamsLabel() string {
	if b.StreamsKnown == 0 {
		return ""
	}
	return fmt.Sprintf("%d/%d streams",
		b.StreamsProvisioned, b.StreamsKnown)
}
