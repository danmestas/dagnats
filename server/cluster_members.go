package server

import (
	"errors"

	"github.com/danmestas/dagnats/internal/api"
	natsserver "github.com/nats-io/nats-server/v2/server"
)

// clusterMembershipFrom adapts the embedded server's JetStream meta-group
// and route table to api.ClusterMembershipFunc. Reading in-process avoids
// needing a system account just to ask who the peers are.
func clusterMembershipFrom(
	ns *natsserver.Server,
) api.ClusterMembershipFunc {
	if ns == nil {
		panic("clusterMembershipFrom: ns must not be nil")
	}
	return func() (api.ClusterMembership, error) {
		jsz, err := ns.Jsz(&natsserver.JSzOptions{})
		if err != nil {
			return api.ClusterMembership{}, err
		}
		routez, err := ns.Routez(&natsserver.RoutezOptions{})
		if err != nil {
			return api.ClusterMembership{}, err
		}
		if jsz.Meta == nil {
			return api.ClusterMembership{}, errors.New(
				"jetstream meta group not available")
		}
		return buildMembership(ns.Name(), jsz.Meta, routez.Routes), nil
	}
}

func boolPtr(v bool) *bool { return &v }

// buildMembership merges the JetStream meta view (leader, lag, offline)
// with the route table (is the TCP route to that server up). The meta
// group lists peers other than the reporting node, so self is prepended.
func buildMembership(
	selfName string,
	meta *natsserver.MetaClusterInfo,
	routes []*natsserver.RouteInfo,
) api.ClusterMembership {
	if selfName == "" {
		panic("buildMembership: selfName must not be empty")
	}
	if meta == nil {
		panic("buildMembership: meta must not be nil")
	}
	connected := make(map[string]bool, len(routes))
	for _, r := range routes {
		connected[r.RemoteName] = true
	}
	// Follower views of other followers read current=false; only the
	// leader's view of "caught up" is trustworthy.
	leaderView := meta.Leader == selfName
	view := api.ClusterMembership{Self: selfName, MetaLeader: meta.Leader}
	self := api.ClusterMember{
		Name:           selfName,
		Self:           true,
		RouteConnected: true,
		MetaLeader:     leaderView,
	}
	if leaderView {
		self.Current = boolPtr(true)
	}
	view.Members = append(view.Members, self)
	for _, peer := range meta.Replicas {
		if peer.Name == selfName {
			continue
		}
		member := api.ClusterMember{
			Name:           peer.Name,
			RouteConnected: connected[peer.Name],
			MetaLeader:     meta.Leader == peer.Name,
			Offline:        peer.Offline,
		}
		if leaderView {
			member.Current = boolPtr(peer.Current)
			member.LagOps = peer.Lag
		}
		view.Members = append(view.Members, member)
	}
	return view
}
